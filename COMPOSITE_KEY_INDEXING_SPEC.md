# Composite Key Indexing Algorithm Specification

## Version: 1.0 | Author: System Architect | Date: 2026-01-17

---

## 1. ĐỊNH NGHĨA BÀI TOÁN

### 1.1 Mô hình dữ liệu

```
Record = {
    field_1: value_1,    // High cardinality (e.g., user_id)
    field_2: value_2,    // Medium cardinality (e.g., category)
    field_3: value_3,    // Low cardinality (e.g., status)
    field_n: value_n,
    payload: data
}
```

### 1.2 Các loại truy vấn cần hỗ trợ

| Query Type | Pattern | Ví dụ |
|------------|---------|-------|
| Exact Match | `A = x` | `user_id = "U001"` |
| Prefix Match | `A = x AND B = y` | `user_id = "U001" AND category = "ORDER"` |
| Range Query | `A = x AND B BETWEEN t1 AND t2` | `user_id = "U001" AND timestamp BETWEEN 1000 AND 2000` |

### 1.3 Ràng buộc hệ thống

- **Không full scan**: O(n) scan không được phép
- **Distributed-friendly**: Hỗ trợ sharding/partitioning
- **Sorted KV-Store**: Dữ liệu được sắp xếp theo key (LSM-tree, B+tree)
- **Byte-comparable**: Key phải so sánh được theo thứ tự byte

---

## 2. CHIẾN LƯỢC THIẾT KẾ COMPOSITE KEY

### 2.1 Nguyên tắc xác định thứ tự field

```
RULE: Fields được sắp xếp theo thứ tự ưu tiên truy vấn, KHÔNG phải cardinality

Priority Order:
1. Equality fields (WHERE A = x) → đặt trước
2. Range fields (WHERE B BETWEEN) → đặt sau equality
3. High-selectivity fields → ưu tiên đặt trước trong cùng nhóm
```

### 2.2 Cardinality Analysis Matrix

| Cardinality | Định nghĩa | Vị trí khuyến nghị |
|-------------|------------|-------------------|
| High | > 10^6 unique values | Position 1-2 (filter sớm) |
| Medium | 10^3 - 10^6 | Position 2-3 |
| Low | < 10^3 | Position cuối hoặc không index |

### 2.3 Prefix Scan vs Range Scan

```
Composite Key: [A][B][C][D]

✅ Prefix Scan hiệu quả:
   - A = x                    → Scan prefix [A]
   - A = x AND B = y          → Scan prefix [A][B]
   - A = x AND B = y AND C = z → Scan prefix [A][B][C]

✅ Range Scan hiệu quả:
   - A = x AND B BETWEEN t1, t2 → Range [A][t1] to [A][t2]

❌ Không hiệu quả (skip field):
   - A = x AND C = z          → Phải scan toàn bộ [A][*]
   - B = y                    → Full scan (không dùng được prefix)
```

### 2.4 Khi nào cần Multiple Composite Keys

```
Scenario: Query patterns conflict

Pattern 1: WHERE user_id = x AND timestamp BETWEEN t1, t2
Pattern 2: WHERE category = y AND timestamp BETWEEN t1, t2

Solution: Tạo 2 composite keys song song
  - Key1: [user_id][timestamp] → payload_ref
  - Key2: [category][timestamp] → payload_ref
  
Trade-off: Storage x2, Write x2, Read O(log n)
```

---

## 3. THUẬT TOÁN CreateCompositeKey

### 3.1 Pseudo-code chính

```
ALGORITHM CreateCompositeKey(fields: List[Field], config: Config) -> bytes:
    
    # Step 1: Validate field order matches schema
    ASSERT fields.order == config.schema.order
    
    # Step 2: Initialize key buffer
    key_parts = []
    
    # Step 3: Optional anti-hotspot prefix
    IF config.enable_anti_hotspot:
        shard_prefix = ComputeShardPrefix(fields[0].value, config.shard_count)
        key_parts.APPEND(shard_prefix)
    
    # Step 4: Encode each field
    FOR field IN fields:
        encoded = EncodeField(field.value, field.type, config)
        key_parts.APPEND(encoded)
    
    # Step 5: Join with delimiter
    composite_key = JoinWithDelimiter(key_parts, config.delimiter)
    
    RETURN composite_key
```

### 3.2 Field Encoding Strategies

| Type | Encoding | Bytes | Sort Order |
|------|----------|-------|------------|
| STRING | UTF-8 + escape + padding | Variable/Fixed | Lexicographic |
| INTEGER | Big-endian + sign flip | 8 | Numeric |
| TIMESTAMP | Big-endian uint64 | 8 | Chronological |
| UUID | Raw 16 bytes | 16 | Lexicographic |
| DECIMAL | Scale → Integer | 8 | Numeric |

### 3.3 Delimiter Selection

```
DELIMITER_STRATEGIES = {
    "NULL_BYTE":     0x00,    # ⚠️ Không dùng nếu value chứa null
    "UNIT_SEPARATOR": 0x1F,   # ✅ ASCII control char, hiếm trong data
    "HIGH_BYTE":     0xFF,    # ⚠️ Ảnh hưởng sort order
    "DOUBLE_BYTE":   0x00_01, # ✅ An toàn hơn, tốn 2 bytes
}

RECOMMENDED: 0x1F (Unit Separator)
```

### 3.4 Anti-Hotspot Shard Prefix

```
ALGORITHM ComputeShardPrefix(value, shard_count: int) -> bytes:
    hash_value = HASH(value)  // MD5, xxHash, MurmurHash
    shard_id = hash_value % shard_count
    RETURN shard_id.to_bytes(2, "big")  // 2 bytes = max 65535 shards
```

---

## 4. THUẬT TOÁN QUERY

### 4.1 Exact Lookup

```
ALGORITHM ExactLookup(kv_store, fields) -> Record:
    key = CreateCompositeKey(fields, config)
    
    // Check bloom filter first
    IF bloom_filter AND NOT bloom_filter.might_contain(key):
        RETURN NULL
    
    // Check cache
    IF cache AND cache.contains(key):
        RETURN cache.get(key)
    
    // Point lookup - O(log n)
    value = kv_store.GET(key)
    
    IF value:
        cache.put(key, value)
    
    RETURN value
```

### 4.2 Prefix Scan

```
ALGORITHM PrefixScan(kv_store, prefix_fields, limit) -> List[Record]:
    prefix = CreateCompositeKey(prefix_fields, config)
    
    start_key = prefix
    end_key = IncrementLastByte(prefix)
    
    RETURN kv_store.RANGE_SCAN(start_key, end_key, limit)
```

### 4.3 Range Scan

```
ALGORITHM RangeScan(kv_store, equality_fields, range_field, min, max, limit):
    start_key = CreateCompositeKey(equality_fields + [range_field: min])
    end_key = CreateCompositeKey(equality_fields + [range_field: max])
    end_key = IncrementLastByte(end_key)
    
    RETURN kv_store.RANGE_SCAN(start_key, end_key, limit)
```

### 4.4 IncrementLastByte

```
ALGORITHM IncrementLastByte(key: bytes) -> bytes:
    FOR i FROM len(key) - 1 DOWNTO 0:
        IF key[i] < 0xFF:
            key[i] += 1
            RETURN key[:i+1]
    RETURN key + 0x00
```

---

## 5. TỐI ƯU NÂNG CAO

### 5.1 Bloom Filter

```
Purpose: Skip disk lookup for non-existent keys
False Positive Rate: 1% recommended
Space: ~10 bits per item
```

### 5.2 LRU Cache

```
Purpose: Cache hot data in memory
TTL: 5 minutes recommended
Size: Based on available memory
Invalidation: On write to same prefix
```

### 5.3 Write Buffer

```
Purpose: Batch writes to reduce IO
Size: 1000-10000 entries
Flush: On threshold or timeout
```

### 5.4 Compression

```
Key Compression: Prefix compression in same block
Value Compression: LZ4 (hot), Zstd (cold)
```

---

## 6. PHÂN TÍCH ĐỘ PHỨC TẠP

### 6.1 Time Complexity

| Operation | Composite Key | Secondary Index |
|-----------|:-------------:|:---------------:|
| Point Lookup | O(log n) | O(log n) + O(log m) |
| Prefix Scan | O(log n + k) | O(log n + k × log m) |
| Range Scan | O(log n + k) | O(log n + k × log m) |
| Insert | O(log n) | O(log n) × num_indexes |

### 6.2 Space Complexity

| Component | Space |
|-----------|-------|
| Composite Key | O(n × key_size) |
| Bloom Filter | O(n × 10 bits) |
| LRU Cache | O(cache_size) |

### 6.3 So sánh

| Criteria | Composite Key | B+Tree | Hash Index |
|----------|:-------------:|:------:|:----------:|
| Point Lookup | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Range Query | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐ |
| Prefix Query | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐ |
| Write Speed | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| Distributed | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |

---

## 7. VÍ DỤ THỰC TẾ

### 7.1 Schema: Order Management

```json
{
  "user_id": "string",
  "order_date": "timestamp",
  "order_id": "uuid",
  "amount": "decimal",
  "status": "string"
}
```

### 7.2 Composite Key Design

```
Key: [shard_prefix:2][user_id:32][order_date:8][order_id:16]
Total: 58 bytes + 3 delimiters = 61 bytes
```

### 7.3 Key Encoding Example

```
Input:
  user_id    = "USR_12345"
  order_date = 1737100800000
  order_id   = "550e8400-e29b-41d4-a716-446655440001"

Output (hex):
  00 10                          // shard prefix (16)
  1F                             // delimiter
  55 53 52 5F 31 32 33 34 35 00... // user_id (32 bytes padded)
  1F                             // delimiter
  00 00 01 94 73 46 B0 00        // timestamp (8 bytes)
  1F                             // delimiter
  55 0E 84 00 E2 9B 41 D4 A7 16 44 66 55 44 00 01 // uuid (16 bytes)
```

### 7.4 Query Examples

```python
# Exact lookup
result = index.exact_lookup({
    "user_id": "USR_12345",
    "order_date": 1737100800000,
    "order_id": "550e8400-e29b-41d4-a716-446655440001"
})

# Prefix scan - all orders for user
results = index.prefix_scan({"user_id": "USR_12345"}, limit=100)

# Range scan - orders in date range
results = index.range_scan(
    equality_values={"user_id": "USR_12345"},
    range_field="order_date",
    min_value=1737100800000,
    max_value=1737200000000,
    limit=100
)
```

---

## 8. BENCHMARK RESULTS

### Go Implementation (In-Memory, 10K records)

| Operation | Throughput | Memory |
|-----------|------------|--------|
| Write | 202,714 ops/s | 1,988 B/op |
| Point Lookup | 173,641 ops/s | 1,296 B/op |
| Prefix Scan | 195 ops/s | 124,345 B/op |

---

## 9. IMPLEMENTATION FILES

| File | Description |
|------|-------------|
| `composite_key_index.go` | Main implementation |
| `composite_key_index_test.go` | Unit tests |
| `cmd/demo/main.go` | Demo application |
| `examples/rocksdb_backend.go` | RocksDB integration |
| `examples/badger_backend.go` | BadgerDB integration |

---

## 10. PRODUCTION RECOMMENDATIONS

### KV Store Backends

| Backend | Use Case |
|---------|----------|
| RocksDB | Single node, high performance |
| BadgerDB | Go native, SSD optimized |
| TiKV | Distributed, Raft consensus |
| FoundationDB | ACID, distributed |

### Configuration Tuning

```yaml
# High write throughput
write_buffer_size: 10000
bloom_filter: false
cache: false

# High read throughput  
write_buffer_size: 1000
bloom_filter: true
bloom_expected_items: estimated_records
cache: true
cache_size: available_memory * 0.3

# Balanced
write_buffer_size: 5000
bloom_filter: true
cache: true
```

---

**END OF SPECIFICATION**
