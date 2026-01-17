package compositekey

import (
	"testing"

	"github.com/google/uuid"
)

// =============================================================================
// FIELD ENCODER TESTS
// =============================================================================

func TestEncodeString(t *testing.T) {
	encoder := &FieldEncoder{}
	config := FieldConfig{Name: "test", Type: FieldTypeString, FixedLength: 16}
	delimiter := []byte{0x1f}

	encoded, err := encoder.Encode("hello", config, delimiter)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	if len(encoded) != 16 {
		t.Errorf("Expected length 16, got %d", len(encoded))
	}

	if string(encoded[:5]) != "hello" {
		t.Errorf("Expected 'hello' prefix, got %s", string(encoded[:5]))
	}
}

func TestEncodeStringWithDelimiter(t *testing.T) {
	encoder := &FieldEncoder{}
	config := FieldConfig{Name: "test", Type: FieldTypeString}
	delimiter := []byte{0x1f}

	value := "hello\x1fworld"
	encoded, err := encoder.Encode(value, config, delimiter)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	// Delimiter should be doubled
	expected := []byte("hello\x1f\x1fworld")
	if string(encoded) != string(expected) {
		t.Errorf("Expected delimiter to be escaped")
	}
}

func TestEncodeIntegerPositive(t *testing.T) {
	encoder := &FieldEncoder{}
	config := FieldConfig{Name: "test", Type: FieldTypeInteger}
	delimiter := []byte{0x1f}

	encoded, err := encoder.Encode(int64(12345), config, delimiter)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	if len(encoded) != 8 {
		t.Errorf("Expected length 8, got %d", len(encoded))
	}
}

func TestEncodeIntegerSortOrder(t *testing.T) {
	encoder := &FieldEncoder{}
	config := FieldConfig{Name: "test", Type: FieldTypeInteger}
	delimiter := []byte{0x1f}

	encNeg, _ := encoder.Encode(int64(-100), config, delimiter)
	encZero, _ := encoder.Encode(int64(0), config, delimiter)
	encPos, _ := encoder.Encode(int64(100), config, delimiter)

	// Negative < Zero < Positive in byte comparison
	if string(encNeg) >= string(encZero) {
		t.Error("Negative should sort before zero")
	}
	if string(encZero) >= string(encPos) {
		t.Error("Zero should sort before positive")
	}
}

func TestEncodeTimestamp(t *testing.T) {
	encoder := &FieldEncoder{}
	config := FieldConfig{Name: "test", Type: FieldTypeTimestamp}
	delimiter := []byte{0x1f}

	timestamp := int64(1704067200000)
	encoded, err := encoder.Encode(timestamp, config, delimiter)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	if len(encoded) != 8 {
		t.Errorf("Expected length 8, got %d", len(encoded))
	}

	// Verify decode
	decoded, _ := encoder.Decode(encoded, config, delimiter)
	if decoded != timestamp {
		t.Errorf("Expected %d, got %v", timestamp, decoded)
	}
}

func TestEncodeUUID(t *testing.T) {
	encoder := &FieldEncoder{}
	config := FieldConfig{Name: "test", Type: FieldTypeUUID}
	delimiter := []byte{0x1f}

	testUUID := "550e8400-e29b-41d4-a716-446655440000"
	encoded, err := encoder.Encode(testUUID, config, delimiter)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	if len(encoded) != 16 {
		t.Errorf("Expected length 16, got %d", len(encoded))
	}

	// Verify decode
	decoded, _ := encoder.Decode(encoded, config, delimiter)
	if decoded != testUUID {
		t.Errorf("Expected %s, got %v", testUUID, decoded)
	}
}

func TestEncodeDecimal(t *testing.T) {
	encoder := &FieldEncoder{}
	config := FieldConfig{Name: "test", Type: FieldTypeDecimal, Precision: 2}
	delimiter := []byte{0x1f}

	encoded, err := encoder.Encode(123.45, config, delimiter)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	if len(encoded) != 8 {
		t.Errorf("Expected length 8, got %d", len(encoded))
	}

	// Verify decode
	decoded, _ := encoder.Decode(encoded, config, delimiter)
	if decoded != 123.45 {
		t.Errorf("Expected 123.45, got %v", decoded)
	}
}

func TestEncodeNull(t *testing.T) {
	encoder := &FieldEncoder{}
	config := FieldConfig{Name: "test", Type: FieldTypeString}
	delimiter := []byte{0x1f}

	encoded, err := encoder.Encode(nil, config, delimiter)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	if len(encoded) != 1 || encoded[0] != 0x00 {
		t.Error("Null should encode to single 0x00 byte")
	}
}

// =============================================================================
// COMPOSITE KEY BUILDER TESTS
// =============================================================================

func TestCreateFullKey(t *testing.T) {
	config := IndexConfig{
		Fields: []FieldConfig{
			{Name: "user_id", Type: FieldTypeString, FixedLength: 16},
			{Name: "timestamp", Type: FieldTypeTimestamp},
			{Name: "order_id", Type: FieldTypeUUID},
		},
		EnableAntiHotspot: true,
		ShardCount:        256,
		Delimiter:         DefaultDelimiter,
	}
	builder := NewCompositeKeyBuilder(config)

	key, err := builder.CreateKey(map[string]interface{}{
		"user_id":   "USER001",
		"timestamp": int64(1704067200000),
		"order_id":  "550e8400-e29b-41d4-a716-446655440000",
	})

	if err != nil {
		t.Fatalf("Failed to create key: %v", err)
	}

	if len(key) == 0 {
		t.Error("Key should not be empty")
	}
}

func TestCreatePrefixKey(t *testing.T) {
	config := IndexConfig{
		Fields: []FieldConfig{
			{Name: "user_id", Type: FieldTypeString, FixedLength: 16},
			{Name: "timestamp", Type: FieldTypeTimestamp},
		},
		EnableAntiHotspot: true,
		ShardCount:        256,
		Delimiter:         DefaultDelimiter,
	}
	builder := NewCompositeKeyBuilder(config)

	prefix, err := builder.CreateKey(map[string]interface{}{
		"user_id": "USER001",
	})
	if err != nil {
		t.Fatalf("Failed to create prefix: %v", err)
	}

	fullKey, err := builder.CreateKey(map[string]interface{}{
		"user_id":   "USER001",
		"timestamp": int64(1704067200000),
	})
	if err != nil {
		t.Fatalf("Failed to create full key: %v", err)
	}

	// Full key should be longer than prefix
	if len(fullKey) <= len(prefix) {
		t.Error("Full key should be longer than prefix")
	}
}

func TestCreateRangeKeys(t *testing.T) {
	config := IndexConfig{
		Fields: []FieldConfig{
			{Name: "user_id", Type: FieldTypeString, FixedLength: 16},
			{Name: "timestamp", Type: FieldTypeTimestamp},
		},
		EnableAntiHotspot: true,
		ShardCount:        256,
		Delimiter:         DefaultDelimiter,
	}
	builder := NewCompositeKeyBuilder(config)

	startKey, endKey, err := builder.CreateRangeKeys(
		map[string]interface{}{"user_id": "USER001"},
		"timestamp",
		int64(1704067200000),
		int64(1704153600000),
	)

	if err != nil {
		t.Fatalf("Failed to create range keys: %v", err)
	}

	if string(startKey) >= string(endKey) {
		t.Error("Start key should be less than end key")
	}
}

func TestIncrementKey(t *testing.T) {
	key1 := []byte{0x00, 0x01, 0x02}
	key2 := IncrementKey(key1)

	if string(key2) <= string(key1) {
		t.Error("Incremented key should be greater")
	}
}

func TestIncrementKeyOverflow(t *testing.T) {
	key1 := []byte{0xff, 0xff, 0xff}
	key2 := IncrementKey(key1)

	if string(key2) <= string(key1) {
		t.Error("Incremented key should be greater")
	}
}

func TestDeterministicShardPrefix(t *testing.T) {
	config := IndexConfig{
		Fields: []FieldConfig{
			{Name: "user_id", Type: FieldTypeString, FixedLength: 16},
		},
		EnableAntiHotspot: true,
		ShardCount:        256,
		Delimiter:         DefaultDelimiter,
	}
	builder := NewCompositeKeyBuilder(config)

	key1, _ := builder.CreateKey(map[string]interface{}{"user_id": "USER001"})
	key2, _ := builder.CreateKey(map[string]interface{}{"user_id": "USER001"})

	// Same user should get same shard prefix
	if key1[0] != key2[0] || key1[1] != key2[1] {
		t.Error("Same user should have same shard prefix")
	}
}

// =============================================================================
// BLOOM FILTER TESTS
// =============================================================================

func TestBloomFilterAddAndCheck(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	bf.Add([]byte("test_key_1"))
	bf.Add([]byte("test_key_2"))

	if !bf.MightContain([]byte("test_key_1")) {
		t.Error("Should contain test_key_1")
	}
	if !bf.MightContain([]byte("test_key_2")) {
		t.Error("Should contain test_key_2")
	}
}

func TestBloomFilterNegativeCheck(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	bf.Add([]byte("test_key_1"))

	// Count false negatives (should be 0)
	falseNegatives := 0
	for i := 0; i < 100; i++ {
		if !bf.MightContain([]byte("test_key_1")) {
			falseNegatives++
		}
	}

	if falseNegatives > 0 {
		t.Errorf("Bloom filter should have no false negatives, got %d", falseNegatives)
	}
}

// =============================================================================
// LRU CACHE TESTS
// =============================================================================

func TestLRUCachePutAndGet(t *testing.T) {
	cache := NewLRUCache(10, 60)

	cache.Put([]byte("key1"), map[string]interface{}{"data": "value1"})
	result, ok := cache.Get([]byte("key1"))

	if !ok {
		t.Error("Should get cached value")
	}

	record, ok := result.(map[string]interface{})
	if !ok {
		t.Error("Should be map type")
	}

	if record["data"] != "value1" {
		t.Errorf("Expected 'value1', got %v", record["data"])
	}
}

func TestLRUCacheMiss(t *testing.T) {
	cache := NewLRUCache(10, 60)

	_, ok := cache.Get([]byte("nonexistent"))

	if ok {
		t.Error("Should return false for cache miss")
	}
}

func TestLRUCacheEviction(t *testing.T) {
	cache := NewLRUCache(3, 60)

	cache.Put([]byte("key1"), "value1")
	cache.Put([]byte("key2"), "value2")
	cache.Put([]byte("key3"), "value3")
	cache.Put([]byte("key4"), "value4") // Should evict key1

	_, ok := cache.Get([]byte("key1"))
	if ok {
		t.Error("key1 should be evicted")
	}

	_, ok = cache.Get([]byte("key2"))
	if !ok {
		t.Error("key2 should still be cached")
	}
}

func TestLRUCacheInvalidatePrefix(t *testing.T) {
	cache := NewLRUCache(10, 60)

	cache.Put([]byte("user1_order1"), "v1")
	cache.Put([]byte("user1_order2"), "v2")
	cache.Put([]byte("user2_order1"), "v3")

	invalidated := cache.InvalidatePrefix([]byte("user1"))

	if invalidated != 2 {
		t.Errorf("Expected 2 invalidated, got %d", invalidated)
	}

	_, ok := cache.Get([]byte("user1_order1"))
	if ok {
		t.Error("user1_order1 should be invalidated")
	}

	_, ok = cache.Get([]byte("user2_order1"))
	if !ok {
		t.Error("user2_order1 should still be cached")
	}
}

// =============================================================================
// IN-MEMORY KV STORE TESTS
// =============================================================================

func TestKVStorePutAndGet(t *testing.T) {
	store := NewInMemoryKVStore()

	store.Put([]byte("key1"), []byte("value1"))
	result, err := store.Get([]byte("key1"))

	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(result) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", string(result))
	}
}

func TestKVStoreDelete(t *testing.T) {
	store := NewInMemoryKVStore()

	store.Put([]byte("key1"), []byte("value1"))
	store.Delete([]byte("key1"))

	result, _ := store.Get([]byte("key1"))
	if result != nil {
		t.Error("Should return nil after delete")
	}
}

func TestKVStoreRangeScan(t *testing.T) {
	store := NewInMemoryKVStore()

	store.Put([]byte("a"), []byte("1"))
	store.Put([]byte("b"), []byte("2"))
	store.Put([]byte("c"), []byte("3"))
	store.Put([]byte("d"), []byte("4"))

	results, err := store.RangeScan([]byte("b"), []byte("d"), 10)
	if err != nil {
		t.Fatalf("RangeScan failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	if string(results[0].Key) != "b" || string(results[0].Value) != "2" {
		t.Error("First result should be b:2")
	}
}

func TestKVStorePrefixScan(t *testing.T) {
	store := NewInMemoryKVStore()

	store.Put([]byte("user1_a"), []byte("1"))
	store.Put([]byte("user1_b"), []byte("2"))
	store.Put([]byte("user2_a"), []byte("3"))

	results, err := store.PrefixScan([]byte("user1"), 10)
	if err != nil {
		t.Fatalf("PrefixScan failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

// =============================================================================
// COMPOSITE KEY INDEX INTEGRATION TESTS
// =============================================================================

func TestIndexPutAndExactLookup(t *testing.T) {
	config := IndexConfig{
		Fields: []FieldConfig{
			{Name: "user_id", Type: FieldTypeString, FixedLength: 16},
			{Name: "order_date", Type: FieldTypeTimestamp},
			{Name: "order_id", Type: FieldTypeUUID},
		},
		EnableAntiHotspot:  true,
		ShardCount:         256,
		Delimiter:          DefaultDelimiter,
		BloomFilterEnabled: true,
		BloomExpectedItems: 1000,
		BloomFPRate:        0.01,
		CacheEnabled:       true,
		CacheMaxSize:       100,
		CacheTTLSeconds:    60,
		WriteBufferSize:    10,
	}

	index := NewCompositeKeyIndex(config, nil)

	record := map[string]interface{}{
		"user_id":    "USER001",
		"order_date": int64(1704067200000),
		"order_id":   "550e8400-e29b-41d4-a716-446655440000",
		"amount":     99.99,
	}

	_, err := index.Put(record)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	index.Flush()

	result, err := index.ExactLookup(map[string]interface{}{
		"user_id":    "USER001",
		"order_date": int64(1704067200000),
		"order_id":   "550e8400-e29b-41d4-a716-446655440000",
	})
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result should not be nil")
	}

	if result["amount"] != 99.99 {
		t.Errorf("Expected amount 99.99, got %v", result["amount"])
	}
}

func TestIndexPrefixScan(t *testing.T) {
	config := IndexConfig{
		Fields: []FieldConfig{
			{Name: "user_id", Type: FieldTypeString, FixedLength: 16},
			{Name: "order_date", Type: FieldTypeTimestamp},
			{Name: "order_id", Type: FieldTypeUUID},
		},
		EnableAntiHotspot:  true,
		ShardCount:         256,
		Delimiter:          DefaultDelimiter,
		BloomFilterEnabled: true,
		CacheEnabled:       true,
		WriteBufferSize:    100,
	}

	index := NewCompositeKeyIndex(config, nil)

	// Insert multiple orders for same user
	for i := 0; i < 5; i++ {
		index.Put(map[string]interface{}{
			"user_id":    "USER001",
			"order_date": int64(1704067200000 + int64(i)*3600000),
			"order_id":   uuid.New().String(),
			"amount":     float64(100 + i),
		})
	}

	// Insert order for different user
	index.Put(map[string]interface{}{
		"user_id":    "USER002",
		"order_date": int64(1704067200000),
		"order_id":   uuid.New().String(),
		"amount":     50.0,
	})

	index.Flush()

	// Scan for USER001
	result, err := index.PrefixScan(map[string]interface{}{
		"user_id": "USER001",
	}, 100)
	if err != nil {
		t.Fatalf("PrefixScan failed: %v", err)
	}

	if len(result.Records) != 5 {
		t.Errorf("Expected 5 records, got %d", len(result.Records))
	}
}

func TestIndexRangeScan(t *testing.T) {
	config := IndexConfig{
		Fields: []FieldConfig{
			{Name: "user_id", Type: FieldTypeString, FixedLength: 16},
			{Name: "order_date", Type: FieldTypeTimestamp},
			{Name: "order_id", Type: FieldTypeUUID},
		},
		EnableAntiHotspot:  true,
		ShardCount:         256,
		Delimiter:          DefaultDelimiter,
		BloomFilterEnabled: false,
		CacheEnabled:       false,
		WriteBufferSize:    100,
	}

	index := NewCompositeKeyIndex(config, nil)

	baseTime := int64(1704067200000)

	// Insert orders across time range
	for i := 0; i < 10; i++ {
		index.Put(map[string]interface{}{
			"user_id":    "USER001",
			"order_date": baseTime + int64(i)*86400000, // Daily
			"order_id":   uuid.New().String(),
			"day":        i,
		})
	}

	index.Flush()

	// Range query for days 2-5
	result, err := index.RangeScan(
		map[string]interface{}{"user_id": "USER001"},
		"order_date",
		baseTime+2*86400000,
		baseTime+5*86400000,
		100,
	)
	if err != nil {
		t.Fatalf("RangeScan failed: %v", err)
	}

	if len(result.Records) != 4 {
		t.Errorf("Expected 4 records (days 2,3,4,5), got %d", len(result.Records))
	}
}

func TestIndexBloomFilterNegative(t *testing.T) {
	config := IndexConfig{
		Fields: []FieldConfig{
			{Name: "user_id", Type: FieldTypeString, FixedLength: 16},
			{Name: "order_date", Type: FieldTypeTimestamp},
			{Name: "order_id", Type: FieldTypeUUID},
		},
		EnableAntiHotspot:  true,
		ShardCount:         256,
		Delimiter:          DefaultDelimiter,
		BloomFilterEnabled: true,
		BloomExpectedItems: 1000,
		BloomFPRate:        0.01,
		CacheEnabled:       false,
		WriteBufferSize:    10,
	}

	index := NewCompositeKeyIndex(config, nil)

	index.Put(map[string]interface{}{
		"user_id":    "USER001",
		"order_date": int64(1704067200000),
		"order_id":   "550e8400-e29b-41d4-a716-446655440000",
	})
	index.Flush()

	// Lookup non-existent
	result, err := index.ExactLookup(map[string]interface{}{
		"user_id":    "NONEXISTENT",
		"order_date": int64(1704067200000),
		"order_id":   "550e8400-e29b-41d4-a716-446655440099",
	})
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}

	if result != nil {
		t.Error("Result should be nil for non-existent key")
	}
}

func TestIndexDelete(t *testing.T) {
	config := IndexConfig{
		Fields: []FieldConfig{
			{Name: "user_id", Type: FieldTypeString, FixedLength: 16},
			{Name: "order_date", Type: FieldTypeTimestamp},
			{Name: "order_id", Type: FieldTypeUUID},
		},
		EnableAntiHotspot:  true,
		ShardCount:         256,
		Delimiter:          DefaultDelimiter,
		BloomFilterEnabled: false,
		CacheEnabled:       true,
		WriteBufferSize:    10,
	}

	index := NewCompositeKeyIndex(config, nil)

	lookupKey := map[string]interface{}{
		"user_id":    "USER001",
		"order_date": int64(1704067200000),
		"order_id":   "550e8400-e29b-41d4-a716-446655440000",
	}

	index.Put(lookupKey)
	index.Flush()

	// Delete
	err := index.Delete(lookupKey)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	result, _ := index.ExactLookup(lookupKey)
	if result != nil {
		t.Error("Result should be nil after delete")
	}
}

// =============================================================================
// BENCHMARK TESTS
// =============================================================================

func BenchmarkPut(b *testing.B) {
	config := IndexConfig{
		Fields: []FieldConfig{
			{Name: "user_id", Type: FieldTypeString, FixedLength: 16},
			{Name: "order_date", Type: FieldTypeTimestamp},
			{Name: "order_id", Type: FieldTypeUUID},
		},
		EnableAntiHotspot:  true,
		ShardCount:         256,
		Delimiter:          DefaultDelimiter,
		BloomFilterEnabled: true,
		BloomExpectedItems: 100000,
		CacheEnabled:       true,
		WriteBufferSize:    1000,
	}

	index := NewCompositeKeyIndex(config, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.Put(map[string]interface{}{
			"user_id":    "USER001",
			"order_date": int64(1704067200000 + int64(i)),
			"order_id":   uuid.New().String(),
			"amount":     99.99,
		})
	}
}

func BenchmarkExactLookup(b *testing.B) {
	config := IndexConfig{
		Fields: []FieldConfig{
			{Name: "user_id", Type: FieldTypeString, FixedLength: 16},
			{Name: "order_date", Type: FieldTypeTimestamp},
			{Name: "order_id", Type: FieldTypeUUID},
		},
		EnableAntiHotspot:  true,
		ShardCount:         256,
		Delimiter:          DefaultDelimiter,
		BloomFilterEnabled: true,
		BloomExpectedItems: 100000,
		CacheEnabled:       true,
		WriteBufferSize:    1000,
	}

	index := NewCompositeKeyIndex(config, nil)

	// Pre-populate
	for i := 0; i < 10000; i++ {
		index.Put(map[string]interface{}{
			"user_id":    "USER001",
			"order_date": int64(1704067200000 + int64(i)),
			"order_id":   uuid.New().String(),
			"amount":     99.99,
		})
	}
	index.Flush()

	lookupKey := map[string]interface{}{
		"user_id":    "USER001",
		"order_date": int64(1704067200000),
		"order_id":   "550e8400-e29b-41d4-a716-446655440000",
	}

	// Warm up cache
	index.Put(map[string]interface{}{
		"user_id":    "USER001",
		"order_date": int64(1704067200000),
		"order_id":   "550e8400-e29b-41d4-a716-446655440000",
		"amount":     99.99,
	})
	index.Flush()
	index.ExactLookup(lookupKey)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.ExactLookup(lookupKey)
	}
}

func BenchmarkPrefixScan(b *testing.B) {
	config := IndexConfig{
		Fields: []FieldConfig{
			{Name: "user_id", Type: FieldTypeString, FixedLength: 16},
			{Name: "order_date", Type: FieldTypeTimestamp},
			{Name: "order_id", Type: FieldTypeUUID},
		},
		EnableAntiHotspot:  true,
		ShardCount:         256,
		Delimiter:          DefaultDelimiter,
		BloomFilterEnabled: false,
		CacheEnabled:       false,
		WriteBufferSize:    1000,
	}

	index := NewCompositeKeyIndex(config, nil)

	// Pre-populate
	for i := 0; i < 1000; i++ {
		index.Put(map[string]interface{}{
			"user_id":    "USER001",
			"order_date": int64(1704067200000 + int64(i)*3600000),
			"order_id":   uuid.New().String(),
			"amount":     99.99,
		})
	}
	index.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.PrefixScan(map[string]interface{}{
			"user_id": "USER001",
		}, 100)
	}
}
