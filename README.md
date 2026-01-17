# Composite Key Indexing

> High-performance composite key indexing algorithm optimized for NoSQL, Key-Value Store, and Blockchain Ledger systems.

## Features

- **Byte-comparable encoding** - Keys sort correctly via lexicographic comparison
- **Anti-hotspot sharding** - Hash-based prefix distributes writes evenly
- **Bloom filter** - Skip disk lookup for non-existent keys
- **LRU cache with TTL** - Cache hot data with automatic expiration
- **Write buffer** - Batch writes for reduced IO
- **Thread-safe** - Safe for concurrent access

## Query Types Supported

| Query Type | Complexity | Example |
|------------|------------|---------|
| Exact Match | O(log n) | `WHERE user_id = 'X' AND order_id = 'Y'` |
| Prefix Scan | O(log n + k) | `WHERE user_id = 'X'` |
| Range Scan | O(log n + k) | `WHERE user_id = 'X' AND date BETWEEN t1 AND t2` |

## Quick Start

```go
package main

import (
    "fmt"
    compositekey "composite_key_indexing"
)

func main() {
    // Define schema
    config := compositekey.IndexConfig{
        Fields: []compositekey.FieldConfig{
            {Name: "user_id", Type: compositekey.FieldTypeString, FixedLength: 32},
            {Name: "order_date", Type: compositekey.FieldTypeTimestamp},
            {Name: "order_id", Type: compositekey.FieldTypeUUID},
        },
        Delimiter:          compositekey.DefaultDelimiter,
        EnableAntiHotspot:  true,
        ShardCount:         256,
        BloomFilterEnabled: true,
        CacheEnabled:       true,
    }

    // Create index
    index := compositekey.NewCompositeKeyIndex(config, nil)

    // Insert
    index.Put(map[string]interface{}{
        "user_id":    "USR_12345",
        "order_date": int64(1737100800000),
        "order_id":   "550e8400-e29b-41d4-a716-446655440001",
        "amount":     99.99,
    })
    index.Flush()

    // Query - Exact lookup
    result, _ := index.ExactLookup(map[string]interface{}{
        "user_id":    "USR_12345",
        "order_date": int64(1737100800000),
        "order_id":   "550e8400-e29b-41d4-a716-446655440001",
    })
    fmt.Println(result)

    // Query - Prefix scan
    results, _ := index.PrefixScan(map[string]interface{}{
        "user_id": "USR_12345",
    }, 100)

    // Query - Range scan
    results, _ = index.RangeScan(
        map[string]interface{}{"user_id": "USR_12345"},
        "order_date",
        int64(1737100800000),
        int64(1737200000000),
        100,
    )
    _ = results
}
```

## Installation

```bash
go mod tidy
go build ./...
```

## Running Tests

```bash
go test -v ./...
```

## Running Benchmarks

```bash
# Quick benchmark
go test -bench=. -benchmem

# Full benchmark suite with visualization (generates charts in images/)
make bench-suite
```

## Running Demo

```bash
go run ./cmd/demo/main.go
```

## Benchmark Results

After running `make bench-suite`, check the `images/` folder:

| File | Description |
|------|-------------|
| `benchmark_report.html` | Interactive HTML report with charts |
| `benchmark_results_*.json` | Raw benchmark data |
| `chart_*.svg` | SVG bar charts |
| `BENCHMARK_SUMMARY.md` | Markdown summary |

> **Note**: The in-memory KV store is for testing only. For production benchmarks, 
> use RocksDB or BadgerDB backends which provide optimized sorted key storage.

## Performance

| Operation | Throughput | Memory/op |
|-----------|------------|-----------|
| Write | 202,714 ops/s | 1,988 B |
| Point Lookup | 173,641 ops/s | 1,296 B |

## Key Design

```
Composite Key Structure:
┌──────────────┬───────────┬──────────┬───────────┬──────────┐
│ Shard Prefix │ Delimiter │ Field 1  │ Delimiter │ Field N  │
│   (2 bytes)  │  (1 byte) │ (varied) │  (1 byte) │ (varied) │
└──────────────┴───────────┴──────────┴───────────┴──────────┘

Field Encoding:
- STRING:    UTF-8 + delimiter escape + zero padding
- INTEGER:   8 bytes big-endian + sign flip for sort order
- TIMESTAMP: 8 bytes big-endian (milliseconds)
- UUID:      16 bytes raw
- DECIMAL:   Scale to integer → encode as INTEGER
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `EnableAntiHotspot` | `true` | Add hash prefix to distribute writes |
| `ShardCount` | `256` | Number of shards (max 65535) |
| `BloomFilterEnabled` | `true` | Enable bloom filter for negative lookups |
| `BloomExpectedItems` | `1000000` | Expected number of items |
| `BloomFPRate` | `0.01` | False positive rate (1%) |
| `CacheEnabled` | `true` | Enable LRU cache |
| `CacheMaxSize` | `10000` | Max cache entries |
| `CacheTTLSeconds` | `300` | Cache TTL (5 minutes) |
| `WriteBufferSize` | `10000` | Write buffer size before flush |

## Production KV Store Backends

For production use, replace `InMemoryKVStore` with:

| Backend | Use Case |
|---------|----------|
| RocksDB | Single node, high performance |
| BadgerDB | SSD optimized, pure Go |
| TiKV | Distributed, Raft consensus |
| FoundationDB | ACID, distributed |

See `examples/` directory for backend implementations.

## Project Structure

```
.
├── composite_key_index.go      # Main implementation
├── composite_key_index_test.go # Unit tests
├── cmd/demo/main.go            # Demo application
├── examples/
│   ├── rocksdb_backend.go      # RocksDB integration
│   └── badger_backend.go       # BadgerDB integration
├── go.mod                      # Go module
├── go.sum                      # Dependencies
├── Makefile                    # Build commands
├── COMPOSITE_KEY_INDEXING_SPEC.md  # Technical specification
└── README.md                   # This file
```

## License

MIT License
