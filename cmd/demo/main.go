// Demo application for Composite Key Indexing
package main

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	compositekey "composite_key_indexing"
)

func main() {
	fmt.Println("======================================================================")
	fmt.Println("COMPOSITE KEY INDEXING DEMONSTRATION (Go)")
	fmt.Println("======================================================================")

	// Define schema for order management system
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
		BloomExpectedItems: 10000,
		BloomFPRate:        0.01,
		CacheEnabled:       true,
		CacheMaxSize:       1000,
		CacheTTLSeconds:    300,
		WriteBufferSize:    100,
	}

	// Create index
	index := compositekey.NewCompositeKeyIndex(config, nil)

	// Insert sample data
	fmt.Println("\n[1] Inserting sample orders...")

	orders := []map[string]interface{}{
		{
			"user_id":    "USR_12345",
			"order_date": int64(1737100800000), // 2025-01-17
			"order_id":   "550e8400-e29b-41d4-a716-446655440001",
			"amount":     99.99,
			"status":     "CONFIRMED",
		},
		{
			"user_id":    "USR_12345",
			"order_date": int64(1737187200000), // 2025-01-18
			"order_id":   "550e8400-e29b-41d4-a716-446655440002",
			"amount":     149.99,
			"status":     "SHIPPED",
		},
		{
			"user_id":    "USR_12345",
			"order_date": int64(1737273600000), // 2025-01-19
			"order_id":   "550e8400-e29b-41d4-a716-446655440003",
			"amount":     299.99,
			"status":     "PENDING",
		},
		{
			"user_id":    "USR_67890",
			"order_date": int64(1737100800000), // 2025-01-17
			"order_id":   "550e8400-e29b-41d4-a716-446655440004",
			"amount":     59.99,
			"status":     "DELIVERED",
		},
	}

	for _, order := range orders {
		key, err := index.Put(order)
		if err != nil {
			fmt.Printf("  Error inserting: %v\n", err)
			continue
		}
		fmt.Printf("  Inserted: %s... -> Key: %x...\n",
			order["order_id"].(string)[:8],
			key[:16])
	}

	// Flush to storage
	index.Flush()

	// Query 1: Exact lookup
	fmt.Println("\n[2] Exact Lookup: user=USR_12345, date=1737100800000, order=550e8400...")
	result, err := index.ExactLookup(map[string]interface{}{
		"user_id":    "USR_12345",
		"order_date": int64(1737100800000),
		"order_id":   "550e8400-e29b-41d4-a716-446655440001",
	})
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  Result: %v\n", result)
	}

	// Query 2: Prefix scan (all orders for user)
	fmt.Println("\n[3] Prefix Scan: All orders for user=USR_12345")
	queryResult, err := index.PrefixScan(map[string]interface{}{
		"user_id": "USR_12345",
	}, 100)
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  Found %d records (scanned: %d)\n", len(queryResult.Records), queryResult.ScanCount)
		for _, r := range queryResult.Records {
			fmt.Printf("    - Order %s... Amount: $%.2f\n",
				r["order_id"].(string)[:8],
				r["amount"].(float64))
		}
	}

	// Query 3: Range scan
	fmt.Println("\n[4] Range Scan: user=USR_12345, date BETWEEN 1737100800000 AND 1737200000000")
	queryResult, err = index.RangeScan(
		map[string]interface{}{"user_id": "USR_12345"},
		"order_date",
		int64(1737100800000),
		int64(1737200000000),
		100,
	)
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  Found %d records (scanned: %d)\n", len(queryResult.Records), queryResult.ScanCount)
		for _, r := range queryResult.Records {
			fmt.Printf("    - Order %s... Date: %.0f\n",
				r["order_id"].(string)[:8],
				r["order_date"].(float64))
		}
	}

	// Demonstrate bloom filter
	fmt.Println("\n[5] Bloom Filter Test: Non-existent key")
	result, err = index.ExactLookup(map[string]interface{}{
		"user_id":    "USR_99999",
		"order_date": int64(1737100800000),
		"order_id":   "550e8400-e29b-41d4-a716-446655440099",
	})
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  Result: %v (bloom filter should skip disk lookup)\n", result)
	}

	// Key encoding example
	fmt.Println("\n[6] Key Encoding Example:")
	builder := compositekey.NewCompositeKeyBuilder(config)
	sampleKey, _ := builder.CreateKey(map[string]interface{}{
		"user_id":    "USR_12345",
		"order_date": int64(1737100800000),
		"order_id":   "550e8400-e29b-41d4-a716-446655440001",
	})
	fmt.Printf("  Raw key length: %d bytes\n", len(sampleKey))
	fmt.Printf("  Key hex: %x\n", sampleKey)

	// Benchmark
	fmt.Println("\n[7] Performance Benchmark:")
	runBenchmark(config)

	fmt.Println("\n======================================================================")
	fmt.Println("DEMONSTRATION COMPLETE")
	fmt.Println("======================================================================")
}

func runBenchmark(config compositekey.IndexConfig) {
	index := compositekey.NewCompositeKeyIndex(config, nil)

	// Write benchmark
	numRecords := 10000
	fmt.Printf("  Inserting %d records...\n", numRecords)

	start := time.Now()
	for i := 0; i < numRecords; i++ {
		index.Put(map[string]interface{}{
			"user_id":    fmt.Sprintf("USR_%05d", i%100),
			"order_date": int64(1704067200000 + int64(i)*3600000),
			"order_id":   uuid.New().String(),
			"amount":     float64(100 + i%1000),
		})
	}
	index.Flush()
	writeTime := time.Since(start)

	fmt.Printf("  Write: %d records in %v (%.0f ops/sec)\n",
		numRecords, writeTime, float64(numRecords)/writeTime.Seconds())

	// Read benchmark
	numReads := 10000
	start = time.Now()
	for i := 0; i < numReads; i++ {
		index.ExactLookup(map[string]interface{}{
			"user_id":    fmt.Sprintf("USR_%05d", i%100),
			"order_date": int64(1704067200000 + int64(i%100)*3600000),
			"order_id":   "550e8400-e29b-41d4-a716-446655440000", // Won't find most
		})
	}
	readTime := time.Since(start)

	fmt.Printf("  Read: %d lookups in %v (%.0f ops/sec)\n",
		numReads, readTime, float64(numReads)/readTime.Seconds())

	// Prefix scan benchmark
	numScans := 1000
	start = time.Now()
	for i := 0; i < numScans; i++ {
		index.PrefixScan(map[string]interface{}{
			"user_id": fmt.Sprintf("USR_%05d", i%100),
		}, 100)
	}
	scanTime := time.Since(start)

	fmt.Printf("  Scan: %d prefix scans in %v (%.0f ops/sec)\n",
		numScans, scanTime, float64(numScans)/scanTime.Seconds())
}
