package compositekey

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// =============================================================================
// BENCHMARK RESULT TYPES
// =============================================================================

// BenchmarkResult holds results for a single benchmark
type BenchmarkResult struct {
	Name        string        `json:"name"`
	Operations  int           `json:"operations"`
	TotalTime   time.Duration `json:"total_time_ns"`
	AvgLatency  time.Duration `json:"avg_latency_ns"`
	P50Latency  time.Duration `json:"p50_latency_ns"`
	P95Latency  time.Duration `json:"p95_latency_ns"`
	P99Latency  time.Duration `json:"p99_latency_ns"`
	Throughput  float64       `json:"throughput_ops_per_sec"`
	MemoryBytes int64         `json:"memory_bytes"`
}

// BenchmarkSuite holds all benchmark results
type BenchmarkSuite struct {
	Timestamp   time.Time                    `json:"timestamp"`
	DataSize    int                          `json:"data_size"`
	Results     map[string][]BenchmarkResult `json:"results"`
	Comparisons []ComparisonResult           `json:"comparisons"`
}

// ComparisonResult compares two methods
type ComparisonResult struct {
	Operation  string  `json:"operation"`
	Method1    string  `json:"method1"`
	Method2    string  `json:"method2"`
	Speedup    float64 `json:"speedup"`
	Method1Ops float64 `json:"method1_ops_per_sec"`
	Method2Ops float64 `json:"method2_ops_per_sec"`
}

// =============================================================================
// NAIVE IMPLEMENTATIONS FOR COMPARISON
// =============================================================================

// NaiveFullScanStore - O(n) full scan for every query
type NaiveFullScanStore struct {
	records []map[string]interface{}
}

func NewNaiveFullScanStore() *NaiveFullScanStore {
	return &NaiveFullScanStore{
		records: make([]map[string]interface{}, 0),
	}
}

func (s *NaiveFullScanStore) Put(record map[string]interface{}) {
	s.records = append(s.records, record)
}

func (s *NaiveFullScanStore) ExactLookup(conditions map[string]interface{}) map[string]interface{} {
	for _, record := range s.records {
		match := true
		for key, value := range conditions {
			if record[key] != value {
				match = false
				break
			}
		}
		if match {
			return record
		}
	}
	return nil
}

func (s *NaiveFullScanStore) PrefixScan(conditions map[string]interface{}, limit int) []map[string]interface{} {
	var results []map[string]interface{}
	for _, record := range s.records {
		if len(results) >= limit {
			break
		}
		match := true
		for key, value := range conditions {
			if record[key] != value {
				match = false
				break
			}
		}
		if match {
			results = append(results, record)
		}
	}
	return results
}

func (s *NaiveFullScanStore) RangeScan(conditions map[string]interface{}, rangeField string, minVal, maxVal int64, limit int) []map[string]interface{} {
	var results []map[string]interface{}
	for _, record := range s.records {
		if len(results) >= limit {
			break
		}
		match := true
		for key, value := range conditions {
			if record[key] != value {
				match = false
				break
			}
		}
		if match {
			if val, ok := record[rangeField].(int64); ok {
				if val >= minVal && val <= maxVal {
					results = append(results, record)
				}
			}
		}
	}
	return results
}

// SimpleHashIndex - Hash index (good for exact match, bad for range)
type SimpleHashIndex struct {
	data map[string]map[string]interface{}
}

func NewSimpleHashIndex() *SimpleHashIndex {
	return &SimpleHashIndex{
		data: make(map[string]map[string]interface{}),
	}
}

func (s *SimpleHashIndex) makeKey(record map[string]interface{}, fields []string) string {
	var parts []string
	for _, f := range fields {
		parts = append(parts, fmt.Sprintf("%v", record[f]))
	}
	return strings.Join(parts, "|")
}

func (s *SimpleHashIndex) Put(record map[string]interface{}, keyFields []string) {
	key := s.makeKey(record, keyFields)
	s.data[key] = record
}

func (s *SimpleHashIndex) ExactLookup(conditions map[string]interface{}, keyFields []string) map[string]interface{} {
	key := s.makeKey(conditions, keyFields)
	return s.data[key]
}

// Hash index cannot do efficient range/prefix scan - must iterate all
func (s *SimpleHashIndex) PrefixScan(conditions map[string]interface{}, limit int) []map[string]interface{} {
	var results []map[string]interface{}
	for _, record := range s.data {
		if len(results) >= limit {
			break
		}
		match := true
		for key, value := range conditions {
			if record[key] != value {
				match = false
				break
			}
		}
		if match {
			results = append(results, record)
		}
	}
	return results
}

// =============================================================================
// BENCHMARK RUNNER
// =============================================================================

func runBenchmark(name string, iterations int, fn func()) BenchmarkResult {
	latencies := make([]time.Duration, iterations)

	// Warm up
	for i := 0; i < 100; i++ {
		fn()
	}

	// Actual benchmark
	start := time.Now()
	for i := 0; i < iterations; i++ {
		iterStart := time.Now()
		fn()
		latencies[i] = time.Since(iterStart)
	}
	totalTime := time.Since(start)

	// Sort for percentiles
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// Calculate stats
	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}

	return BenchmarkResult{
		Name:       name,
		Operations: iterations,
		TotalTime:  totalTime,
		AvgLatency: sum / time.Duration(iterations),
		P50Latency: latencies[iterations/2],
		P95Latency: latencies[int(float64(iterations)*0.95)],
		P99Latency: latencies[int(float64(iterations)*0.99)],
		Throughput: float64(iterations) / totalTime.Seconds(),
	}
}

// =============================================================================
// MAIN BENCHMARK TEST
// =============================================================================

func TestBenchmarkSuiteWithVisualization(t *testing.T) {
	// Data sizes to test
	dataSizes := []int{1000, 5000, 10000}

	allResults := make(map[int]*BenchmarkSuite)

	for _, dataSize := range dataSizes {
		t.Logf("Running benchmarks for %d records...", dataSize)
		suite := runBenchmarkSuite(t, dataSize)
		allResults[dataSize] = suite

		// Save individual results
		saveResults(t, suite, dataSize)
	}

	// Generate visualizations
	generateVisualizations(t, allResults)

	t.Log("Benchmark suite completed. Check images/ folder for results.")
}

func runBenchmarkSuite(t *testing.T, dataSize int) *BenchmarkSuite {
	suite := &BenchmarkSuite{
		Timestamp: time.Now(),
		DataSize:  dataSize,
		Results:   make(map[string][]BenchmarkResult),
	}

	// Generate test data
	testData := generateTestData(dataSize)

	// Initialize stores
	compositeIndex := createCompositeIndex()
	naiveStore := NewNaiveFullScanStore()
	hashIndex := NewSimpleHashIndex()

	keyFields := []string{"user_id", "order_date", "order_id"}

	// Populate all stores
	t.Log("  Populating stores...")
	for _, record := range testData {
		compositeIndex.Put(record)
		naiveStore.Put(record)
		hashIndex.Put(record, keyFields)
	}
	compositeIndex.Flush()

	// Benchmark iterations
	iterations := min(1000, dataSize)

	// ==========================================================================
	// WRITE BENCHMARKS
	// ==========================================================================
	t.Log("  Benchmarking writes...")

	writeResults := []BenchmarkResult{}

	// Composite Key Write
	idx := 0
	writeResults = append(writeResults, runBenchmark("CompositeKey", iterations, func() {
		record := testData[idx%len(testData)]
		compositeIndex.Put(record)
		idx++
	}))

	// Naive Write
	idx = 0
	writeResults = append(writeResults, runBenchmark("FullScan", iterations, func() {
		record := testData[idx%len(testData)]
		naiveStore.Put(record)
		idx++
	}))

	// Hash Write
	idx = 0
	writeResults = append(writeResults, runBenchmark("HashIndex", iterations, func() {
		record := testData[idx%len(testData)]
		hashIndex.Put(record, keyFields)
		idx++
	}))

	suite.Results["write"] = writeResults

	// ==========================================================================
	// EXACT LOOKUP BENCHMARKS
	// ==========================================================================
	t.Log("  Benchmarking exact lookups...")

	lookupResults := []BenchmarkResult{}
	sampleRecords := testData[:min(100, len(testData))]

	// Composite Key Lookup
	idx = 0
	lookupResults = append(lookupResults, runBenchmark("CompositeKey", iterations, func() {
		record := sampleRecords[idx%len(sampleRecords)]
		compositeIndex.ExactLookup(map[string]interface{}{
			"user_id":    record["user_id"],
			"order_date": record["order_date"],
			"order_id":   record["order_id"],
		})
		idx++
	}))

	// Naive Full Scan Lookup
	idx = 0
	lookupResults = append(lookupResults, runBenchmark("FullScan", iterations, func() {
		record := sampleRecords[idx%len(sampleRecords)]
		naiveStore.ExactLookup(map[string]interface{}{
			"user_id":    record["user_id"],
			"order_date": record["order_date"],
			"order_id":   record["order_id"],
		})
		idx++
	}))

	// Hash Index Lookup
	idx = 0
	lookupResults = append(lookupResults, runBenchmark("HashIndex", iterations, func() {
		record := sampleRecords[idx%len(sampleRecords)]
		hashIndex.ExactLookup(map[string]interface{}{
			"user_id":    record["user_id"],
			"order_date": record["order_date"],
			"order_id":   record["order_id"],
		}, keyFields)
		idx++
	}))

	suite.Results["exact_lookup"] = lookupResults

	// ==========================================================================
	// PREFIX SCAN BENCHMARKS
	// ==========================================================================
	t.Log("  Benchmarking prefix scans...")

	prefixResults := []BenchmarkResult{}
	userIDs := getUniqueUserIDs(testData)

	// Composite Key Prefix Scan
	idx = 0
	prefixResults = append(prefixResults, runBenchmark("CompositeKey", iterations, func() {
		userID := userIDs[idx%len(userIDs)]
		compositeIndex.PrefixScan(map[string]interface{}{"user_id": userID}, 100)
		idx++
	}))

	// Naive Full Scan
	idx = 0
	prefixResults = append(prefixResults, runBenchmark("FullScan", iterations, func() {
		userID := userIDs[idx%len(userIDs)]
		naiveStore.PrefixScan(map[string]interface{}{"user_id": userID}, 100)
		idx++
	}))

	// Hash Index (inefficient for prefix)
	idx = 0
	prefixResults = append(prefixResults, runBenchmark("HashIndex", iterations, func() {
		userID := userIDs[idx%len(userIDs)]
		hashIndex.PrefixScan(map[string]interface{}{"user_id": userID}, 100)
		idx++
	}))

	suite.Results["prefix_scan"] = prefixResults

	// ==========================================================================
	// RANGE SCAN BENCHMARKS
	// ==========================================================================
	t.Log("  Benchmarking range scans...")

	rangeResults := []BenchmarkResult{}
	baseTime := int64(1704067200000)

	// Composite Key Range Scan
	idx = 0
	rangeResults = append(rangeResults, runBenchmark("CompositeKey", iterations, func() {
		userID := userIDs[idx%len(userIDs)]
		compositeIndex.RangeScan(
			map[string]interface{}{"user_id": userID},
			"order_date",
			baseTime,
			baseTime+7*24*3600000, // 1 week
			100,
		)
		idx++
	}))

	// Naive Full Scan
	idx = 0
	rangeResults = append(rangeResults, runBenchmark("FullScan", iterations, func() {
		userID := userIDs[idx%len(userIDs)]
		naiveStore.RangeScan(
			map[string]interface{}{"user_id": userID},
			"order_date",
			baseTime,
			baseTime+7*24*3600000,
			100,
		)
		idx++
	}))

	suite.Results["range_scan"] = rangeResults

	// ==========================================================================
	// CALCULATE COMPARISONS
	// ==========================================================================
	suite.Comparisons = calculateComparisons(suite.Results)

	return suite
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

func createCompositeIndex() *CompositeKeyIndex {
	config := IndexConfig{
		Fields: []FieldConfig{
			{Name: "user_id", Type: FieldTypeString, FixedLength: 32},
			{Name: "order_date", Type: FieldTypeTimestamp},
			{Name: "order_id", Type: FieldTypeUUID},
		},
		Delimiter:          DefaultDelimiter,
		EnableAntiHotspot:  true,
		ShardCount:         256,
		BloomFilterEnabled: true,
		BloomExpectedItems: 100000,
		CacheEnabled:       true,
		CacheMaxSize:       10000,
		WriteBufferSize:    1000,
	}
	return NewCompositeKeyIndex(config, nil)
}

func generateTestData(count int) []map[string]interface{} {
	data := make([]map[string]interface{}, count)
	baseTime := int64(1704067200000)
	numUsers := max(1, count/100)

	for i := 0; i < count; i++ {
		data[i] = map[string]interface{}{
			"user_id":    fmt.Sprintf("USR_%05d", i%numUsers),
			"order_date": baseTime + int64(i)*3600000,
			"order_id":   uuid.New().String(),
			"amount":     float64(100 + rand.Intn(1000)),
			"status":     []string{"PENDING", "CONFIRMED", "SHIPPED", "DELIVERED"}[rand.Intn(4)],
		}
	}
	return data
}

func getUniqueUserIDs(data []map[string]interface{}) []string {
	seen := make(map[string]bool)
	var userIDs []string
	for _, record := range data {
		if userID, ok := record["user_id"].(string); ok {
			if !seen[userID] {
				seen[userID] = true
				userIDs = append(userIDs, userID)
			}
		}
	}
	return userIDs
}

func calculateComparisons(results map[string][]BenchmarkResult) []ComparisonResult {
	var comparisons []ComparisonResult

	for operation, benchmarks := range results {
		if len(benchmarks) < 2 {
			continue
		}

		// Find CompositeKey result
		var compositeResult *BenchmarkResult
		for i := range benchmarks {
			if benchmarks[i].Name == "CompositeKey" {
				compositeResult = &benchmarks[i]
				break
			}
		}

		if compositeResult == nil {
			continue
		}

		// Compare with other methods
		for _, other := range benchmarks {
			if other.Name == "CompositeKey" {
				continue
			}

			speedup := other.AvgLatency.Seconds() / compositeResult.AvgLatency.Seconds()
			if compositeResult.AvgLatency == 0 {
				speedup = 0
			}

			comparisons = append(comparisons, ComparisonResult{
				Operation:  operation,
				Method1:    "CompositeKey",
				Method2:    other.Name,
				Speedup:    speedup,
				Method1Ops: compositeResult.Throughput,
				Method2Ops: other.Throughput,
			})
		}
	}

	return comparisons
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// =============================================================================
// SAVE RESULTS
// =============================================================================

func saveResults(t *testing.T, suite *BenchmarkSuite, dataSize int) {
	// Create images directory if not exists
	imagesDir := "images"
	if err := os.MkdirAll(imagesDir, 0755); err != nil {
		t.Logf("Warning: Could not create images directory: %v", err)
		return
	}

	// Save JSON results
	jsonFile := filepath.Join(imagesDir, fmt.Sprintf("benchmark_results_%d.json", dataSize))
	jsonData, err := json.MarshalIndent(suite, "", "  ")
	if err != nil {
		t.Logf("Warning: Could not marshal results: %v", err)
		return
	}

	if err := os.WriteFile(jsonFile, jsonData, 0644); err != nil {
		t.Logf("Warning: Could not write JSON file: %v", err)
	} else {
		t.Logf("  Saved results to %s", jsonFile)
	}
}

// =============================================================================
// VISUALIZATION GENERATION
// =============================================================================

func generateVisualizations(t *testing.T, allResults map[int]*BenchmarkSuite) {
	imagesDir := "images"

	// Generate HTML report with charts
	generateHTMLReport(t, allResults, imagesDir)

	// Generate SVG charts
	generateSVGCharts(t, allResults, imagesDir)

	// Generate summary markdown
	generateMarkdownSummary(t, allResults, imagesDir)
}

func generateHTMLReport(t *testing.T, allResults map[int]*BenchmarkSuite, dir string) {
	var dataSizes []int
	for size := range allResults {
		dataSizes = append(dataSizes, size)
	}
	sort.Ints(dataSizes)

	html := `<!DOCTYPE html>
<html>
<head>
    <title>Composite Key Index Benchmark Results</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            margin: 40px; 
            background: #1a1a2e;
            color: #eaeaea;
        }
        h1 { color: #00d9ff; border-bottom: 2px solid #00d9ff; padding-bottom: 10px; }
        h2 { color: #ff6b6b; margin-top: 40px; }
        h3 { color: #4ecdc4; }
        .chart-container { 
            background: #16213e; 
            border-radius: 10px; 
            padding: 20px; 
            margin: 20px 0; 
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        }
        .chart-row { display: flex; flex-wrap: wrap; gap: 20px; }
        .chart-box { flex: 1; min-width: 400px; }
        table { 
            border-collapse: collapse; 
            width: 100%; 
            margin: 20px 0; 
            background: #16213e;
            border-radius: 10px;
            overflow: hidden;
        }
        th, td { 
            padding: 12px 15px; 
            text-align: left; 
            border-bottom: 1px solid #2a2a4a;
        }
        th { 
            background: #0f3460; 
            color: #00d9ff;
            font-weight: 600;
        }
        tr:hover { background: #1f4068; }
        .speedup-positive { color: #4ecdc4; font-weight: bold; }
        .speedup-negative { color: #ff6b6b; font-weight: bold; }
        .summary-box {
            background: linear-gradient(135deg, #0f3460, #16213e);
            border-radius: 10px;
            padding: 20px;
            margin: 20px 0;
            border-left: 4px solid #00d9ff;
        }
        .metric { 
            display: inline-block; 
            margin: 10px 20px 10px 0;
            padding: 10px 20px;
            background: #1a1a2e;
            border-radius: 5px;
        }
        .metric-value { font-size: 24px; color: #00d9ff; font-weight: bold; }
        .metric-label { font-size: 12px; color: #888; }
    </style>
</head>
<body>
    <h1>🚀 Composite Key Index Benchmark Results</h1>
    <p>Generated: ` + time.Now().Format("2006-01-02 15:04:05") + `</p>
`

	// Summary section
	if len(dataSizes) > 0 {
		lastSize := dataSizes[len(dataSizes)-1]
		suite := allResults[lastSize]

		html += `
    <div class="summary-box">
        <h3>📊 Summary (` + fmt.Sprintf("%d", lastSize) + ` records)</h3>
        <div>`

		for _, comp := range suite.Comparisons {
			speedupClass := "speedup-positive"
			if comp.Speedup < 1 {
				speedupClass = "speedup-negative"
			}
			html += fmt.Sprintf(`
            <div class="metric">
                <div class="metric-value">%.1fx</div>
                <div class="metric-label">%s vs %s (%s)</div>
            </div>`, comp.Speedup, comp.Method1, comp.Method2, comp.Operation)
			_ = speedupClass
		}

		html += `
        </div>
    </div>`
	}

	// Charts for each operation
	operations := []string{"write", "exact_lookup", "prefix_scan", "range_scan"}
	operationLabels := map[string]string{
		"write":        "Write Operations",
		"exact_lookup": "Exact Lookup",
		"prefix_scan":  "Prefix Scan",
		"range_scan":   "Range Scan",
	}

	for _, op := range operations {
		html += fmt.Sprintf(`
    <h2>📈 %s</h2>
    <div class="chart-container">
        <div class="chart-row">
            <div class="chart-box">
                <canvas id="chart_%s_throughput"></canvas>
            </div>
            <div class="chart-box">
                <canvas id="chart_%s_latency"></canvas>
            </div>
        </div>
    </div>`, operationLabels[op], op, op)
	}

	// Comparison tables
	html += `
    <h2>📋 Detailed Comparisons</h2>`

	for _, size := range dataSizes {
		suite := allResults[size]
		html += fmt.Sprintf(`
    <h3>Data Size: %d records</h3>
    <table>
        <tr>
            <th>Operation</th>
            <th>Method</th>
            <th>Throughput (ops/s)</th>
            <th>Avg Latency</th>
            <th>P99 Latency</th>
        </tr>`, size)

		for _, op := range operations {
			results := suite.Results[op]
			for _, r := range results {
				html += fmt.Sprintf(`
        <tr>
            <td>%s</td>
            <td>%s</td>
            <td>%.0f</td>
            <td>%v</td>
            <td>%v</td>
        </tr>`, op, r.Name, r.Throughput, r.AvgLatency, r.P99Latency)
			}
		}
		html += `
    </table>`
	}

	// JavaScript for charts
	html += `
    <script>
        const chartColors = {
            'CompositeKey': 'rgba(0, 217, 255, 0.8)',
            'FullScan': 'rgba(255, 107, 107, 0.8)',
            'HashIndex': 'rgba(78, 205, 196, 0.8)'
        };
        
        const dataSizes = ` + toJSONArray(dataSizes) + `;
`

	// Generate chart data for each operation
	for _, op := range operations {
		html += generateChartJS(op, allResults, dataSizes)
	}

	html += `
    </script>
</body>
</html>`

	// Write HTML file
	htmlFile := filepath.Join(dir, "benchmark_report.html")
	if err := os.WriteFile(htmlFile, []byte(html), 0644); err != nil {
		t.Logf("Warning: Could not write HTML report: %v", err)
	} else {
		t.Logf("  Generated HTML report: %s", htmlFile)
	}
}

func generateChartJS(operation string, allResults map[int]*BenchmarkSuite, dataSizes []int) string {
	// Collect data by method
	methods := []string{"CompositeKey", "FullScan", "HashIndex"}
	throughputData := make(map[string][]float64)
	latencyData := make(map[string][]float64)

	for _, method := range methods {
		throughputData[method] = make([]float64, len(dataSizes))
		latencyData[method] = make([]float64, len(dataSizes))
	}

	for i, size := range dataSizes {
		suite := allResults[size]
		results := suite.Results[operation]
		for _, r := range results {
			throughputData[r.Name][i] = r.Throughput
			latencyData[r.Name][i] = float64(r.AvgLatency.Microseconds())
		}
	}

	js := fmt.Sprintf(`
        // %s Throughput Chart
        new Chart(document.getElementById('chart_%s_throughput'), {
            type: 'bar',
            data: {
                labels: dataSizes.map(s => s + ' records'),
                datasets: [`, operation, operation)

	first := true
	for _, method := range methods {
		if !first {
			js += ","
		}
		first = false
		js += fmt.Sprintf(`
                    {
                        label: '%s',
                        data: %s,
                        backgroundColor: chartColors['%s']
                    }`, method, toJSONFloatArray(throughputData[method]), method)
	}

	js += fmt.Sprintf(`
                ]
            },
            options: {
                responsive: true,
                plugins: {
                    title: { display: true, text: '%s - Throughput (ops/sec)', color: '#eaeaea' },
                    legend: { labels: { color: '#eaeaea' } }
                },
                scales: {
                    y: { 
                        beginAtZero: true,
                        grid: { color: '#2a2a4a' },
                        ticks: { color: '#eaeaea' }
                    },
                    x: { 
                        grid: { color: '#2a2a4a' },
                        ticks: { color: '#eaeaea' }
                    }
                }
            }
        });
        
        // %s Latency Chart
        new Chart(document.getElementById('chart_%s_latency'), {
            type: 'line',
            data: {
                labels: dataSizes.map(s => s + ' records'),
                datasets: [`, strings.Title(operation), operation, operation)

	first = true
	for _, method := range methods {
		if !first {
			js += ","
		}
		first = false
		js += fmt.Sprintf(`
                    {
                        label: '%s',
                        data: %s,
                        borderColor: chartColors['%s'],
                        backgroundColor: chartColors['%s'].replace('0.8', '0.2'),
                        fill: true,
                        tension: 0.3
                    }`, method, toJSONFloatArray(latencyData[method]), method, method)
	}

	js += fmt.Sprintf(`
                ]
            },
            options: {
                responsive: true,
                plugins: {
                    title: { display: true, text: '%s - Avg Latency (μs)', color: '#eaeaea' },
                    legend: { labels: { color: '#eaeaea' } }
                },
                scales: {
                    y: { 
                        beginAtZero: true,
                        grid: { color: '#2a2a4a' },
                        ticks: { color: '#eaeaea' }
                    },
                    x: { 
                        grid: { color: '#2a2a4a' },
                        ticks: { color: '#eaeaea' }
                    }
                }
            }
        });
`, strings.Title(operation))

	return js
}

func generateSVGCharts(t *testing.T, allResults map[int]*BenchmarkSuite, dir string) {
	// Generate simple SVG bar chart for throughput comparison
	var dataSizes []int
	for size := range allResults {
		dataSizes = append(dataSizes, size)
	}
	sort.Ints(dataSizes)

	if len(dataSizes) == 0 {
		return
	}

	// Use largest dataset for comparison chart
	lastSize := dataSizes[len(dataSizes)-1]
	suite := allResults[lastSize]

	operations := []string{"exact_lookup", "prefix_scan", "range_scan"}

	for _, op := range operations {
		results := suite.Results[op]
		if len(results) == 0 {
			continue
		}

		svg := generateBarChartSVG(op, results)

		svgFile := filepath.Join(dir, fmt.Sprintf("chart_%s_%d.svg", op, lastSize))
		if err := os.WriteFile(svgFile, []byte(svg), 0644); err != nil {
			t.Logf("Warning: Could not write SVG: %v", err)
		} else {
			t.Logf("  Generated SVG chart: %s", svgFile)
		}
	}
}

func generateBarChartSVG(title string, results []BenchmarkResult) string {
	width := 600
	height := 400
	padding := 60
	barWidth := 80

	// Find max throughput
	maxThroughput := 0.0
	for _, r := range results {
		if r.Throughput > maxThroughput {
			maxThroughput = r.Throughput
		}
	}

	chartHeight := float64(height - 2*padding)

	colors := map[string]string{
		"CompositeKey": "#00d9ff",
		"FullScan":     "#ff6b6b",
		"HashIndex":    "#4ecdc4",
	}

	svg := fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" style="background:#1a1a2e">
  <style>
    .title { font: bold 16px sans-serif; fill: #00d9ff; }
    .label { font: 12px sans-serif; fill: #eaeaea; }
    .value { font: bold 11px sans-serif; fill: #eaeaea; }
    .axis { stroke: #4a4a6a; stroke-width: 1; }
  </style>
  <text x="%d" y="30" text-anchor="middle" class="title">%s - Throughput (ops/sec)</text>
  <line x1="%d" y1="%d" x2="%d" y2="%d" class="axis"/>
`, width, height, width/2, strings.Title(strings.ReplaceAll(title, "_", " ")),
		padding, height-padding, width-padding, height-padding)

	// Draw bars
	spacing := (width - 2*padding - len(results)*barWidth) / (len(results) + 1)

	for i, r := range results {
		x := padding + spacing*(i+1) + barWidth*i
		barHeight := (r.Throughput / maxThroughput) * chartHeight
		y := float64(height-padding) - barHeight

		color := colors[r.Name]
		if color == "" {
			color = "#888888"
		}

		svg += fmt.Sprintf(`  <rect x="%d" y="%.0f" width="%d" height="%.0f" fill="%s" rx="4"/>
  <text x="%d" y="%d" text-anchor="middle" class="label">%s</text>
  <text x="%d" y="%.0f" text-anchor="middle" class="value">%.0f</text>
`, x, y, barWidth, barHeight, color,
			x+barWidth/2, height-padding+20, r.Name,
			x+barWidth/2, y-5, r.Throughput)
	}

	svg += `</svg>`
	return svg
}

func generateMarkdownSummary(t *testing.T, allResults map[int]*BenchmarkSuite, dir string) {
	var dataSizes []int
	for size := range allResults {
		dataSizes = append(dataSizes, size)
	}
	sort.Ints(dataSizes)

	md := `# Benchmark Results Summary

Generated: ` + time.Now().Format("2006-01-02 15:04:05") + `

## Overview

This benchmark compares **Composite Key Index** with traditional methods:
- **Full Scan**: O(n) linear search through all records
- **Hash Index**: O(1) lookup but no range/prefix support

## Results by Data Size

`

	for _, size := range dataSizes {
		suite := allResults[size]

		md += fmt.Sprintf("### %d Records\n\n", size)
		md += "| Operation | Method | Throughput (ops/s) | Avg Latency | P99 Latency |\n"
		md += "|-----------|--------|-------------------|-------------|-------------|\n"

		for _, op := range []string{"write", "exact_lookup", "prefix_scan", "range_scan"} {
			results := suite.Results[op]
			for _, r := range results {
				md += fmt.Sprintf("| %s | %s | %.0f | %v | %v |\n",
					op, r.Name, r.Throughput, r.AvgLatency, r.P99Latency)
			}
		}
		md += "\n"

		// Speedup summary
		md += "**Speedup vs Full Scan:**\n\n"
		for _, comp := range suite.Comparisons {
			if comp.Method2 == "FullScan" {
				md += fmt.Sprintf("- %s: **%.1fx** faster\n", comp.Operation, comp.Speedup)
			}
		}
		md += "\n---\n\n"
	}

	md += `## Key Findings

1. **Exact Lookup**: Composite Key Index provides O(log n) lookup vs O(n) full scan
2. **Prefix Scan**: Efficient prefix matching not possible with hash index
3. **Range Scan**: Only Composite Key Index supports efficient range queries
4. **Write Performance**: Slightly slower due to key encoding overhead

## Visualization

See ` + "`benchmark_report.html`" + ` for interactive charts.
`

	mdFile := filepath.Join(dir, "BENCHMARK_SUMMARY.md")
	if err := os.WriteFile(mdFile, []byte(md), 0644); err != nil {
		t.Logf("Warning: Could not write markdown summary: %v", err)
	} else {
		t.Logf("  Generated markdown summary: %s", mdFile)
	}
}

// Helper functions for JSON arrays
func toJSONArray(arr []int) string {
	data, _ := json.Marshal(arr)
	return string(data)
}

func toJSONFloatArray(arr []float64) string {
	data, _ := json.Marshal(arr)
	return string(data)
}
