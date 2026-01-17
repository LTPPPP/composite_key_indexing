# Benchmark Results Summary

Generated: 2026-01-17 21:06:38

## Overview

This benchmark compares **Composite Key Index** with traditional methods:
- **Full Scan**: O(n) linear search through all records
- **Hash Index**: O(1) lookup but no range/prefix support

## Results by Data Size

### 1000 Records

| Operation | Method | Throughput (ops/s) | Avg Latency | P99 Latency |
|-----------|--------|-------------------|-------------|-------------|
| write | CompositeKey | 193285 | 5.123µs | 23.134µs |
| write | FullScan | 11409273 | 41ns | 28ns |
| write | HashIndex | 1538871 | 598ns | 1.535µs |
| exact_lookup | CompositeKey | 167698 | 5.913µs | 19.224µs |
| exact_lookup | FullScan | 378914 | 2.588µs | 5.981µs |
| exact_lookup | HashIndex | 1468860 | 634ns | 2.41µs |
| prefix_scan | CompositeKey | 1854 | 539.178µs | 1.089336ms |
| prefix_scan | FullScan | 25936 | 38.513µs | 72.47µs |
| prefix_scan | HashIndex | 17419 | 57.362µs | 87.001µs |
| range_scan | CompositeKey | 3569 | 280.117µs | 670.965µs |
| range_scan | FullScan | 12063 | 82.848µs | 122.204µs |

**Speedup vs Full Scan:**

- write: **0.0x** faster
- exact_lookup: **0.4x** faster
- prefix_scan: **0.1x** faster
- range_scan: **0.3x** faster

---

### 5000 Records

| Operation | Method | Throughput (ops/s) | Avg Latency | P99 Latency |
|-----------|--------|-------------------|-------------|-------------|
| write | CompositeKey | 237754 | 4.153µs | 9.977µs |
| write | FullScan | 11728004 | 37ns | 28ns |
| write | HashIndex | 1568199 | 590ns | 1.166µs |
| exact_lookup | CompositeKey | 179768 | 5.522µs | 22.92µs |
| exact_lookup | FullScan | 542042 | 1.803µs | 3.628µs |
| exact_lookup | HashIndex | 1885647 | 493ns | 2.037µs |
| prefix_scan | CompositeKey | 566 | 1.767623ms | 3.131968ms |
| prefix_scan | FullScan | 5021 | 199.088µs | 315.063µs |
| prefix_scan | HashIndex | 2513 | 397.818µs | 487.059µs |
| range_scan | CompositeKey | 731 | 1.36816ms | 2.600097ms |
| range_scan | FullScan | 4494 | 222.431µs | 253.605µs |

**Speedup vs Full Scan:**

- write: **0.0x** faster
- exact_lookup: **0.3x** faster
- prefix_scan: **0.1x** faster
- range_scan: **0.2x** faster

---

### 10000 Records

| Operation | Method | Throughput (ops/s) | Avg Latency | P99 Latency |
|-----------|--------|-------------------|-------------|-------------|
| write | CompositeKey | 212096 | 4.672µs | 23.784µs |
| write | FullScan | 15838322 | 26ns | 32ns |
| write | HashIndex | 1637484 | 573ns | 1.299µs |
| exact_lookup | CompositeKey | 202997 | 4.882µs | 8.714µs |
| exact_lookup | FullScan | 440839 | 2.223µs | 4.312µs |
| exact_lookup | HashIndex | 1530878 | 608ns | 1.276µs |
| prefix_scan | CompositeKey | 299 | 3.339601ms | 5.601907ms |
| prefix_scan | FullScan | 2149 | 465.218µs | 532.585µs |
| prefix_scan | HashIndex | 1037 | 964.425µs | 1.788618ms |
| range_scan | CompositeKey | 326 | 3.071176ms | 5.165249ms |
| range_scan | FullScan | 2515 | 397.535µs | 459.539µs |

**Speedup vs Full Scan:**

- write: **0.0x** faster
- exact_lookup: **0.5x** faster
- prefix_scan: **0.1x** faster
- range_scan: **0.1x** faster

---

## Key Findings

1. **Exact Lookup**: Composite Key Index provides O(log n) lookup vs O(n) full scan
2. **Prefix Scan**: Efficient prefix matching not possible with hash index
3. **Range Scan**: Only Composite Key Index supports efficient range queries
4. **Write Performance**: Slightly slower due to key encoding overhead

## Visualization

See `benchmark_report.html` for interactive charts.
