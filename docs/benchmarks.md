# Benchmarks

## Running Locally

```bash
# Run Go micro-benchmarks
make bench

# Run with JSON output
make bench-json

# Generate benchmark report (benchmarks.md)
make bench-report
```

## Benchmark Tests

| Test | Description |
|------|-------------|
| `BenchmarkInsert` | Single-row insert latency |
| `BenchmarkSelect` | Full table scan latency |
| `BenchmarkSelectWhere` | Filtered query latency |
| `BenchmarkSelectPK` | Primary key lookup (O(1)) |
| `BenchmarkCreateTable` | DDL overhead |
| `BenchmarkCopyInto` | Bulk CSV import throughput |

## View Results

- **[Latest Benchmarks](../benchmarks.md)** — Go micro-benchmark results
