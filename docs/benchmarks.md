# Benchmarks

View the latest benchmark results:

- **[Latest Benchmarks](../benchmarks.md)** - Go micro-benchmark results
- **[Performance Report](../performance.md)** - Performance test results

## Running Locally

```bash
# Run Go micro-benchmarks
make bench

# Run with JSON output
make bench-json

# Generate benchmark report (benchmarks.md)
make bench-report

# Run performance tests
make perf

# Generate performance report (performance.md)
make perf-report

# Run soak test (10 minutes)
make soak
```

## Performance Tests

| Test | Description |
|------|-------------|
| `TestPerfLatency` | Single-client latency (p50, p95, p99) |
| `TestPerfThroughput` | Multi-client throughput |
| `TestPerfConnectionChurn` | Rapid connect/disconnect cycles |
| `TestPerfTLSOverhead` | TLS vs non-TLS comparison |
| `TestPerfSustainedLoad` | 10-minute soak test |

## Configuration

```bash
# Test duration
export COMMITDB_PERF_DURATION_SEC=10

# Latency threshold
export COMMITDB_PERF_P99_MS=100

# TLS overhead threshold  
export COMMITDB_PERF_TLS_OVERHEAD_PCT=15

# Soak test duration
export COMMITDB_PERF_SOAK_DURATION_MIN=10
```
