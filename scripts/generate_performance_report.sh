#!/bin/bash
# scripts/generate_performance_report.sh - Generate PERFORMANCE.md from test results
set -e

OUTPUT_FILE="${1:-performance.md}"
VERSION="${2:-$(git describe --tags --always)}"
MODE="${3:-run}"  # 'run' = run tests, 'cached' = use /tmp/perf_raw.txt

echo "Generating performance report..."

# Header
cat > "$OUTPUT_FILE" << EOF
# CommitDB Performance Report - ${VERSION}

Generated: $(date -u '+%Y-%m-%d %H:%M:%S UTC')
Commit: ${GITHUB_SHA:-$(git rev-parse HEAD)}

## Test Environment

- **OS**: $(uname -s) $(uname -r)
- **Architecture**: $(uname -m)
- **Go Version**: $(go version | awk '{print $3}')
- **CPU Cores**: $(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "unknown")

## Performance Test Results

EOF

# Run performance tests if not using cached output
if [ "$MODE" != "cached" ]; then
    echo "Running performance tests..."
    COMMITDB_PERF_DURATION_SEC=${COMMITDB_PERF_DURATION_SEC:-5} \
    COMMITDB_PERF_P99_MS=${COMMITDB_PERF_P99_MS:-1000} \
      go test -tags=perf -v -run='^TestPerf' ./tests/ -short -timeout=10m 2>&1 | tee /tmp/perf_raw.txt
fi

# Parse and format results
echo '```' >> "$OUTPUT_FILE"
grep -E '(=== RUN|--- PASS|--- FAIL|Performance|Clients|Duration|Requests|Throughput|Latency|Errors|Memory|Goroutines|├|└|│)' /tmp/perf_raw.txt >> "$OUTPUT_FILE" || true
echo '```' >> "$OUTPUT_FILE"

# Add summary table
echo "" >> "$OUTPUT_FILE"
echo "## Summary" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"
echo "| Test | Status | Throughput | P99 Latency | Errors |" >> "$OUTPUT_FILE"
echo "|------|--------|------------|-------------|--------|" >> "$OUTPUT_FILE"

# Parse test results for summary
current_test=""
throughput=""
p99=""
errors=""

while IFS= read -r line; do
    if [[ $line =~ ===\ RUN\ +TestPerf([A-Za-z]+) ]]; then
        current_test="${BASH_REMATCH[1]}"
        throughput="-"
        p99="-"
        errors="-"
    fi
    
    if [[ $line =~ Throughput:[[:space:]]+([0-9.]+)[[:space:]]*(req/s|conn/s) ]]; then
        throughput="${BASH_REMATCH[1]} ${BASH_REMATCH[2]}"
    fi
    
    if [[ $line =~ p99:[[:space:]]+([0-9.]+[a-zµ]+) ]]; then
        p99="${BASH_REMATCH[1]}"
    fi
    
    if [[ $line =~ Errors:[[:space:]]+([0-9]+) ]]; then
        errors="${BASH_REMATCH[1]}"
    fi
    
    if [[ $line =~ ---\ (PASS|FAIL):[[:space:]]+TestPerf([A-Za-z]+) ]]; then
        status="${BASH_REMATCH[1]}"
        if [ "$status" = "PASS" ]; then
            status="✅ PASS"
        else
            status="❌ FAIL"
        fi
        
        if [ -n "$current_test" ]; then
            echo "| $current_test | $status | $throughput | $p99 | $errors |" >> "$OUTPUT_FILE"
        fi
    fi
done < /tmp/perf_raw.txt

# Add Go benchmarks section if cached benchmark data exists
if [ -f /tmp/bench_raw.txt ]; then
    echo "" >> "$OUTPUT_FILE"
    echo "## Go Micro-Benchmarks" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
    echo '```' >> "$OUTPUT_FILE"
    cat /tmp/bench_raw.txt >> "$OUTPUT_FILE"
    echo '```' >> "$OUTPUT_FILE"
fi

echo ""
echo "Performance report saved to $OUTPUT_FILE"
