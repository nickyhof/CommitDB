#!/bin/bash
# scripts/run_benchmarks.sh - Run benchmarks and output JSON results
set -e

OUTPUT_FILE="${1:-benchmark_results.json}"
MODE="${2:-run}"  # 'run' = run tests, 'cached' = use /tmp files, 'true' = run with perf
PERF_DURATION="${COMMITDB_PERF_DURATION_SEC:-5}"

# Run benchmarks if not using cached output
if [ "$MODE" != "cached" ]; then
    echo "Running Go benchmarks..."
    go test -bench=. -benchmem -run=^$ ./tests/ 2>/dev/null | tee /tmp/bench_raw.txt
fi

# Parse benchmark output into JSON
echo "{" > "$OUTPUT_FILE"
echo '  "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'",' >> "$OUTPUT_FILE"
echo '  "commit": "'${GITHUB_SHA:-$(git rev-parse HEAD)}'",' >> "$OUTPUT_FILE"
echo '  "ref": "'${GITHUB_REF_NAME:-$(git rev-parse --abbrev-ref HEAD)}'",' >> "$OUTPUT_FILE"
echo '  "benchmarks": [' >> "$OUTPUT_FILE"

first=true
while IFS= read -r line; do
    if [[ $line =~ ^Benchmark([A-Za-z0-9_]+)-([0-9]+)[[:space:]]+([0-9]+)[[:space:]]+([0-9.]+)[[:space:]]ns/op ]]; then
        name="${BASH_REMATCH[1]}"
        procs="${BASH_REMATCH[2]}"
        iterations="${BASH_REMATCH[3]}"
        ns_per_op="${BASH_REMATCH[4]}"
        
        # Extract memory stats if present
        allocs_per_op="0"
        bytes_per_op="0"
        if [[ $line =~ ([0-9]+)[[:space:]]B/op ]]; then
            bytes_per_op="${BASH_REMATCH[1]}"
        fi
        if [[ $line =~ ([0-9]+)[[:space:]]allocs/op ]]; then
            allocs_per_op="${BASH_REMATCH[1]}"
        fi
        
        if [ "$first" = true ]; then
            first=false
        else
            echo "," >> "$OUTPUT_FILE"
        fi
        
        echo -n "    {\"name\": \"$name\", \"ns_per_op\": $ns_per_op, \"bytes_per_op\": $bytes_per_op, \"allocs_per_op\": $allocs_per_op}" >> "$OUTPUT_FILE"
    fi
done < /tmp/bench_raw.txt

echo "" >> "$OUTPUT_FILE"
echo "  ]," >> "$OUTPUT_FILE"

# Run performance tests if requested (and not cached)
if [ "$MODE" = "true" ] && [ "$MODE" != "cached" ]; then
    echo ""
    echo "Running performance tests..."
    
    COMMITDB_PERF_DURATION_SEC=$PERF_DURATION COMMITDB_PERF_P99_MS=1000 \
        go test -tags=perf -v -run='^TestPerf' ./tests/ -short -timeout=10m 2>&1 | tee /tmp/perf_raw.txt
fi

# Parse performance test results if file exists
if [ -f /tmp/perf_raw.txt ] && [ -s /tmp/perf_raw.txt ]; then
    echo '  "performance_tests": [' >> "$OUTPUT_FILE"
    
    first=true
    current_test=""
    throughput=""
    p99=""
    errors=""
    
    while IFS= read -r line; do
        # Detect test start
        if [[ $line =~ ===\ RUN\ +TestPerf([A-Za-z]+) ]]; then
            current_test="${BASH_REMATCH[1]}"
            throughput=""
            p99=""
            errors=""
        fi
        
        # Extract metrics from output
        if [[ $line =~ Throughput:[[:space:]]+([0-9.]+) ]]; then
            throughput="${BASH_REMATCH[1]}"
        fi
        if [[ $line =~ p99:[[:space:]]+([0-9.]+[a-zµ]+) ]]; then
            p99="${BASH_REMATCH[1]}"
        fi
        if [[ $line =~ Errors:[[:space:]]+([0-9]+) ]]; then
            errors="${BASH_REMATCH[1]}"
        fi
        
        # Detect test end (PASS or FAIL)
        if [[ $line =~ ---\ (PASS|FAIL):[[:space:]]+TestPerf([A-Za-z]+) ]]; then
            status="${BASH_REMATCH[1]}"
            test_name="${BASH_REMATCH[2]}"
            
            if [ -n "$throughput" ]; then
                if [ "$first" = true ]; then
                    first=false
                else
                    echo "," >> "$OUTPUT_FILE"
                fi
                
                echo -n "    {\"name\": \"$test_name\", \"status\": \"$status\", \"throughput\": \"$throughput\", \"p99\": \"$p99\", \"errors\": \"$errors\"}" >> "$OUTPUT_FILE"
            fi
        fi
    done < /tmp/perf_raw.txt
    
    echo "" >> "$OUTPUT_FILE"
    echo "  ]" >> "$OUTPUT_FILE"
else
    echo '  "performance_tests": []' >> "$OUTPUT_FILE"
fi

echo "}" >> "$OUTPUT_FILE"

echo ""
echo "Benchmark results saved to $OUTPUT_FILE"
cat "$OUTPUT_FILE"
