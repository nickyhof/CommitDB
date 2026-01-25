#!/bin/bash
# scripts/benchmark.sh - Run benchmarks and output JSON results
set -e

OUTPUT_FILE="${1:-benchmark_results.json}"
PERF_DURATION="${COMMITDB_PERF_DURATION_SEC:-5}"

echo "Running Go benchmarks..."

# Run Go benchmarks and parse output
go test -bench=. -benchmem -run=^$ ./tests/ 2>/dev/null | tee /tmp/bench_raw.txt

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
echo "  ]" >> "$OUTPUT_FILE"
echo "}" >> "$OUTPUT_FILE"

echo "Benchmark results saved to $OUTPUT_FILE"
cat "$OUTPUT_FILE"
