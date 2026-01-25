#!/bin/bash
# scripts/compare_benchmarks.sh - Compare current benchmarks to baseline and output markdown
set -e

CURRENT_FILE="${1:-benchmark_results.json}"
BASELINE_FILE="${2:-}"
THRESHOLD="${3:-20}"  # Alert threshold percentage

if [ ! -f "$CURRENT_FILE" ]; then
    echo "Error: Current benchmark file not found: $CURRENT_FILE"
    exit 1
fi

echo "## Benchmark Results"
echo ""
echo "**Commit:** \`$(jq -r '.commit' "$CURRENT_FILE" | head -c 7)\`"
echo "**Date:** $(jq -r '.timestamp' "$CURRENT_FILE")"
echo ""

# If no baseline, just show current results
if [ -z "$BASELINE_FILE" ] || [ ! -f "$BASELINE_FILE" ]; then
    echo "| Benchmark | ns/op | B/op | allocs/op |"
    echo "|-----------|-------|------|-----------|"
    jq -r '.benchmarks[] | "| \(.name) | \(.ns_per_op) | \(.bytes_per_op) | \(.allocs_per_op) |"' "$CURRENT_FILE"
    exit 0
fi

# Compare with baseline
echo "| Benchmark | ns/op | vs baseline | B/op | allocs/op |"
echo "|-----------|-------|-------------|------|-----------|"

has_regression=false

jq -r '.benchmarks[] | "\(.name) \(.ns_per_op) \(.bytes_per_op) \(.allocs_per_op)"' "$CURRENT_FILE" | while read name ns_op bytes allocs; do
    baseline_ns=$(jq -r ".benchmarks[] | select(.name == \"$name\") | .ns_per_op" "$BASELINE_FILE" 2>/dev/null || echo "0")
    
    if [ "$baseline_ns" != "0" ] && [ "$baseline_ns" != "null" ] && [ -n "$baseline_ns" ]; then
        # Calculate percentage change
        change=$(echo "scale=1; (($ns_op - $baseline_ns) / $baseline_ns) * 100" | bc 2>/dev/null || echo "0")
        
        # Format the change
        if (( $(echo "$change > 0" | bc -l) )); then
            if (( $(echo "$change > $THRESHOLD" | bc -l) )); then
                change_str="⚠️ +${change}%"
                has_regression=true
            else
                change_str="+${change}%"
            fi
        elif (( $(echo "$change < -5" | bc -l) )); then
            change_str="🚀 ${change}%"
        else
            change_str="${change}%"
        fi
    else
        change_str="(new)"
    fi
    
    echo "| $name | $ns_op | $change_str | $bytes | $allocs |"
done

if [ "$has_regression" = true ]; then
    echo ""
    echo "> ⚠️ **Warning:** Some benchmarks regressed by more than ${THRESHOLD}%"
fi
