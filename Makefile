.PHONY: all build test test-race test-cover bench bench-json bench-report perf perf-report soak clean run-cli fmt lint vet deps help

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GORUN=$(GOCMD) run
GOFMT=gofmt
GOVET=$(GOCMD) vet

# Output directories
DIST=dist

# Default target
all: build

# Show help
help:
	@echo "CommitDB Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Build:"
	@echo "  build          Build CLI binary"
	@echo "  clean          Remove build artifacts"
	@echo ""
	@echo "Test:"
	@echo "  test           Run all tests"
	@echo "  test-race      Run tests with race detector"
	@echo "  test-cover     Run tests with coverage report"
	@echo "  bench          Run benchmarks"
	@echo "  bench-json     Run benchmarks and output JSON"
	@echo "  bench-report   Generate benchmarks.md report"
	@echo "  perf           Run performance tests"
	@echo "  perf-report    Generate performance.md report"
	@echo "  soak           Run soak test (long-running)"
	@echo ""
	@echo "Development:"
	@echo "  run-cli        Run the CLI"
	@echo "  fmt            Format code"
	@echo "  vet            Run go vet"
	@echo "  lint           Run all checks (fmt, vet)"
	@echo "  deps           Download dependencies"
	@echo ""

# Build CLI
build:
	@mkdir -p $(DIST)
	$(GOBUILD) -o $(DIST)/commitdb ./cmd/commitdb

# Run CLI
run-cli:
	$(GORUN) ./cmd/commitdb

# Run tests
test:
	$(GOTEST) -v ./...

# Run tests with race detector
test-race:
	$(GOTEST) -v -race ./...

# Run tests with coverage
test-cover:
	$(GOTEST) -v -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run benchmarks only
bench:
	$(GOTEST) -bench=. -benchmem ./tests -run=^$$

# Run comparative benchmarks (CommitDB vs DuckDB)
bench-compare:
	$(GOTEST) -v -tags=comparative -bench=. -benchmem ./tests -run=^$$

# Run benchmarks and output JSON
bench-json:
	chmod +x scripts/run_benchmarks.sh
	./scripts/run_benchmarks.sh benchmark_results.json

# Generate benchmark report (benchmarks.md)
bench-report:
	@echo "# CommitDB Benchmarks" > benchmarks.md
	@echo "" >> benchmarks.md
	@echo "Generated: $$(date -u '+%Y-%m-%d %H:%M:%S UTC')" >> benchmarks.md
	@echo "" >> benchmarks.md
	@echo "## Go Micro-benchmarks" >> benchmarks.md
	@echo '```' >> benchmarks.md
	$(GOTEST) -bench=. -benchmem ./tests -run=^$$ >> benchmarks.md
	@echo '```' >> benchmarks.md
	@echo "Benchmark report saved to benchmarks.md"

# Run performance tests only
perf:
	$(GOTEST) -v -timeout=15m -tags=perf -run=^TestPerf ./tests

# Generate performance report
perf-report:
	chmod +x scripts/generate_performance_report.sh
	./scripts/generate_performance_report.sh performance.md

# Run soak test (long-running)
soak:
	$(GOTEST) -v -timeout=30m -tags=perf -run=^TestPerfSustainedLoad ./tests

# Format code
fmt:
	$(GOFMT) -s -w .

# Run go vet
vet:
	$(GOVET) ./...

# Run all checks
lint: fmt vet

# Download dependencies
deps:
	$(GOCMD) mod download
	$(GOCMD) mod tidy

# Clean build artifacts
clean:
	rm -rf $(DIST)
	rm -f commitdb
	rm -f coverage.out coverage.html
	rm -f benchmark_results.json
