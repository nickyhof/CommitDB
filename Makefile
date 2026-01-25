.PHONY: all build test test-race test-cover bench bench-json bench-report perf perf-report soak lib lib-all clean run-server run-cli fmt lint vet deps help

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GORUN=$(GOCMD) run
GOFMT=gofmt
GOVET=$(GOCMD) vet

# Output directories
DIST=dist
LIB=lib

# Detect OS
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	LIB_EXT=so
	LIB_NAME=libcommitdb.so
endif
ifeq ($(UNAME_S),Darwin)
	LIB_EXT=dylib
	LIB_NAME=libcommitdb.dylib
endif

# Default target
all: build

# Show help
help:
	@echo "CommitDB Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Build:"
	@echo "  build          Build CLI and server binaries"
	@echo "  lib            Build shared library for current platform"
	@echo "  clean          Remove build artifacts"
	@echo ""
	@echo "Test:"
	@echo "  test           Run all tests"
	@echo "  test-race      Run tests with race detector"
	@echo "  test-cover     Run tests with coverage report"
	@echo "  bench          Run benchmarks"
	@echo "  bench-json     Run benchmarks and output JSON"
	@echo "  bench-report   Generate BENCHMARKS.md report"
	@echo "  perf           Run performance tests"
	@echo "  perf-report    Generate PERFORMANCE.md report"
	@echo "  soak           Run soak test (long-running)"
	@echo ""
	@echo "Development:"
	@echo "  run-server     Run the server (default port 3306)"
	@echo "  run-cli        Run the CLI"
	@echo "  fmt            Format code"
	@echo "  vet            Run go vet"
	@echo "  lint           Run all checks (fmt, vet)"
	@echo "  deps           Download dependencies"
	@echo ""

# Build CLI and Server
build:
	@mkdir -p $(DIST)
	$(GOBUILD) -o $(DIST)/commitdb-cli ./cmd/cli
	$(GOBUILD) -o $(DIST)/commitdb-server ./cmd/server

# Run server
run-server:
	$(GORUN) ./cmd/server

# Run CLI
run-cli:
	$(GORUN) ./cmd/cli

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

# Run benchmarks and output JSON
bench-json:
	chmod +x scripts/benchmark.sh
	./scripts/benchmark.sh benchmark_results.json

# Generate benchmark report (BENCHMARKS.md)
bench-report:
	@echo "# CommitDB Benchmarks" > BENCHMARKS.md
	@echo "" >> BENCHMARKS.md
	@echo "Generated: $$(date -u '+%Y-%m-%d %H:%M:%S UTC')" >> BENCHMARKS.md
	@echo "" >> BENCHMARKS.md
	@echo "## Go Micro-benchmarks" >> BENCHMARKS.md
	@echo '```' >> BENCHMARKS.md
	$(GOTEST) -bench=. -benchmem ./tests -run=^$$ >> BENCHMARKS.md
	@echo '```' >> BENCHMARKS.md
	@echo "Benchmark report saved to BENCHMARKS.md"

# Run performance tests only
perf:
	$(GOTEST) -v -timeout=15m -tags=perf -run=^TestPerf ./tests

# Generate performance report
perf-report:
	chmod +x scripts/generate_performance_report.sh
	./scripts/generate_performance_report.sh PERFORMANCE.md

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

# Build shared library for current platform
lib:
	@mkdir -p $(LIB)
	CGO_ENABLED=1 $(GOBUILD) -buildmode=c-shared -o $(LIB)/$(LIB_NAME) ./bindings

# Build shared libraries for all platforms
lib-all: lib-linux-amd64 lib-linux-arm64 lib-darwin-amd64 lib-darwin-arm64

lib-linux-amd64:
	@mkdir -p $(LIB)
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(GOBUILD) -buildmode=c-shared -o $(LIB)/libcommitdb-linux-amd64.so ./bindings

lib-linux-arm64:
	@mkdir -p $(LIB)
	CGO_ENABLED=1 GOOS=linux GOARCH=arm64 $(GOBUILD) -buildmode=c-shared -o $(LIB)/libcommitdb-linux-arm64.so ./bindings

lib-darwin-amd64:
	@mkdir -p $(LIB)
	CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 $(GOBUILD) -buildmode=c-shared -o $(LIB)/libcommitdb-darwin-amd64.dylib ./bindings

lib-darwin-arm64:
	@mkdir -p $(LIB)
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 $(GOBUILD) -buildmode=c-shared -o $(LIB)/libcommitdb-darwin-arm64.dylib ./bindings

# Clean build artifacts
clean:
	rm -rf $(DIST) $(LIB)
	rm -f commitdb-cli commitdb-server
	rm -f coverage.out coverage.html
	rm -f benchmark_results.json
