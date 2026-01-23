.PHONY: build test lib lib-all clean

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test

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

# Build CLI and Server
build:
	$(GOBUILD) -o $(DIST)/commitdb-cli ./cmd/cli
	$(GOBUILD) -o $(DIST)/commitdb-server ./cmd/server

# Run tests
test:
	$(GOTEST) -v ./...

# Build shared library for current platform
lib:
	@mkdir -p $(LIB)
	CGO_ENABLED=1 $(GOBUILD) -buildmode=c-shared -o $(LIB)/$(LIB_NAME) ./bindings

# Build shared libraries for multiple platforms
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
