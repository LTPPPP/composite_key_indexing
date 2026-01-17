# Composite Key Indexing - Makefile
# =================================

.PHONY: all build test bench clean demo help fmt lint deps

# Default target
all: test

# ============================================================================
# BUILD
# ============================================================================

# Build Go binaries
build:
	@echo "Building..."
	go build -v ./...

# ============================================================================
# TEST
# ============================================================================

# Run tests
test:
	@echo "Running tests..."
	go test -v ./...

# Run tests with coverage
test-cover:
	@echo "Running tests with coverage..."
	go test -v -cover -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# ============================================================================
# BENCHMARK
# ============================================================================

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem -benchtime=3s

# Run benchmarks with memory profiling
bench-mem:
	@echo "Running benchmarks with memory profiling..."
	go test -bench=. -benchmem -memprofile=mem.out
	go tool pprof -top mem.out

# Run full benchmark suite with visualization
bench-suite:
	@echo "Running benchmark suite with visualization..."
	@mkdir -p images
	go test -v -run TestBenchmarkSuiteWithVisualization -timeout 10m
	@echo ""
	@echo "Results saved to images/ folder:"
	@ls -la images/

# ============================================================================
# DEMO
# ============================================================================

# Run demo
demo:
	@echo "Running demo..."
	go run ./cmd/demo/main.go

# ============================================================================
# CODE QUALITY
# ============================================================================

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...
	@which goimports > /dev/null && goimports -w . || true

# Lint code
lint:
	@echo "Linting code..."
	@which golangci-lint > /dev/null && golangci-lint run || go vet ./...

# ============================================================================
# DEPENDENCIES
# ============================================================================

# Install dependencies
deps:
	@echo "Installing dependencies..."
	go mod tidy
	go mod download

# Update dependencies
deps-update:
	@echo "Updating dependencies..."
	go get -u ./...
	go mod tidy

# ============================================================================
# CLEANUP
# ============================================================================

# Clean build artifacts
clean:
	@echo "Cleaning..."
	go clean
	rm -f coverage.out coverage.html
	rm -f mem.out cpu.out

# ============================================================================
# HELP
# ============================================================================

help:
	@echo "Composite Key Indexing - Build Commands"
	@echo "========================================"
	@echo ""
	@echo "Build:"
	@echo "  make build        - Build binaries"
	@echo ""
	@echo "Testing:"
	@echo "  make test         - Run tests"
	@echo "  make test-cover   - Run tests with coverage"
	@echo ""
	@echo "Benchmarks:"
	@echo "  make bench        - Run benchmarks"
	@echo "  make bench-mem    - Run with memory profiling"
	@echo "  make bench-suite  - Run full suite with visualization (saves to images/)"
	@echo ""
	@echo "Demo:"
	@echo "  make demo         - Run demo application"
	@echo ""
	@echo "Code Quality:"
	@echo "  make fmt          - Format code"
	@echo "  make lint         - Lint code"
	@echo ""
	@echo "Dependencies:"
	@echo "  make deps         - Install dependencies"
	@echo "  make deps-update  - Update dependencies"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean        - Clean build artifacts"
	@echo ""
