# Go parameters
GO=go
BINARY=benchmark_app
CMD_PATH=cmd/benchmark
OUTPUT=./bin

.PHONY: all build run clean bench

all: build run clean

# Build the benchmark binary
build:
	@echo "Building the benchmark binary..."
	@mkdir -p $(OUTPUT)
	$(GO) build -o $(OUTPUT)/$(BINARY) $(CMD_PATH)

# Run the benchmark binary
run:
	@echo "Running the benchmark..."
	$(OUTPUT)/$(BINARY)

# Run benchmarks directly with go test
bench:
	@echo "Running benchmarks..."
	$(GO) test -bench=. $(CMD_PATH) -run=^#

# Clean the binary after running
clean:
	@echo "Cleaning up..."
	rm -rf $(OUTPUT)
