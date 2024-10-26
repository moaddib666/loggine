# Go parameters
GO=go
BINARY=benchmark_app
OUTPUT=./bin
GO_FLAGS=-ldflags="-s -w"
.PHONY: all build run clean bench

.build-inspector:
	$(GO) build $(GO_FLAGS) -o $(OUTPUT)/inspector ./cmd/inspector/...

.build-data-node:
	$(GO) build $(GO_FLAGS) -o $(OUTPUT)/data-node ./cmd/data-node/...

build: .build-inspector .build-data-node

