# Go parameters
GO=go
OUTPUT=./bin
GO_FLAGS=-ldflags="-s -w"
.PHONY: all build run clean bench

.build-inspector:
	$(GO) build $(GO_FLAGS) -o $(OUTPUT)/inspector ./cmd/inspector/...

.build-data-node:
	$(GO) build $(GO_FLAGS) -o $(OUTPUT)/data-node ./cmd/data-node/...

.build-client:
	$(GO) build $(GO_FLAGS) -o $(OUTPUT)/client ./cmd/cli/...

.build-compressor:
	$(GO) build $(GO_FLAGS) -o $(OUTPUT)/compressor ./cmd/compression_challange/...

.build-app:
	$(GO) build $(GO_FLAGS) -o $(OUTPUT)/app ./cmd/application/...

.docs:
	swag init -g cmd/application/main.go -o internal/adapters/api/web_api/docs

build: .docs .build-inspector .build-app

app: .build-app
inspector: .build-inspector

