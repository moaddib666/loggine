# Build stage
FROM golang:latest AS builder

WORKDIR /app

# Copy the Go modules and source code
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/opensearch/* .
COPY internal/adapters/opensearch internal/adapters/opensearch

# Build a statically linked binary to avoid libc issues
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/opensearch-proxy .

# Final stage with a minimal image
FROM alpine:latest

WORKDIR /app
COPY --from=builder /bin/opensearch-proxy /bin/opensearch-proxy

# Expose port for proxy
EXPOSE 8080

ENTRYPOINT ["/bin/opensearch-proxy"]
