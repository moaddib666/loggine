package compression

import (
	"LogDb/internal/ports"
)

// Factory provides a way to get the appropriate compressor
func Factory(compressorType Type) (ports.Compression, error) {
	switch compressorType {
	case Gzip:
		return &GzipCompression{}, nil
	case Lz4:
		return &LZ4Compression{}, nil
	case Snappy:
		return &SnappyCompression{}, nil
	case Zstd:
		return NewZstdCompression() // Zstd returns an error, so we handle it here
	default:
		return &NoneCompression{}, nil
	}
}
