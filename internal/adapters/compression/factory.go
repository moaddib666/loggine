package compression

import (
	"LogDb/internal/domain/compression_types"
	"LogDb/internal/ports"
)

// Factory provides a way to get the appropriate compressor
func Factory(compressorType compression_types.CompressionType) ports.Compression {
	switch compressorType {
	case compression_types.Gzip:
		return &GzipCompression{}
	case compression_types.Lz4:
		return &LZ4Compression{}
	case compression_types.Snappy:
		return &SnappyCompression{}
	case compression_types.Zstd:
		return NewZstdCompression() // Zstd returns an error, so we handle it here
	default:
		return &NoneCompression{}
	}
}
