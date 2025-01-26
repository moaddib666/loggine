package ports

import (
	"LogDb/internal/domain/compression_types"
	"io"
)

type Compression interface {
	// Compress compresses the given data.
	Compress(data []byte) ([]byte, error)
	// Decompress decompresses the given data.
	Decompress(data []byte) ([]byte, error)
	// CompressStream compresses the data from the reader and writes it to the writer.
	CompressStream(reader io.Reader, writer io.Writer) (int64, error)
	// DecompressStream decompresses the data from the reader and writes it to the writer.
	DecompressStream(reader io.Reader, writer io.Writer) (int64, error)
}

type Compressible interface {
	// Compress compresses the data.
	Compress() error
}

type CompressionPolicy interface {
	// Apply applies the policy to the target.
	Apply(target Compressible)
}

type CompressionFactoryMethod func(compressorType compression_types.CompressionType) Compression
