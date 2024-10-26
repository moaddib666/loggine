package ports

import "io"

type Compression interface {
	// Compress compresses the given data.
	Compress(data []byte) ([]byte, error)
	// Decompress decompresses the given data.
	Decompress(data []byte) ([]byte, error)
	// CompressStream compresses the data from the reader and writes it to the writer.
	CompressStream(reader io.Reader, writer io.Writer) error
	// DecompressStream decompresses the data from the reader and writes it to the writer.
	DecompressStream(reader io.Reader, writer io.Writer) error
}
