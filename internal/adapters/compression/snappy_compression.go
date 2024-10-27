package compression

import (
	"github.com/golang/snappy"
	"io"
)

// SnappyCompression implements the Compression interface using Snappy
type SnappyCompression struct{}

func (s *SnappyCompression) Compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func (s *SnappyCompression) Decompress(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

// CompressStream compresses the data from the reader and writes it to the writer
func (s *SnappyCompression) CompressStream(reader io.Reader, writer io.Writer) (int64, error) {
	snappyWriter := snappy.NewBufferedWriter(writer)
	defer snappyWriter.Close()
	return io.Copy(snappyWriter, reader) // Stream compression
}

// DecompressStream decompresses the data from the reader and writes it to the writer
func (s *SnappyCompression) DecompressStream(reader io.Reader, writer io.Writer) (int64, error) {
	snappyReader := snappy.NewReader(reader)
	return io.Copy(writer, snappyReader) // Stream decompression
}
