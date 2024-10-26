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
func (s *SnappyCompression) CompressStream(reader io.Reader, writer io.Writer) error {
	snappyWriter := snappy.NewBufferedWriter(writer)
	defer snappyWriter.Close()

	_, err := io.Copy(snappyWriter, reader) // Stream compression
	return err
}

// DecompressStream decompresses the data from the reader and writes it to the writer
func (s *SnappyCompression) DecompressStream(reader io.Reader, writer io.Writer) error {
	snappyReader := snappy.NewReader(reader)

	_, err := io.Copy(writer, snappyReader) // Stream decompression
	return err
}
