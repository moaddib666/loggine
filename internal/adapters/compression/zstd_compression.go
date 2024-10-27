package compression

import (
	"github.com/klauspost/compress/zstd"
	"io"
)

// ZstdCompression implements the Compression interface using Zstd (modern and fast)
type ZstdCompression struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func NewZstdCompression() *ZstdCompression {
	encoder, _ := zstd.NewWriter(nil)
	decoder, _ := zstd.NewReader(nil)

	return &ZstdCompression{
		encoder: encoder,
		decoder: decoder,
	}
}

func (z *ZstdCompression) Compress(data []byte) ([]byte, error) {
	return z.encoder.EncodeAll(data, nil), nil
}

func (z *ZstdCompression) Decompress(data []byte) ([]byte, error) {
	return z.decoder.DecodeAll(data, nil)
}

// CompressStream compresses the data from the reader and writes it to the writer
func (z *ZstdCompression) CompressStream(reader io.Reader, writer io.Writer) (int64, error) {
	zstdWriter, err := zstd.NewWriter(writer)
	if err != nil {
		return 0, err
	}
	defer zstdWriter.Close()

	return io.Copy(zstdWriter, reader) // Stream compression
}

// DecompressStream decompresses the data from the reader and writes it to the writer
func (z *ZstdCompression) DecompressStream(reader io.Reader, writer io.Writer) (int64, error) {
	zstdReader, err := zstd.NewReader(reader)
	if err != nil {
		return 0, err
	}
	defer zstdReader.Close()
	return io.Copy(writer, zstdReader)
}
