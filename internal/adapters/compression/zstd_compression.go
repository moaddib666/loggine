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

func NewZstdCompression() (*ZstdCompression, error) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}

	return &ZstdCompression{
		encoder: encoder,
		decoder: decoder,
	}, nil
}

func (z *ZstdCompression) Compress(data []byte) ([]byte, error) {
	return z.encoder.EncodeAll(data, nil), nil
}

func (z *ZstdCompression) Decompress(data []byte) ([]byte, error) {
	return z.decoder.DecodeAll(data, nil)
}

// CompressStream compresses the data from the reader and writes it to the writer
func (z *ZstdCompression) CompressStream(reader io.Reader, writer io.Writer) error {
	zstdWriter, err := zstd.NewWriter(writer)
	if err != nil {
		return err
	}
	defer zstdWriter.Close()

	_, err = io.Copy(zstdWriter, reader) // Stream compression
	return err
}

// DecompressStream decompresses the data from the reader and writes it to the writer
func (z *ZstdCompression) DecompressStream(reader io.Reader, writer io.Writer) error {
	zstdReader, err := zstd.NewReader(reader)
	if err != nil {
		return err
	}
	defer zstdReader.Close()

	_, err = io.Copy(writer, zstdReader) // Stream decompression

	return err
}
