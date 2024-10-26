package compression

import (
	"bytes"
	"github.com/pierrec/lz4/v4"
	"io"
	"io/ioutil"
)

// LZ4Compression implements the Compression interface using LZ4
type LZ4Compression struct{}

func (l *LZ4Compression) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	defer w.Close()

	_, err := w.Write(data)
	if err != nil {
		return nil, err
	}

	err = w.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (l *LZ4Compression) Decompress(data []byte) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(data))
	return ioutil.ReadAll(r)
}

// CompressStream compresses the data from the reader and writes it to the writer
func (l *LZ4Compression) CompressStream(reader io.Reader, writer io.Writer) error {
	lz4Writer := lz4.NewWriter(writer)
	defer lz4Writer.Close()

	_, err := io.Copy(lz4Writer, reader) // Stream compression
	return err
}

// DecompressStream decompresses the data from the reader and writes it to the writer
func (l *LZ4Compression) DecompressStream(reader io.Reader, writer io.Writer) error {
	lz4Reader := lz4.NewReader(reader)

	_, err := io.Copy(writer, lz4Reader) // Stream decompression
	return err
}
