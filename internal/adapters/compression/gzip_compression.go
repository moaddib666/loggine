package compression

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
)

// GzipCompression implements the Compression interface using gzip
type GzipCompression struct{}

func (g *GzipCompression) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	defer gz.Close()

	_, err := gz.Write(data)
	if err != nil {
		return nil, err
	}

	err = gz.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (g *GzipCompression) Decompress(data []byte) ([]byte, error) {
	buf := bytes.NewReader(data)
	gz, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	return ioutil.ReadAll(gz)
}

// CompressStream compresses the data from the reader and writes it to the writer
func (g *GzipCompression) CompressStream(reader io.Reader, writer io.Writer) (int64, error) {
	gz := gzip.NewWriter(writer)
	defer gz.Close()

	written, err := io.Copy(gz, reader) // Stream compression
	return written, err
}

// DecompressStream decompresses the data from the reader and writes it to the writer
func (g *GzipCompression) DecompressStream(reader io.Reader, writer io.Writer) (int64, error) {
	gz, err := gzip.NewReader(reader)
	if err != nil {
		return 0, err
	}
	defer gz.Close()

	written, err := io.Copy(writer, gz) // Stream decompression
	return written, err
}
