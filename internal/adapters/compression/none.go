package compression

import (
	"io"
)

type NoneCompression struct{}

func (n *NoneCompression) CompressStream(reader io.Reader, writer io.Writer) (int64, error) {
	return io.Copy(writer, reader)
}

func (n *NoneCompression) DecompressStream(reader io.Reader, writer io.Writer) (int64, error) {
	return io.Copy(writer, reader)
}

func (n *NoneCompression) Compress(data []byte) ([]byte, error) {
	return data, nil
}

func (n *NoneCompression) Decompress(data []byte) ([]byte, error) {
	return data, nil
}
