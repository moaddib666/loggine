package compression

import (
	"io"
)

type NoneCompression struct{}

func (n *NoneCompression) CompressStream(reader io.Reader, writer io.Writer) error {
	_, err := io.Copy(writer, reader)
	return err
}

func (n *NoneCompression) DecompressStream(reader io.Reader, writer io.Writer) error {
	_, err := io.Copy(writer, reader)
	return err
}

func (n *NoneCompression) Compress(data []byte) ([]byte, error) {
	return data, nil
}

func (n *NoneCompression) Decompress(data []byte) ([]byte, error) {
	return data, nil
}
