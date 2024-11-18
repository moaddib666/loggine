package io

import (
	"bytes"
	"fmt"
	"io"
)

type ReadWriteSeeker struct {
	buffer *bytes.Buffer
	offset int64
}

// NewReadWriteSeeker creates a new ReadWriteSeeker instance.
func NewReadWriteSeeker(buf *bytes.Buffer) *ReadWriteSeeker {
	return &ReadWriteSeeker{
		buffer: buf,
		offset: 0,
	}
}

// Read reads data from the buffer.
func (rws *ReadWriteSeeker) Read(p []byte) (int, error) {
	n, err := rws.buffer.Read(p)
	rws.offset += int64(n)
	return n, err
}

// Write writes data to the buffer.
func (rws *ReadWriteSeeker) Write(p []byte) (int, error) {
	n, err := rws.buffer.Write(p)
	rws.offset += int64(n)
	return n, err
}

// Seek sets the offset for the next Read or Write operation.
func (rws *ReadWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = rws.offset + offset
	case io.SeekEnd:
		newOffset = int64(rws.buffer.Len()) + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("negative position: %d", newOffset)
	}

	rws.offset = newOffset
	rws.buffer.Truncate(int(newOffset))
	return newOffset, nil
}
