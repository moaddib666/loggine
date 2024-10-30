package datastor

import (
	"fmt"
	"io"
)

// chunkedReader is a custom reader that loads data in chunks and supports seeking
type chunkedReader struct {
	baseReader io.ReadSeeker
	chunkSize  int
	buffer     []byte
	bufferPos  int64 // Start position of the buffer in the file
	bufferLen  int   // Length of data currently in the buffer
	offset     int64 // Current read offset within the file
	fileSize   int64 // Total size of the file (used for SeekEnd)
}

// Read reads data from the buffer and loads new chunks when needed
func (cr *chunkedReader) Read(p []byte) (int, error) {
	var totalRead int
	for totalRead < len(p) {
		if cr.bufferLen == 0 || cr.offset < cr.bufferPos || cr.offset >= cr.bufferPos+int64(cr.bufferLen) {
			err := cr.loadChunk()
			if err != nil {
				if err == io.EOF && totalRead > 0 {
					return totalRead, nil
				}
				return totalRead, err
			}
		}

		start := cr.offset - cr.bufferPos
		available := cr.bufferLen - int(start)
		toCopy := len(p) - totalRead
		if available < toCopy {
			toCopy = available
		}

		n := copy(p[totalRead:], cr.buffer[start:int(start)+int(toCopy)])
		totalRead += n
		cr.offset += int64(n)
		if cr.offset >= cr.fileSize {
			return totalRead, io.EOF
		}
		if n == 0 {
			break
		}
	}

	if totalRead == 0 {
		return 0, io.EOF
	}
	return totalRead, nil
}

// Seek implements the io.Seeker interface and adjusts the buffer accordingly
func (cr *chunkedReader) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = cr.offset + offset
	case io.SeekEnd:
		// Calculate file size only once
		if cr.fileSize == 0 {
			fileSize, err := cr.baseReader.Seek(0, io.SeekEnd)
			if err != nil {
				return 0, err
			}
			cr.fileSize = fileSize
		}
		newOffset = cr.fileSize + offset
	default:
		return 0, fmt.Errorf("invalid whence value")
	}

	// Ensure the new offset is within valid bounds
	if newOffset < 0 || (cr.fileSize > 0 && newOffset > cr.fileSize) {
		return 0, fmt.Errorf("invalid seek offset")
	}

	// Update the offset
	cr.offset = newOffset

	return cr.offset, nil
}

// loadChunk loads the chunk that includes the current offset into the buffer
func (cr *chunkedReader) loadChunk() error {
	// Calculate the start position of the new chunk
	chunkStart := cr.offset - (cr.offset % int64(cr.chunkSize))

	// Seek the baseReader to the start of the chunk
	_, err := cr.baseReader.Seek(chunkStart, io.SeekStart)
	if err != nil {
		return err
	}

	// Read the chunk into the buffer
	cr.bufferPos = chunkStart
	n, err := cr.baseReader.Read(cr.buffer[:cr.chunkSize])
	if err != nil && err != io.EOF {
		return err
	}

	cr.bufferLen = n
	return err
}

// NewChunkedReader creates a new chunkedReader that reads in specified chunk sizes
func NewChunkedReader(reader io.ReadSeeker, chunkSize int) (*chunkedReader, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}
	if chunkSize <= 0 {
		return nil, fmt.Errorf("chunk size must be greater than 0")
	}

	// Determine file size
	currentPos, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	fileSize, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	_, err = reader.Seek(currentPos, io.SeekStart)
	if err != nil {
		return nil, err
	}

	return &chunkedReader{
		baseReader: reader,
		chunkSize:  chunkSize,
		buffer:     make([]byte, chunkSize),
		fileSize:   fileSize,
	}, nil
}
