package adapters

import (
	"LogDb/internal/ports"
)

// ReaderFactory is used to create readers for LogRecord
type ReaderFactory struct {
	fileName string
}

// NewReader creates a new reader (LogRecordBinaryProcessor) for reading log records
func (r *ReaderFactory) NewReader() ports.Scanner {
	reader, err := NewFileReader(r.fileName)
	if err != nil {
		// You can handle the error or panic here based on your error handling approach
		panic(err)
	}
	return reader
}

// NewReaderFactory creates a new ReaderFactory
func NewReaderFactory(fileName string) *ReaderFactory {
	return &ReaderFactory{fileName: fileName}
}
