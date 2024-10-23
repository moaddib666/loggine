package adapters

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"log"
)

var _ ports.Repository = &SingleFileStorage{}

type SingleFileStorage struct {
	writer        ports.Writer
	readerFactory ports.ReaderFactory
}

func NewSingleFileStorage(fileName string) *SingleFileStorage {
	writer, err := NewFileWriter(fileName)
	if err != nil {
		log.Fatalf("Error creating file writer: %v", err)
	}
	readerFactory := NewReaderFactory(fileName)
	if err != nil {
		log.Fatalf("Error creating reader factory: %v", err)
	}
	return &SingleFileStorage{
		writer:        writer,
		readerFactory: readerFactory,
	}
}

// AddLogRecord adds a log record to the storage
func (f *SingleFileStorage) AddLogRecord(record domain.LogRecord) error {
	_, err := f.writer.WriteLogRecord(record)
	return err
}

// GetReader returns a reader for the storage
func (f *SingleFileStorage) GetReader() ports.Scanner {
	return f.readerFactory.NewReader()
}

// Flush flushes the storage
func (f *SingleFileStorage) Flush() error {
	return f.writer.Close()
}
