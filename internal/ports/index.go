package ports

import (
	"LogDb/internal/domain"
)

type Index interface {
	BindStorage(storage DataStorage) error
	AddDataFile(df *domain.DataFileHeader) error
	//deleteDataFile(df *domain.DataFileHeader) error

	GetDataFilesForRead(q PreparedQuery) ([]IndexOperation, error)
}

// IndexOperation defines the interface for an index operation.
type IndexOperation interface {
	GetDataFileHeader() *domain.DataFileHeader         // Cheap operation get data file header from memory
	GetDataFile(path string) (*domain.DataFile, error) // Expensive operation get data file from disk
	Done() error                                       // Marks the operation as done
}

// IndexItem defines the interface for a primary index item.
type IndexItem interface {
	// GetHeader returns the header of the index.
	GetHeader() *domain.DataFileHeader

	// RequestReadAccess - read possible if no write operation is in progress
	RequestReadAccess() (IndexOperation, error)

	// RequestWriteAccess - write possible if no read or write operation is in progress
	RequestWriteAccess() (IndexOperation, error)

	// AwaitReadAccess waits while a write operation is in progress.
	AwaitReadAccess() (IndexOperation, error)

	// AwaitWriteAccess waits while a read or write operation is in progress.
	AwaitWriteAccess() (IndexOperation, error)
}
