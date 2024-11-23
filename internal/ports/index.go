package ports

import (
	"LogDb/internal/domain"
)

type Index interface {
	BindStorage(storage DataStorage) error
	AddDataFile(df *domain.DataFileHeader) error
	DeleteDataFile(df *domain.DataFileHeader) error

	GetDataFilesForRead(q PreparedQuery) ([]*domain.DataFile, error)
}

// IndexOperation defines the interface for an index operation.
type IndexOperation interface {
	Done() error
}

// PrimaryIndex defines the interface for a primary index item.
type PrimaryIndex interface {
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
