package ports

import (
	"LogDb/internal/domain"
)

type Index interface {
	BindStorage(storage DataStorage) error
	AddDataFile(df *domain.DataFileHeader) error
	DeleteDataFile(df *domain.DataFileHeader) error

	GetDataFilesForRead(q *domain.Query) ([]*domain.DataFile, error)
	GetDataFileForWrite(record *domain.LogRecord) (*domain.DataFile, bool, error)
}
