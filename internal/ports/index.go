package ports

import (
	"LogDb/internal/domain"
	"io"
)

type Index interface {
	BindStorage(storage DataStorage) error
	AddDataFile(df *domain.DataFileHeader) error
	DeleteDataFile(df *domain.DataFileHeader) error

	GetDataFilesForRead() ([]io.ReadSeekCloser, error)
	GetDataFileForWrite(record *domain.LogRecord) (*domain.DataFile, bool, error)
}
