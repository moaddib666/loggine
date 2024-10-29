package ports

import (
	"LogDb/internal/domain"
)

type DataFileRepository interface {
	Open(fileName string) error
	GetHeader() (*domain.DataFileHeader, error)
	Close() error
}

type DataPageRepositoryWriter interface {
	WriteLogRecord(record domain.LogRecord) error
}

type DataPageRepositoryReader interface {
	Query(filterSet FilterSet) (*domain.QueryResult, error)
}

type DataPageManager interface {
	AddLogRecord(record domain.LogRecord) error
	GetReader() Scanner
}

type Writer interface {
	WriteLogRecord(record domain.LogRecord) (int, error)
	Close() error
}

type Scanner interface {
	ScanLogRecord(filters FilterSet) (domain.LogRecord, int, error)
	ScannedRecordsCount() int
	Close() error
}

type ReaderFactory interface {
	NewReader() Scanner
}

type LogTransformer interface {
	ToString(record *domain.LogRecord) string
	FromString(str string) *domain.LogRecord
	FromBytes(b []byte) *domain.LogRecord
}

type DataStorage interface {
	GetDataFilesHeaders() ([]*domain.DataFileHeader, error)
	// TODO: Add flags to the function like readonly, writeonly, readwrite
	GetDataFile(name string) (*domain.DataFile, error)
	CreateDataFile(name string, id uint32, y, m, d uint64) (*domain.DataFile, error)

	GetDataPage(pageNumber uint32, df *domain.DataFile) (*domain.DataPage, error)
	CreateDataPage(df *domain.DataFile, pageNumber uint32) (*domain.DataPage, error)

	StoreLogRecord(record *domain.LogRecord) error
	Query(query PreparedQuery) (*domain.QueryResult, error)

	GetFileExt() string

	Close() error
}
