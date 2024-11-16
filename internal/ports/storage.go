package ports

import (
	"LogDb/internal/domain"
	"io"
)

//type DataFileRepository interface {
//	Open(fileName string) error
//	GetHeader() (*domain.DataFileHeader, error)
//	Close() error
//}

//type DataPageRepositoryWriter interface {
//	WriteLogRecord(record domain.LogRecord) error
//}

//type DataPageRepositoryReader interface {
//	Query(filterSet FilterSet) (*domain.QueryResult, error)
//}

//type DataPageManager interface {
//	AddLogRecord(record domain.LogRecord) error
//	GetReader() Scanner
//}

//type Writer interface {
//	WriteLogRecord(record domain.LogRecord) (int, error)
//	Close() error
//}

//type Scanner interface {
//	ScanLogRecord(filters FilterSet) (domain.LogRecord, int, error)
//	ScannedRecordsCount() int
//	Close() error
//}

//type ReaderFactory interface {
//	NewReader() Scanner
//}

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

	//SelectDataPage(pageNumber uint32, df *domain.DataFile) (*domain.DataPage, error)
	//CreateDataPage(df *domain.DataFile, pageNumber uint32) (*domain.DataPage, error)

	StoreLogRecord(record *domain.LogRecord) error
	Query(query PreparedQuery) (*domain.QueryResult, error)

	GetFileExt() string

	Close() error
}

// DataFileReader defines the operations for managing data files and pages.
type DataFileReader interface {

	// GetHeader retrieves and returns the data file header from the data source, caching it in memory
	GetHeader() (*domain.DataFileHeader, error)

	// SelectDataPage retrieves a specific data page by its page number
	SelectDataPage(pageNumber uint32) error

	// CreateDataPage creates a new data page with the given page number
	CreateDataPage(pageNumber uint32) error

	// FirstDataPage returns the first data page in the data file
	FirstDataPage() error

	// GetCurrentDataPageHeader returns the currently loaded data page
	GetCurrentDataPageHeader() (*domain.DataPageHeader, error)

	// NextDataPage moves to and returns the next data page in the data file
	NextDataPage() (*domain.DataPageHeader, error)

	// GetDataPageReader returns a reader for the current data in the data page it's limited by page size
	GetDataPageReader() io.ReadSeeker
	// Close closes the data file manager
	Close() error
}

// DataFileManagerFactory defines the operations for creating data file managers
type DataFileManagerFactory interface {
	NewDataFileManager(fileName string) (DataFileReader, error)
	FromDataFile(df *domain.DataFile) DataFileReader
}

// DataPageReaderInterface defines the interface for reading records from a data page.
type DataPageReader interface {
	Scan() bool
	Metadata() *domain.RecordMeta
	Labels() ([]domain.Label, error)
	Message() ([]byte, error)
}

// DataPagerReaderFactory defines the operations for creating data page readers
type DataPageReaderFactory interface {
	NewDataPageReader(header *domain.DataPageHeader, reader io.ReadSeeker) DataPageReader
}

// DataFileWriter defines the operations for writing data to a data file
type DataFileWriter interface {
	// Open opens the data file for writing
	Open(basedir, fileName string) error
	//// Create creates a new data file with the given ID and date
	Create(basedir string, id uint32, y, m, day uint64) error

	// Close flushes any remaining data and closes the file
	Close() error

	// CreateDataPage creates a new data page in the data file
	CreateDataPage(pageNumber uint32) error

	// WriteLogRecord writes a log record to the current data page
	WriteLogRecord(record *domain.LogRecord) error
}

// DataFileWriterFactory defines the operations for creating data page writers
// TODO: think about separate data page writer and data file writer
type DataFileWriterFactory interface {
	New() DataFileWriter
	Create(id uint32, y, m, day uint64) (DataFileWriter, error)
	Open(fileName string) (DataFileWriter, error)
}
