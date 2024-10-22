package ports

import (
	"LogDb/internal/domain"
)

type Repository interface {
	AddLogRecord(record domain.LogRecord) error
}

type Writer interface {
	WriteLogRecord(record domain.LogRecord) (int, error)
}

type Scanner interface {
	ScanLogRecord() (domain.LogRecord, int, error)
}

type ReaderFactory interface {
	NewReader() Scanner
}
