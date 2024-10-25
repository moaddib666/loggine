package ports

import (
	"LogDb/internal/domain"
	"io"
)

type Serializer interface {
	WriteDataPageHeader(header *domain.DataPageHeader, writer io.Writer) (int, error)
	ReadDataPageHeader(header *domain.DataPageHeader, reader io.Reader) (int, error)

	WriteFileHeader(header *domain.DataFileHeader, writer io.Writer) (int, error)
	ReadFileHeader(header *domain.DataFileHeader, reader io.Reader) (int, error)

	ReadLogRecordMeta(header *domain.RecordMeta, reader io.Reader) (int, error)
	ReadLogLabel(label *domain.Label, reader io.Reader) (int, error)
	ReadLogRecordMessage(message []byte, reader io.Reader) (int, error)

	WriteLogRecord(record *domain.LogRecord, writer io.Writer) (int, error)
	WriteLogRecordMeta(header *domain.RecordMeta, writer io.Writer) (int, error)
	WriteLogLabel(label *domain.Label, writer io.Writer) (int, error)
	WriteLogRecordMessage(message []byte, writer io.Writer) (int, error)
}
