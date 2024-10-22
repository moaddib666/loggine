package adapters

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"bytes"
	"encoding/binary"
	"os"
	"strings"
	"sync"
)

// LogRecordBinaryProcessor implements the binary serialization for LogRecord
type LogRecordBinaryProcessor struct {
	fd *os.File
	mu sync.Mutex
}

func (l *LogRecordBinaryProcessor) WriteLogRecord(record domain.LogRecord) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	reader := new(bytes.Buffer)

	labels := []byte(strings.Join(record.Labels, ""))
	message := []byte(record.Message)
	labelsSize := uint64(len(labels))
	messageSize := uint64(len(message))
	totalSize := labelsSize + messageSize
	// Write size
	if err := binary.Write(reader, binary.LittleEndian, totalSize); err != nil {
		return 0, err
	}

	// Write timestamp
	if err := binary.Write(reader, binary.LittleEndian, record.Timestamp.Unix()); err != nil {
		return 0, err
	}

	// Write schema version
	if err := binary.Write(reader, binary.LittleEndian, 1); err != nil {
		return 0, err
	}

	// Write size of Labels
	if err := binary.Write(reader, binary.LittleEndian, labelsSize); err != nil {
		return 0, err
	}

	// Write Labels content
	if _, err := reader.Write(labels); err != nil {
		return 0, err
	}

	// Write size of Message
	if err := binary.Write(reader, binary.LittleEndian, messageSize); err != nil {
		return 0, err
	}

	// Write Message content
	if _, err := reader.Write(message); err != nil {
		return 0, err
	}

	return l.fd.Write(reader.Bytes())
}

func (l *LogRecordBinaryProcessor) ScanLogRecord() (domain.LogRecord, int, error) {
	record := domain.LogRecord{}
	var totalSize uint64
	var schemaVersion uint64

	// Read size
	if err := binary.Read(l.fd, binary.LittleEndian, &totalSize); err != nil {
		return record, 0, err
	}

	// Read timestamp
	if err := binary.Read(l.fd, binary.LittleEndian, &record.Timestamp); err != nil {
		return record, 0, err
	}

	// Read schema version
	if err := binary.Read(l.fd, binary.LittleEndian, schemaVersion); err != nil {
		return record, 0, err
	}

	// Read size of Labels
	var labelsSize uint64
	if err := binary.Read(l.fd, binary.LittleEndian, &labelsSize); err != nil {
		return record, 0, err
	}

	// Read Labels content
	labels := make([]byte, labelsSize)
	if _, err := l.fd.Read(labels); err != nil {
		return record, 0, err
	}
	//record.Labels = string(labels)

	// Read size of Message
	var messageSize uint64
	if err := binary.Read(l.fd, binary.LittleEndian, &messageSize); err != nil {
		return record, 0, err
	}

	// Read Message content
	message := make([]byte, messageSize)
	if _, err := l.fd.Read(message); err != nil {
		return record, 0, err
	}
	record.Message = string(message)

	return record, 0, nil
}

// NewLogRecordBinaryProcessor creates a new LogRecordBinaryProcessor
func NewLogRecordBinaryProcessor(fd *os.File) *LogRecordBinaryProcessor {
	return &LogRecordBinaryProcessor{fd: fd}
}

// NewFileWriter creates a new LogRecordBinaryProcessor
func NewFileWriter(fileName string) (*LogRecordBinaryProcessor, error) {
	fd, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return NewLogRecordBinaryProcessor(fd), nil
}

// NewFileReader creates a new LogRecordBinaryProcessor
func NewFileReader(fileName string) (*LogRecordBinaryProcessor, error) {
	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return NewLogRecordBinaryProcessor(fd), nil
}

var _ ports.ReaderFactory = &ReaderFactory{}

type ReaderFactory struct {
	fileName string
}

func (r *ReaderFactory) NewReader() ports.Scanner {
	reader, _ := NewFileReader(r.fileName)
	return reader
}

func NewReaderFactory(fileName string) *ReaderFactory {
	return &ReaderFactory{fileName: fileName}
}
