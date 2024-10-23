package adapters

import (
	"LogDb/internal/domain"
	"bytes"
	"encoding/binary"
	"os"
	"sync"
	"time"
)

// LogRecordBinaryProcessor implements the binary serialization for LogRecord
type LogRecordBinaryProcessor struct {
	fd *os.File
	mu sync.Mutex
}

func (l *LogRecordBinaryProcessor) WriteLogRecord(record domain.LogRecord) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Create a buffer to hold the entire record
	buf := new(bytes.Buffer)

	// Write timestamp (convert int64 to uint64)
	if err := binary.Write(buf, binary.LittleEndian, uint64(record.Timestamp.Unix())); err != nil {
		return 0, err
	}

	// Write schema version (fixed to 1)
	if err := binary.Write(buf, binary.LittleEndian, record.SchemaVersion); err != nil {
		return 0, err
	}

	// Write Labels content
	labelsBuffer := new(bytes.Buffer)
	for _, label := range record.Labels {
		// Write the label type (e.g., 0 = string, 1 = int, 2 = float)
		if err := binary.Write(labelsBuffer, binary.LittleEndian, label.Type); err != nil {
			return 0, err
		}

		// Write the length of the label value
		if err := binary.Write(labelsBuffer, binary.LittleEndian, uint64(len(label.Value))); err != nil {
			return 0, err
		}

		// Write the actual label value
		if _, err := labelsBuffer.Write(label.Value); err != nil {
			return 0, err
		}
	}
	labelsSize := labelsBuffer.Len()
	labelsCount := uint64(len(record.Labels))

	// Write size of Labels
	if err := binary.Write(buf, binary.LittleEndian, uint64(labelsSize)); err != nil {
		return 0, err
	}

	// Write count of Labels
	if err := binary.Write(buf, binary.LittleEndian, labelsCount); err != nil {
		return 0, err
	}

	// Append labels buffer to main buffer
	buf.Write(labelsBuffer.Bytes())

	// Write size of Message
	messageSize := uint64(len(record.Message))
	if err := binary.Write(buf, binary.LittleEndian, messageSize); err != nil {
		return 0, err
	}

	// Write Message content
	if _, err := buf.Write(record.Message); err != nil {
		return 0, err
	}

	// Now calculate the total size of the record (header + labels + message)
	totalSize := uint64(buf.Len()) // Calculate size after the buffer is filled

	// Prepend total size at the beginning of the buffer
	finalBuffer := new(bytes.Buffer)
	if err := binary.Write(finalBuffer, binary.LittleEndian, totalSize); err != nil {
		return 0, err
	}
	finalBuffer.Write(buf.Bytes()) // Write the rest of the buffer

	// Write the final buffer to file atomically
	return l.fd.Write(finalBuffer.Bytes())
}

func (l *LogRecordBinaryProcessor) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fd.Close()
}

func (l *LogRecordBinaryProcessor) ScanLogRecord() (domain.LogRecord, int, error) {
	record := domain.NewEmptyLogRecord()
	var totalSize, schemaVersion, labelsSize, messageSize uint64

	l.mu.Lock()
	defer l.mu.Unlock()

	// Read total size (labels + message)
	if err := binary.Read(l.fd, binary.LittleEndian, &totalSize); err != nil {
		return record, 0, err
	}

	// Read timestamp (as uint64 and convert to time.Time)
	var timestamp uint64
	if err := binary.Read(l.fd, binary.LittleEndian, &timestamp); err != nil {
		return record, 0, err
	}
	record.Timestamp = time.Unix(int64(timestamp), 0)

	// Read schema version (just to validate, fixed to 1)
	if err := binary.Read(l.fd, binary.LittleEndian, &schemaVersion); err != nil {
		return record, 0, err
	}

	// Read size of Labels
	if err := binary.Read(l.fd, binary.LittleEndian, &labelsSize); err != nil {
		return record, 0, err
	}

	// Read count of Labels
	var labelsCount uint64
	if err := binary.Read(l.fd, binary.LittleEndian, &labelsCount); err != nil {
		return record, 0, err
	}
	// Read Labels content
	record.Labels = make([]domain.Label, labelsCount) // Assuming labelsSize is the count
	for i := 0; i < int(labelsCount); i++ {
		var labelType uint8
		var labelLength uint64

		// Read the label type
		if err := binary.Read(l.fd, binary.LittleEndian, &labelType); err != nil {
			return record, 0, err
		}

		// Read the label length
		if err := binary.Read(l.fd, binary.LittleEndian, &labelLength); err != nil {
			return record, 0, err
		}

		// Read the label value
		labelValue := make([]byte, labelLength)
		if _, err := l.fd.Read(labelValue); err != nil {
			return record, 0, err
		}

		// Add label to record
		record.Labels[i] = domain.Label{Type: labelType, Value: labelValue}
	}

	// Read size of Message
	if err := binary.Read(l.fd, binary.LittleEndian, &messageSize); err != nil {
		return record, 0, err
	}

	// Read Message content
	record.Message = make([]byte, messageSize)
	if _, err := l.fd.Read(record.Message); err != nil {
		return record, 0, err
	}

	return record, 0, nil
}

// NewLogRecordBinaryProcessor creates a new LogRecordBinaryProcessor
func NewLogRecordBinaryProcessor(fd *os.File) *LogRecordBinaryProcessor {
	return &LogRecordBinaryProcessor{fd: fd}
}

// NewFileWriter creates a new LogRecordBinaryProcessor for writing
func NewFileWriter(fileName string) (*LogRecordBinaryProcessor, error) {
	fd, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0644)
	if err != nil {
		return nil, err
	}
	return NewLogRecordBinaryProcessor(fd), nil
}

// NewFileReader creates a new LogRecordBinaryProcessor for reading
func NewFileReader(fileName string) (*LogRecordBinaryProcessor, error) {
	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return NewLogRecordBinaryProcessor(fd), nil
}
