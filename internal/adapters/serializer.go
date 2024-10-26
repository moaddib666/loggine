package adapters

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const (
	// LogRecordBinaryHeaderSize is the size of the header in bytes
	LogRecordBinaryHeaderSize = 64
)

// LogRecordBinaryProcessor implements the binary serialization for LogRecord
type LogRecordBinaryProcessor struct {
	fd                  *os.File
	mu                  sync.Mutex
	processRecordsCount int
}

//func (l *LogRecordBinaryProcessor) WriteLogRecord(record domain.LogRecord) (int, error) {
//	l.mu.Lock()
//	defer l.mu.Unlock()
//	var timestamp, recordSize, schemaVersion, labelsSize, labelsCount, messageSize uint64
//	// Create a buffer to hold the record header
//	headerBuf := new(bytes.Buffer)
//	// Create a buffer to hold the labels
//	labelsBuf := new(bytes.Buffer)
//	// Create a buffer to hold the message
//	messageBuf := new(bytes.Buffer)
//
//	// Write Labels content
//	for _, label := range record.Labels {
//		currentLabel := new(bytes.Buffer)
//		// Write the label type (e.g., 0 = string, 1 = int, 2 = float)
//		if err := binary.Write(currentLabel, binary.LittleEndian, label.Type); err != nil {
//			return 0, err
//		}
//
//		// Write the length of the label value
//		if err := binary.Write(currentLabel, binary.LittleEndian, uint64(len(label.Value))); err != nil {
//			return 0, err
//		}
//
//		// Write the actual label value
//		if err := binary.Write(currentLabel, binary.LittleEndian, label.Value); err != nil {
//			return 0, err
//		}
//		labelsBuf.Write(currentLabel.Bytes())
//	}
//	labelsSize = uint64(labelsBuf.Len())
//	labelsCount = uint64(len(record.Labels))
//
//	// Write Message content
//	if err := binary.Write(messageBuf, binary.LittleEndian, record.Message); err != nil {
//		return 0, err
//	}
//	messageSize = uint64(messageBuf.Len())
//
//	// Fill and write header buffer
//	timestamp = uint64(record.Timestamp.Unix())
//	schemaVersion = record.SchemaVersion
//	labelsSize = uint64(labelsBuf.Len())
//	labelsCount = uint64(len(record.Labels))
//	messageSize = uint64(messageBuf.Len())
//	recordSize = labelsSize + messageSize
//	if err := binary.Write(headerBuf, binary.LittleEndian, recordSize); err != nil {
//		return 0, err
//	}
//	if err := binary.Write(headerBuf, binary.LittleEndian, timestamp); err != nil {
//		return 0, err
//	}
//	if err := binary.Write(headerBuf, binary.LittleEndian, schemaVersion); err != nil {
//		return 0, err
//	}
//	if err := binary.Write(headerBuf, binary.LittleEndian, labelsSize); err != nil {
//		return 0, err
//	}
//	if err := binary.Write(headerBuf, binary.LittleEndian, labelsCount); err != nil {
//		return 0, err
//	}
//	if err := binary.Write(headerBuf, binary.LittleEndian, messageSize); err != nil {
//		return 0, err
//	}
//	// Write the header buffer to the file
//	finalBuffer := new(bytes.Buffer)
//	finalBuffer.Write(headerBuf.Bytes())  // 64 bytes
//	finalBuffer.Write(labelsBuf.Bytes())  // N bytes
//	finalBuffer.Write(messageBuf.Bytes()) // M bytes
//
//	// Write the final buffer to file atomically
//	return l.fd.Write(finalBuffer.Bytes())
//}

func (l *LogRecordBinaryProcessor) WriteLogRecord(record domain.LogRecord) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Calculate sizes upfront
	var labelsSize uint64
	for _, label := range record.Labels {
		// Each label consists of:
		// - Type (1 byte)
		// - Value length (8 bytes)
		// - Value (variable length)
		labelsSize += 1 + 8 + uint64(len(label.Value))
	}
	labelsCount := uint64(len(record.Labels))
	messageSize := uint64(len(record.Message))

	// Header size consists of six uint64 fields (6 * 8 bytes)
	const headerSize = 8 * 6

	// Total record size
	recordSize := uint64(headerSize) + labelsSize + messageSize

	// Pre-allocate a single buffer with the exact size needed
	buf := make([]byte, recordSize)
	offset := 0

	// Write record size
	binary.LittleEndian.PutUint64(buf[offset:], recordSize)
	offset += 8

	// Write timestamp
	timestamp := uint64(record.Timestamp.Unix())
	binary.LittleEndian.PutUint64(buf[offset:], timestamp)
	offset += 8

	// Write schema version
	binary.LittleEndian.PutUint64(buf[offset:], record.SchemaVersion)
	offset += 8

	// Write labels size
	binary.LittleEndian.PutUint64(buf[offset:], labelsSize)
	offset += 8

	// Write labels count
	binary.LittleEndian.PutUint64(buf[offset:], labelsCount)
	offset += 8

	// Write message size
	binary.LittleEndian.PutUint64(buf[offset:], messageSize)
	offset += 8

	// Write labels
	for _, label := range record.Labels {
		// Write label type
		buf[offset] = label.Type
		offset += 1

		// Write length of label value
		binary.LittleEndian.PutUint64(buf[offset:], uint64(len(label.Value)))
		offset += 8

		// Write label value
		copy(buf[offset:], label.Value)
		offset += len(label.Value)
	}

	// Write message
	copy(buf[offset:], record.Message)
	offset += len(record.Message)

	// Ensure the offset matches the buffer length
	if offset != len(buf) {
		return 0, fmt.Errorf("buffer size mismatch: expected %d, got %d", len(buf), offset)
	}

	// Write the buffer to the file atomically
	n, err := l.fd.Write(buf)
	if err != nil {
		return n, err
	}
	return n, nil
}

func (l *LogRecordBinaryProcessor) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fd.Close()
}

// ScannedRecordsCount returns the number of records scanned
func (l *LogRecordBinaryProcessor) ScannedRecordsCount() int {
	return l.processRecordsCount
}

// incrementProcessedRecords
func (l *LogRecordBinaryProcessor) incrementProcessedRecords() {
	l.processRecordsCount++
}
func (l *LogRecordBinaryProcessor) ScanLogRecord(f ports.FilterSet) (domain.LogRecord, int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var record domain.LogRecord
	headerSize := 8 * 6 // 6 uint64 fields
	var totalBytesRead int

	for {
		// Read the fixed-size header
		headerBuf := make([]byte, headerSize)
		n, err := io.ReadFull(l.fd, headerBuf)
		if err == io.EOF {
			// End of file reached
			return record, 0, io.EOF
		} else if err != nil {
			return record, 0, err
		}
		l.incrementProcessedRecords()
		totalBytesRead += n

		offset := 0

		// Read RecordSize
		recordSize := binary.LittleEndian.Uint64(headerBuf[offset:])
		offset += 8

		// Read Timestamp
		timestamp := binary.LittleEndian.Uint64(headerBuf[offset:])
		offset += 8

		// Check timestamp filter
		if f.IsBefore(timestamp) {
			// Skip the rest of the record
			skipBytes := int64(recordSize - uint64(headerSize))
			if skipBytes > 0 {
				if _, err := l.fd.Seek(skipBytes, io.SeekCurrent); err != nil {
					return record, 0, err
				}
			}
			totalBytesRead += int(skipBytes)
			continue
		}

		if f.IsAfter(timestamp) {
			return record, 0, io.EOF
		}

		// Read SchemaVersion
		schemaVersion := binary.LittleEndian.Uint64(headerBuf[offset:])
		offset += 8

		// Read LabelsSize
		labelsSize := binary.LittleEndian.Uint64(headerBuf[offset:])
		offset += 8

		// Read LabelsCount
		labelsCount := binary.LittleEndian.Uint64(headerBuf[offset:])
		offset += 8

		// Read MessageSize
		messageSize := binary.LittleEndian.Uint64(headerBuf[offset:])
		offset += 8

		// Read labels and message data
		dataSize := int(labelsSize + messageSize)
		dataBuf := make([]byte, dataSize)
		n, err = io.ReadFull(l.fd, dataBuf)
		if err != nil {
			return record, 0, err
		}
		totalBytesRead += n
		dataOffset := 0

		// Parse Labels
		record.Labels = make([]domain.Label, labelsCount)
		for i := 0; i < int(labelsCount); i++ {
			// Read label type (1 byte)
			if dataOffset >= len(dataBuf) {
				return record, 0, fmt.Errorf("unexpected end of data when reading label type at label %d", i)
			}
			labelType := dataBuf[dataOffset]
			dataOffset++

			// Read label length (8 bytes)
			if dataOffset+8 > len(dataBuf) {
				return record, 0, fmt.Errorf("unexpected end of data when reading label length at label %d", i)
			}
			labelLength := binary.LittleEndian.Uint64(dataBuf[dataOffset:])
			dataOffset += 8

			// Read label value
			if dataOffset+int(labelLength) > len(dataBuf) {
				// Debugging output
				fmt.Printf("Label %d: labelLength=%d, dataOffset=%d, dataBufLen=%d\n", i, labelLength, dataOffset, len(dataBuf))
				return record, 0, fmt.Errorf("unexpected end of data when reading label value at label %d", i)
			}
			labelValue := dataBuf[dataOffset : dataOffset+int(labelLength)]
			dataOffset += int(labelLength)

			record.Labels[i] = domain.Label{Type: labelType, Value: labelValue}
		}

		// Parse Message
		if dataOffset+int(messageSize) > len(dataBuf) {
			return record, 0, fmt.Errorf("unexpected end of data when reading message")
		}
		record.Message = dataBuf[dataOffset : dataOffset+int(messageSize)]
		dataOffset += int(messageSize)

		// Verify that all data has been consumed
		if dataOffset != len(dataBuf) {
			fmt.Printf("Data offset mismatch: dataOffset=%d, expected=%d\n", dataOffset, len(dataBuf))
			return record, 0, fmt.Errorf("data size mismatch in record parsing")
		}

		// Populate the rest of the record fields
		record.Timestamp = time.Unix(int64(timestamp), 0)
		record.SchemaVersion = schemaVersion

		return record, totalBytesRead, nil
	}
}

//func (l *LogRecordBinaryProcessor) ScanLogRecord(f ports.FilterSet) (domain.LogRecord, int, error) {
//	record := domain.NewEmptyLogRecord()
//	l.mu.Lock()
//	defer l.mu.Unlock()
//
//	var totalSize, schemaVersion, labelsSize, messageSize uint64
//	for {
//
//		// Read total size (labels + message)
//		if err := binary.Read(l.fd, binary.LittleEndian, &totalSize); err != nil {
//			return record, 0, err
//		}
//
//		// Read timestamp (as uint64 and convert to time.Time)
//		var timestamp uint64
//		if err := binary.Read(l.fd, binary.LittleEndian, &timestamp); err != nil {
//			return record, 0, err
//		}
//
//		if !f.FilterByTimeStamp(timestamp) {
//			end := totalSize
//			// skip the rest of the record
//			if _, err := l.fd.Seek(int64(end), 1); err != nil {
//				panic(err)
//			}
//			continue
//
//		}
//		record.Timestamp = time.Unix(int64(timestamp), 0)
//
//		// Read schema version (just to validate, fixed to 1)
//		if err := binary.Read(l.fd, binary.LittleEndian, &schemaVersion); err != nil {
//			return record, 0, err
//		}
//
//		// Read size of Labels
//		if err := binary.Read(l.fd, binary.LittleEndian, &labelsSize); err != nil {
//			return record, 0, err
//		}
//
//		// Read count of Labels
//		var labelsCount uint64
//		if err := binary.Read(l.fd, binary.LittleEndian, &labelsCount); err != nil {
//			return record, 0, err
//		}
//		// Read Labels content
//		record.Labels = make([]domain.Label, labelsCount) // Assuming labelsSize is the count
//		for i := 0; i < int(labelsCount); i++ {
//			var labelType uint8
//			var labelLength uint64
//
//			// Read the label type
//			if err := binary.Read(l.fd, binary.LittleEndian, &labelType); err != nil {
//				return record, 0, err
//			}
//
//			// Read the label length
//			if err := binary.Read(l.fd, binary.LittleEndian, &labelLength); err != nil {
//				return record, 0, err
//			}
//
//			// Read the label value
//			labelValue := make([]byte, labelLength)
//			if _, err := l.fd.Read(labelValue); err != nil {
//				return record, 0, err
//			}
//
//			// Add label to record
//			record.Labels[i] = domain.Label{Type: labelType, Value: labelValue}
//		}
//
//		// Read size of Message
//		if err := binary.Read(l.fd, binary.LittleEndian, &messageSize); err != nil {
//			return record, 0, err
//		}
//
//		// Read Message content
//		record.Message = make([]byte, messageSize)
//		if _, err := l.fd.Read(record.Message); err != nil {
//			return record, 0, err
//		}
//		return record, int(totalSize), nil
//
//	}
//	return record, 0, nil
//}

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
