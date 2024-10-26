package serializer

import (
	"LogDb/internal/domain"
	"encoding/binary"
	"io"
)

type BinarySerializer struct {
}

var Default = &BinarySerializer{}

// WriteLogRecord writes log record to writer
func (b *BinarySerializer) WriteLogRecord(record *domain.LogRecord, writer io.Writer) (int, error) {
	var err error
	// Header size consists of six uint64 fields (6 * 8 bytes)
	const headerSize = 8 * 6

	var labelsSize uint64
	for _, label := range record.Labels {
		labelsSize += 1 + 8 + uint64(len(label.Value))
	}
	labelsCount := uint64(len(record.Labels))
	messageSize := uint64(len(record.Message))

	// Total record size
	recordSize := uint64(headerSize) + labelsSize + messageSize

	recordMetaData := domain.RecordMeta{
		Timestamp:     uint64(record.Timestamp.Unix()),
		RecordSize:    recordSize,
		SchemaVersion: 1,
		LabelsSize:    labelsSize,
		LabelsCount:   labelsCount,
		MessageSize:   messageSize,
	}

	_, err = b.WriteLogRecordMeta(&recordMetaData, writer)

	// Write labels
	for _, label := range record.Labels {
		_, err = b.WriteLogLabel(&label, writer)
		if err != nil {
			return 0, err
		}
	}

	// Write message
	_, err = b.WriteLogRecordMessage(record.Message, writer)

	return int(recordSize), err

}

func (b *BinarySerializer) WriteLogRecordMeta(header *domain.RecordMeta, writer io.Writer) (int, error) {
	return domain.RecordMetaSize, binary.Write(writer, binary.LittleEndian, header)
}

func (b *BinarySerializer) WriteLogLabel(label *domain.Label, writer io.Writer) (int, error) {
	offset := 0
	// write label type
	if err := binary.Write(writer, binary.LittleEndian, label.Type); err != nil {
		return offset, err
	}
	offset += 1
	// write label size
	if err := binary.Write(writer, binary.LittleEndian, label.Size); err != nil {
		return offset, err
	}
	offset += 8
	// write label value
	if err := binary.Write(writer, binary.LittleEndian, label.Value); err != nil {
		return offset, err
	}
	offset += int(label.Size)
	return offset, nil
}

func (b *BinarySerializer) WriteLogRecordMessage(message []byte, writer io.Writer) (int, error) {
	err := binary.Write(writer, binary.LittleEndian, message)
	return len(message), err
}

func (b *BinarySerializer) ReadLogRecordMeta(header *domain.RecordMeta, reader io.Reader) (int, error) {
	return domain.RecordMetaSize, binary.Read(reader, binary.LittleEndian, header)
}

func (b *BinarySerializer) ReadLogLabel(label *domain.Label, reader io.Reader) (int, error) {
	offset := 0
	// read label type
	if err := binary.Read(reader, binary.LittleEndian, &label.Type); err != nil {
		return offset, err
	}
	offset += 1
	// read label size
	if err := binary.Read(reader, binary.LittleEndian, &label.Size); err != nil {
		return offset, err
	}
	offset += 8
	// read label value
	label.Value = make([]byte, label.Size)
	if err := binary.Read(reader, binary.LittleEndian, label.Value); err != nil {
		return offset, err
	}
	offset += int(label.Size)
	return offset, nil
}

func (b *BinarySerializer) ReadLogRecordMessage(message []byte, reader io.Reader) (int, error) {
	err := binary.Read(reader, binary.LittleEndian, message)
	return len(message), err
}

func (b *BinarySerializer) WriteDataPageHeader(header *domain.DataPageHeader, writer io.Writer) (int, error) {
	return domain.DataPageHeaderSize, binary.Write(writer, binary.LittleEndian, header)
}

func (b *BinarySerializer) ReadDataPageHeader(header *domain.DataPageHeader, reader io.Reader) (int, error) {
	return domain.DataPageHeaderSize, binary.Read(reader, binary.LittleEndian, header)
}

func (b *BinarySerializer) WriteFileHeader(header *domain.DataFileHeader, writer io.Writer) (int, error) {
	header.UpdateChecksum()
	return domain.DataFileHeaderSize, binary.Write(writer, binary.LittleEndian, header)
}

func (b *BinarySerializer) ReadFileHeader(header *domain.DataFileHeader, reader io.Reader) (int, error) {
	return domain.DataFileHeaderSize, binary.Read(reader, binary.LittleEndian, header)
}

// WriteWALHeader writes WAL header to writer
func (b *BinarySerializer) WriteWALHeader(header *domain.WALHeader, writer io.Writer) (int, error) {
	return domain.WALHeaderSize, binary.Write(writer, binary.LittleEndian, header)
}

// ReadWALHeader reads WAL header from reader
func (b *BinarySerializer) ReadWALHeader(header *domain.WALHeader, reader io.Reader) (int, error) {
	return domain.WALHeaderSize, binary.Read(reader, binary.LittleEndian, header)
}
