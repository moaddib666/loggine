package serializer

import (
	"LogDb/internal/domain"
	"encoding/binary"
	"io"
)

type BinarySerializer struct {
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
	return domain.DataFileHeaderSize, binary.Write(writer, binary.LittleEndian, header)
}

func (b *BinarySerializer) ReadFileHeader(header *domain.DataFileHeader, reader io.Reader) (int, error) {
	return domain.DataFileHeaderSize, binary.Read(reader, binary.LittleEndian, header)
}
