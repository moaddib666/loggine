package serializer_test

import (
	"LogDb/internal/adapters/serializer"
	"LogDb/internal/domain"
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

// Helper function to create a sample RecordMeta
func sampleRecordMeta() *domain.RecordMeta {
	return &domain.RecordMeta{
		Timestamp:     uint64(1638316800), // Example timestamp
		RecordSize:    uint64(128),
		SchemaVersion: uint64(1),
		LabelsSize:    uint64(32),
		LabelsCount:   uint64(2),
		MessageSize:   uint64(64),
	}
}

// Helper function to create a sample Label
func sampleLabel() *domain.Label {
	return &domain.Label{
		Type:  0, // Assuming 0 represents string type
		Size:  uint64(len("sample_value")),
		Value: []byte("sample_value"),
	}
}

// Helper function to create a sample DataPageHeader
func sampleDataPageHeader() *domain.DataPageHeader {
	return &domain.DataPageHeader{
		Number:      uint32(1),
		PageSize:    uint64(1024),
		RecordCount: uint64(10),
	}
}

// Helper function to create a sample DataFileHeader
func sampleDataFileHeader() *domain.DataFileHeader {
	return &domain.DataFileHeader{
		Version:     uint64(1),
		Id:          uint32(12345),
		Year:        uint64(2021),
		Month:       uint64(11),
		Day:         uint64(30),
		RecordCount: uint64(100),
		Checksum:    uint64(0),
	}
}

func TestBinarySerializer_WriteAndReadLogRecordMeta(t *testing.T) {
	serializer := &serializer.BinarySerializer{}
	meta := sampleRecordMeta()

	var buf bytes.Buffer
	n, err := serializer.WriteLogRecordMeta(meta, &buf)
	if err != nil {
		t.Fatalf("WriteLogRecordMeta failed: %v", err)
	}
	if n != domain.RecordMetaSize {
		t.Errorf("Expected to write %d bytes, wrote %d bytes", domain.RecordMetaSize, n)
	}

	var readMeta domain.RecordMeta
	n, err = serializer.ReadLogRecordMeta(&readMeta, &buf)
	if err != nil {
		t.Fatalf("ReadLogRecordMeta failed: %v", err)
	}
	if n != domain.RecordMetaSize {
		t.Errorf("Expected to read %d bytes, read %d bytes", domain.RecordMetaSize, n)
	}

	if !reflect.DeepEqual(meta, &readMeta) {
		t.Errorf("Original and read RecordMeta do not match.\nOriginal: %+v\nRead: %+v", meta, &readMeta)
	}
}

func TestBinarySerializer_WriteAndReadLogLabel(t *testing.T) {
	serializer := &serializer.BinarySerializer{}
	label := sampleLabel()

	var buf bytes.Buffer
	n, err := serializer.WriteLogLabel(label, &buf)
	if err != nil {
		t.Fatalf("WriteLogLabel failed: %v", err)
	}

	expectedSize := 1 + 8 + len(label.Value)
	if n != expectedSize {
		t.Errorf("Expected to write %d bytes, wrote %d bytes", expectedSize, n)
	}

	var readLabel domain.Label
	n, err = serializer.ReadLogLabel(&readLabel, &buf)
	if err != nil {
		t.Fatalf("ReadLogLabel failed: %v", err)
	}
	if n != expectedSize {
		t.Errorf("Expected to read %d bytes, read %d bytes", expectedSize, n)
	}

	if label.Type != readLabel.Type || label.Size != readLabel.Size || !bytes.Equal(label.Value, readLabel.Value) {
		t.Errorf("Original and read Label do not match.\nOriginal: %+v\nRead: %+v", label, &readLabel)
	}
}

func TestBinarySerializer_WriteAndReadLogRecordMessage(t *testing.T) {
	serializer := &serializer.BinarySerializer{}
	message := []byte("This is a sample log message.")

	var buf bytes.Buffer
	n, err := serializer.WriteLogRecordMessage(message, &buf)
	if err != nil {
		t.Fatalf("WriteLogRecordMessage failed: %v", err)
	}
	if n != len(message) {
		t.Errorf("Expected to write %d bytes, wrote %d bytes", len(message), n)
	}

	readMessage := make([]byte, len(message))
	n, err = serializer.ReadLogRecordMessage(readMessage, &buf)
	if err != nil {
		t.Fatalf("ReadLogRecordMessage failed: %v", err)
	}
	if n != len(message) {
		t.Errorf("Expected to read %d bytes, read %d bytes", len(message), n)
	}

	if !bytes.Equal(message, readMessage) {
		t.Errorf("Original and read messages do not match.\nOriginal: %s\nRead: %s", message, readMessage)
	}
}

func TestBinarySerializer_WriteAndReadDataPageHeader(t *testing.T) {
	serializer := &serializer.BinarySerializer{}
	header := sampleDataPageHeader()

	var buf bytes.Buffer
	n, err := serializer.WriteDataPageHeader(header, &buf)
	if err != nil {
		t.Fatalf("WriteDataPageHeader failed: %v", err)
	}
	if n != domain.DataPageHeaderSize {
		t.Errorf("Expected to write %d bytes, wrote %d bytes", domain.DataPageHeaderSize, n)
	}

	var readHeader domain.DataPageHeader
	n, err = serializer.ReadDataPageHeader(&readHeader, &buf)
	if err != nil {
		t.Fatalf("ReadDataPageHeader failed: %v", err)
	}
	if n != domain.DataPageHeaderSize {
		t.Errorf("Expected to read %d bytes, read %d bytes", domain.DataPageHeaderSize, n)
	}

	if !reflect.DeepEqual(header, &readHeader) {
		t.Errorf("Original and read DataPageHeader do not match.\nOriginal: %+v\nRead: %+v", header, &readHeader)
	}
}

func TestBinarySerializer_WriteAndReadDataFileHeader(t *testing.T) {
	serializer := &serializer.BinarySerializer{}
	header := sampleDataFileHeader()

	var buf bytes.Buffer
	n, err := serializer.WriteFileHeader(header, &buf)
	if err != nil {
		t.Fatalf("WriteFileHeader failed: %v", err)
	}
	if n != domain.DataFileHeaderSize {
		t.Errorf("Expected to write %d bytes, wrote %d bytes", domain.DataFileHeaderSize, n)
	}

	var readHeader domain.DataFileHeader
	n, err = serializer.ReadFileHeader(&readHeader, &buf)
	if err != nil {
		t.Fatalf("ReadFileHeader failed: %v", err)
	}
	if n != domain.DataFileHeaderSize {
		t.Errorf("Expected to read %d bytes, read %d bytes", domain.DataFileHeaderSize, n)
	}

	if !reflect.DeepEqual(header, &readHeader) {
		t.Errorf("Original and read DataFileHeader do not match.\nOriginal: %+v\nRead: %+v", header, &readHeader)
	}
}

// Additional test to cover full serialization and deserialization of a log record
func TestBinarySerializer_WriteAndReadFullLogRecord(t *testing.T) {
	serializer := &serializer.BinarySerializer{}
	meta := sampleRecordMeta()
	label1 := &domain.Label{
		Type:  0,
		Size:  uint64(len("value1")),
		Value: []byte("value1"),
	}
	label2 := &domain.Label{
		Type:  1,
		Size:  8, // Assuming 8 bytes for an int64
		Value: make([]byte, 8),
	}
	binary.LittleEndian.PutUint64(label2.Value, uint64(1234567890))
	// size 64
	message := []byte("Sample log message. This is a test. 1234567890")
	for len(message) < int(meta.MessageSize) {
		message = append(message, message...)[:meta.MessageSize]
	}

	var buf bytes.Buffer

	// Write RecordMeta
	_, err := serializer.WriteLogRecordMeta(meta, &buf)
	if err != nil {
		t.Fatalf("WriteLogRecordMeta failed: %v", err)
	}

	// Write Labels
	labels := []*domain.Label{label1, label2}
	for _, label := range labels {
		_, err := serializer.WriteLogLabel(label, &buf)
		if err != nil {
			t.Fatalf("WriteLogLabel failed: %v", err)
		}
	}

	// Write Message
	_, err = serializer.WriteLogRecordMessage(message, &buf)
	if err != nil {
		t.Fatalf("WriteLogRecordMessage failed: %v", err)
	}

	// Read RecordMeta
	var readMeta domain.RecordMeta
	_, err = serializer.ReadLogRecordMeta(&readMeta, &buf)
	if err != nil {
		t.Fatalf("ReadLogRecordMeta failed: %v", err)
	}

	// Read Labels
	readLabels := make([]*domain.Label, meta.LabelsCount)
	for i := uint64(0); i < meta.LabelsCount; i++ {
		var readLabel domain.Label
		_, err := serializer.ReadLogLabel(&readLabel, &buf)
		if err != nil {
			t.Fatalf("ReadLogLabel failed: %v", err)
		}
		readLabels[i] = &readLabel
	}

	// Read Message
	readMessage := make([]byte, meta.MessageSize)
	_, err = serializer.ReadLogRecordMessage(readMessage, &buf)
	if err != nil {
		t.Fatalf("ReadLogRecordMessage failed: %v", err)
	}

	// Validate RecordMeta
	if !reflect.DeepEqual(meta, &readMeta) {
		t.Errorf("Original and read RecordMeta do not match.\nOriginal: %+v\nRead: %+v", meta, &readMeta)
	}

	// Validate Labels
	if len(labels) != len(readLabels) {
		t.Fatalf("Expected %d labels, got %d", len(labels), len(readLabels))
	}
	for i := range labels {
		if labels[i].Type != readLabels[i].Type ||
			labels[i].Size != readLabels[i].Size ||
			!bytes.Equal(labels[i].Value, readLabels[i].Value) {
			t.Errorf("Label %d does not match.\nOriginal: %+v\nRead: %+v", i, labels[i], readLabels[i])
		}
	}

	// Validate Message
	if !bytes.Equal(message, readMessage) {
		t.Errorf("Original and read messages do not match.\nOriginal: %s\nRead: %s", message, readMessage)
	}
}
