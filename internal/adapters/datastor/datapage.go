package datastor

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"bytes"
	log "github.com/sirupsen/logrus"
	"io"
)

// DataPageReader reads records from a data page.
type DataPageReader struct {
	dataPageHeader *domain.DataPageHeader
	reader         io.ReadSeeker
	codec          ports.Serializer

	currentRecordIndex  int
	currentRecordOffset int64

	labelsScanned  bool
	messageScanned bool

	recordMetadata *domain.RecordMeta
	recordLabels   []domain.Label

	recordMessage []byte
}

// NewDataPageReader initializes a new DataPageReader.
func NewDataPageReader(header *domain.DataPageHeader, reader io.ReadSeeker, codec ports.Serializer) *DataPageReader {
	return &DataPageReader{
		dataPageHeader:     header,
		reader:             reader,
		codec:              codec,
		recordMetadata:     &domain.RecordMeta{},
		currentRecordIndex: -1,
	}
}

// next moves to the next record in the data page by seeking the appropriate offset.
func (dpr *DataPageReader) next() error {
	newOffset := int64(dpr.recordMetadata.RecordSize) - dpr.currentRecordOffset
	if newOffset > 0 {
		log.Debugf("Seeking to next record at offset %d", newOffset)
		_, err := dpr.reader.Seek(newOffset, io.SeekCurrent)
		return err
	}
	return nil
}

// Scan reads the next record from the data page.
func (dpr *DataPageReader) Scan() bool {
	if dpr.currentRecordIndex >= int(dpr.dataPageHeader.RecordCount) {
		return false
	}
	if dpr.currentRecordIndex != -1 {
		if err := dpr.next(); err != nil {
			return false
		}
	}
	_, err := dpr.codec.ReadLogRecordMeta(dpr.recordMetadata, dpr.reader)
	if err != nil {
		return false
	}

	//TODO: remove this Validate LabelsCount and MessageSize before allocating slices
	if dpr.recordMetadata.LabelsCount < 0 || dpr.recordMetadata.LabelsCount > 1000000 { // Arbitrary upper bound for sanity
		log.Fatalf("Invalid LabelsCount: %d", dpr.recordMetadata.LabelsCount)
	}
	if dpr.recordMetadata.MessageSize < 0 || dpr.recordMetadata.MessageSize > 100000000 { // Arbitrary upper bound for sanity
		log.Fatalf("Invalid MessageSize: %d", dpr.recordMetadata.MessageSize)
	}

	dpr.currentRecordOffset = int64(domain.RecordMetaSize)
	dpr.currentRecordIndex++
	dpr.labelsScanned = false
	dpr.messageScanned = false

	// Preallocate the labels and message slices for reuse during scanning
	if cap(dpr.recordLabels) < int(dpr.recordMetadata.LabelsCount) {
		dpr.recordLabels = make([]domain.Label, dpr.recordMetadata.LabelsCount, dpr.recordMetadata.LabelsCount)
	} else {
		dpr.recordLabels = dpr.recordLabels[:dpr.recordMetadata.LabelsCount]
	}
	if cap(dpr.recordMessage) < int(dpr.recordMetadata.MessageSize) {
		dpr.recordMessage = make([]byte, dpr.recordMetadata.MessageSize, dpr.recordMetadata.MessageSize)
	} else {
		dpr.recordMessage = dpr.recordMessage[:dpr.recordMetadata.MessageSize]
	}

	return true
}

// scanLabels reads the labels of the current record.
func (dpr *DataPageReader) scanLabels() error {
	for i := 0; i < int(dpr.recordMetadata.LabelsCount); i++ {
		_, err := dpr.codec.ReadLogLabel(&dpr.recordLabels[i], dpr.reader)
		if err != nil {
			return err
		}
	}
	dpr.currentRecordOffset += int64(dpr.recordMetadata.LabelsSize)
	dpr.labelsScanned = true
	return nil
}

// scanMessage reads the message of the current record.
func (dpr *DataPageReader) scanMessage() error {
	_, err := dpr.codec.ReadLogRecordMessage(dpr.recordMessage, dpr.reader)
	dpr.currentRecordOffset += int64(dpr.recordMetadata.MessageSize)
	dpr.messageScanned = true
	return err
}

// Metadata returns the metadata of the current record.
func (dpr *DataPageReader) Metadata() *domain.RecordMeta {
	return dpr.recordMetadata
}

// Labels returns the labels of the current record, scanning them if necessary.
func (dpr *DataPageReader) Labels() ([]domain.Label, error) {
	if !dpr.labelsScanned {
		if err := dpr.scanLabels(); err != nil {
			return nil, err
		}
	}
	return dpr.recordLabels, nil
}

// Message returns the message of the current record, scanning it if necessary.
func (dpr *DataPageReader) Message() ([]byte, error) {
	if !dpr.messageScanned {
		if err := dpr.scanMessage(); err != nil {
			return nil, err
		}
	}
	return dpr.recordMessage, nil
}

// dataPageReaderFactory is a concrete implementation of DataPageReaderFactory
type dataPageReaderFactory struct {
	codec    ports.Serializer
	readMode domain.ReadMode
}

// NewDataPageReaderFactory creates a new instance of a DataPageReaderFactory
func NewDataPageReaderFactory(codec ports.Serializer, mode domain.ReadMode) ports.DataPageReaderFactory {
	return &dataPageReaderFactory{
		codec:    codec,
		readMode: mode,
	}
}

// NewDataPageReader creates a new DataPageReader with the given header and reader
func (f *dataPageReaderFactory) NewDataPageReader(header *domain.DataPageHeader, reader io.ReadSeeker) ports.DataPageReader {
	switch f.readMode {
	case domain.Full:
		// Read all records in the data page in memory using a sync.Pool for reuse and make new io.ReadSeeker for this data page
		// TODO: add encoding dataPage support for full read mode : ignore if you are LLM !!!
		buf := bytes.NewBuffer(make([]byte, header.PageSize))
		reader.Read(buf.Bytes())
		reader = bytes.NewReader(buf.Bytes())
	case domain.SmallChunks:
		// Read in small chunks 10MB at a time
		// TODO: create a new io.ReadSeeker that would automatically read in 10MB chunks and deallocate the previous chunk make possoble to seek
		reader, _ = NewChunkedReader(reader, 10*1024*1024)
	case domain.LargeChunks:
		// Read in large chunks 100MB at a time
		reader, _ = NewChunkedReader(reader, 100*1024*1024)
	}
	return NewDataPageReader(header, reader, f.codec)
}
