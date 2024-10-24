package repository

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"os"
	"sync"
	"time"
)

type DataFileWriter struct {
	codec  ports.Serializer
	fh     *os.File
	header *domain.DataFileHeader

	currentDataPage *domain.DataPageHeader
	dataPageWriter  bytes.Buffer

	mu     sync.Mutex
	offset int64
}

// NewDataFileWriter creates a new DataFileWriter
func NewDataFileWriter(codec ports.Serializer, ts time.Time) *DataFileWriter {
	return &DataFileWriter{
		codec: codec,
		header: &domain.DataFileHeader{
			Version:     1,
			Id:          uuid.New().ID(),
			Year:        uint64(ts.Year()),
			Month:       uint64(ts.Month()),
			Day:         uint64(ts.Day()),
			RecordCount: 0,
			Checksum:    0,
		},
	}
}

// writeHeader writes the file header to the file
func (d *DataFileWriter) writeHeader() error {
	if d.header == nil {
		return errors.New("header is not set")
	}
	d.fh.Seek(0, io.SeekStart)
	n, err := d.codec.WriteFileHeader(d.header, d.fh)
	if err != nil {
		return err
	}
	d.offset = int64(n)
	d.fh.Seek(d.offset, io.SeekStart)
	return nil
}

// saveDataPage saves the current data page to the file
func (d *DataFileWriter) saveDataPage() error {
	if d.currentDataPage == nil {
		return errors.New("no data page to save")
	}
	d.currentDataPage.PageSize = uint64(d.dataPageWriter.Len())
	d.codec.WriteDataPageHeader(d.currentDataPage, d.fh)
	// wrote writer to file
	n, err := d.fh.Write(d.dataPageWriter.Bytes())
	if err != nil {
		return err
	}
	d.offset += int64(n) + int64(domain.DataPageHeaderSize)
	d.dataPageWriter.Reset()
	return nil
}

// newDataPage creates a new data page
func (d *DataFileWriter) newDataPage(i uint32) error {
	if d.currentDataPage != nil {
		if d.currentDataPage.Number >= i {
			return errors.New("invalid page number")
		}
		if err := d.saveDataPage(); err != nil {
			return err
		}
	}
	d.currentDataPage = &domain.DataPageHeader{
		Number:      i,
		PageSize:    0,
		RecordCount: 0,
	}
	return nil
}

// Open opens the file for writing
func (d *DataFileWriter) Open() error {
	file, err := os.OpenFile(fmt.Sprintf("%d-%d-%d.%d.chunk", d.header.Year, d.header.Month, d.header.Day, d.header.Id), os.O_RDWR|os.O_CREATE, 0600)
	if err == nil {
		d.fh = file
	}
	return err
}

// Close closes the file
func (d *DataFileWriter) Close() error {
	if d.currentDataPage != nil {
		if err := d.saveDataPage(); err != nil {
			return err
		}
	}
	return d.fh.Close()
}

// AppendLogRecord appends a log record to the current data page
func (d *DataFileWriter) AppendLogRecord(record domain.LogRecord) error {
	if d.currentDataPage == nil {
		if err := d.newDataPage(0); err != nil {
			return err
		}
	}
	// TODO: Normalize logs so that end of minute does not have logs from new minute

	if record.Timestamp.Day() != int(d.header.Day) {
		return errors.New("day mismatch")
	}
	if record.Timestamp.Month() != time.Month(d.header.Month) {
		return errors.New("month mismatch")
	}
	if record.Timestamp.Year() != int(d.header.Year) {
		return errors.New("year mismatch")
	}
	logRecordMin := uint32(record.Timestamp.Minute())
	if logRecordMin != d.currentDataPage.Number {
		if logRecordMin < d.currentDataPage.Number {
			return errors.New("minute mismatch - log record is from the past")
		}
		if err := d.newDataPage(logRecordMin); err != nil {
			return err
		}
		d.currentDataPage.Number = logRecordMin
	}

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

	// Write schema version (assuming you have this field)
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
		return fmt.Errorf("buffer size mismatch: expected %d, got %d", len(buf), offset)
	}

	// Write the buffer to the data page writer
	n, err := d.dataPageWriter.Write(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return fmt.Errorf("incomplete write: wrote %d bytes, expected %d", n, len(buf))
	}

	// Update the record count
	d.currentDataPage.RecordCount++
	return nil
}
