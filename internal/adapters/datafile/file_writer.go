package datafile

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

type Writer struct {
	codec  ports.Serializer
	fh     *os.File
	header *domain.DataFileHeader

	currentDataPage *domain.DataPageHeader
	dataPageWriter  bytes.Buffer

	mu     sync.Mutex // Mutex to prevent simultaneous writes or seeks
	offset int64
}

// NewDataFileWriter creates a new DataFileWriter
func NewDataFileWriter(codec ports.Serializer, ts time.Time) *Writer {
	return &Writer{
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
func (d *Writer) writeHeader() error {
	if d.header == nil {
		return errors.New("header is not set")
	}
	// Seek to the start of the file
	if _, err := d.fh.Seek(0, io.SeekStart); err != nil {
		return err
	}
	n, err := d.codec.WriteFileHeader(d.header, d.fh)
	if err != nil {
		return err
	}
	d.offset = int64(n)
	// Seek to the new offset
	if _, err := d.fh.Seek(d.offset, io.SeekStart); err != nil {
		return err
	}
	return nil
}

// saveDataPage saves the current data page to the file
func (d *Writer) saveDataPage() error {
	if d.currentDataPage == nil {
		return errors.New("no data page to save")
	}
	d.currentDataPage.PageSize = uint64(d.dataPageWriter.Len())
	// Write the page header
	if _, err := d.codec.WriteDataPageHeader(d.currentDataPage, d.fh); err != nil {
		return err
	}
	// Write the data page content
	n, err := d.fh.Write(d.dataPageWriter.Bytes())
	if err != nil {
		return err
	}
	d.offset += int64(n) + int64(domain.DataPageHeaderSize)
	d.dataPageWriter.Reset()
	d.fh.Seek(d.offset, io.SeekStart)
	return nil
}

// newDataPage creates a new data page
func (d *Writer) newDataPage(i uint32) error {
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
func (d *Writer) Open() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	fileName := fmt.Sprintf("%d-%d-%d.%d.chunk", d.header.Year, d.header.Month, d.header.Day, d.header.Id)
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	d.fh = file
	// Write the initial file header
	if err := d.writeHeader(); err != nil {
		d.fh.Close()
		return err
	}
	return nil
}

// Close closes the file
func (d *Writer) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.currentDataPage != nil {
		if err := d.saveDataPage(); err != nil {
			return err
		}
	}
	// Update the file header with the final record count
	if err := d.writeHeader(); err != nil {
		return err
	}

	if err := d.fh.Sync(); err != nil {
		return err
	}

	if err := d.fh.Close(); err != nil {
		return err
	}
	d.fh = nil
	return nil
}

// AppendLogRecord appends a log record to the current data page
func (d *Writer) AppendLogRecord(record domain.LogRecord) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.currentDataPage == nil {
		if err := d.newDataPage(0); err != nil {
			return err
		}
	}

	// Check if the record's date matches the data file header
	if record.Timestamp.Day() != int(d.header.Day) ||
		record.Timestamp.Month() != time.Month(d.header.Month) ||
		record.Timestamp.Year() != int(d.header.Year) {
		return errors.New("timestamp of record does not match data file date")
	}

	// Determine the minute number for the record (minutes since midnight)
	logRecordMin := uint32(record.Timestamp.Hour()*60 + record.Timestamp.Minute())
	if logRecordMin != d.currentDataPage.Number {
		if logRecordMin < d.currentDataPage.Number {
			return errors.New("minute mismatch - log record is from the past")
		}
		if err := d.newDataPage(logRecordMin); err != nil {
			return err
		}
	}

	// Calculate sizes upfront
	var labelsSize uint64
	for _, label := range record.Labels {
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

	// Update the record counts
	d.currentDataPage.RecordCount++
	d.header.RecordCount++

	return nil
}
