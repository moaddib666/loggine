package datastor

import (
	"LogDb/internal/domain"
	"LogDb/internal/domain/compression_types"
	"LogDb/internal/internal_errors"
	"LogDb/internal/ports"
	"bytes"
	"errors"
	"github.com/sirupsen/logrus"
	"io"
	"sync"
	"time"
)

var _ ports.DataFileWriter = &DataFileWriter{}

type DataFileWriter struct {
	source                *domain.DataFile
	currentDataPageHeader *domain.DataPageHeader
	codec                 ports.Serializer
	logger                *logrus.Entry
	logsBuffer            *bytes.Buffer
	mu                    sync.Mutex
	flushErrChan          chan error
	bufferFlushSizeBytes  int
}

func (d *DataFileWriter) sync() error {
	d.logger.Debugf("Syncing data file %s", d.source.Header)
	if d.source == nil {
		return nil
	}
	if err := d.flushBuffer(); err != nil {
		return err
	}
	if err := d.flushCurrentDataPageHeader(); err != nil {
		return err
	}
	if err := d.flushDataFileHeader(); err != nil {
		return err
	}
	if err := d.source.Sync(); err != nil {
		return err
	}
	if _, err := d.source.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

func (d *DataFileWriter) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sync()
}

// flushDataFileHeader updates the data file header
func (d *DataFileWriter) flushDataFileHeader() error {
	d.logger.Debugf("Updating data file header %s", d.source.Header)
	_, err := d.source.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = d.codec.WriteFileHeader(d.source.Header, d.source)
	return err
}

func (d *DataFileWriter) GetLastDataPage() (*domain.DataPageHeader, error) {
	return d.currentDataPageHeader, nil
}

// NewDataFileWriter creates a new DataFileWriter
func NewDataFileWriter(dataFile *domain.DataFile, codec ports.Serializer, logger *logrus.Entry) *DataFileWriter {
	dfw := &DataFileWriter{
		codec:                codec,
		logger:               logger,
		logsBuffer:           &bytes.Buffer{},
		flushErrChan:         make(chan error, 1),
		source:               dataFile,
		bufferFlushSizeBytes: 1024 * 1024, // 1MB
	}
	// FIXME: Move this to configurable options
	WithAutoFlush(5*time.Second, dfw)
	return dfw
}

// WithAutoFlush(time time.Duration)
func WithAutoFlush(interval time.Duration, dfw *DataFileWriter) {
	go func() {
		for {
			select {
			case <-time.After(interval):
				err := dfw.Sync()
				if err != nil {
					return
				}
			}
		}
	}()
}

// Close flushes any remaining data and closes the file
func (d *DataFileWriter) Close() error {
	if err := d.Sync(); err != nil {
		return err
	}
	return d.source.Close()
}

// flushCurrentDataPageHeader updates the current data page header
func (d *DataFileWriter) flushCurrentDataPageHeader() error {
	if d.currentDataPageHeader == nil {
		return internal_errors.DataPageNotSelected
	}
	d.logger.Debugf("Updating data page header %s", d.currentDataPageHeader)
	// check if buffer is empty
	if d.logsBuffer.Len() != 0 {
		return errors.New("buffer is not empty")
	}
	var offset = int64(domain.DataPageHeaderSize)
	if d.currentDataPageHeader.CompressionAlgorithm != compression_types.None {
		offset += int64(d.currentDataPageHeader.CompressedPageSize)
	} else {
		offset += int64(d.currentDataPageHeader.PageSize)
	}
	if pos, err := d.source.Seek(-offset, io.SeekEnd); err != nil {
		return err
	} else {
		d.logger.Debugf("Seeked to %d", pos)
	}
	_, err := d.codec.WriteDataPageHeader(d.currentDataPageHeader, d.source)

	return err
}

// AppendDataPage creates a new data page in the data file
func (d *DataFileWriter) AppendDataPage(header *domain.DataPageHeader) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if header.Number >= domain.MaxDataPagesInDataFile || header.Number <= d.source.Header.LastDataPageNumber && d.source.Header.LastDataPageNumber != 0 {
		return internal_errors.DataPageNumberOutOfRange
	}

	// Flush the current data page if it exists
	if d.currentDataPageHeader != nil {
		// sync in hard operation use fast data update here
		if err := d.flushBuffer(); err != nil {
			return err
		}
		if err := d.flushCurrentDataPageHeader(); err != nil {
			return err
		}

		if _, err := d.source.Seek(0, io.SeekEnd); err != nil {
			return err
		}
	}

	// Create a new data page header
	d.currentDataPageHeader = header
	d.source.Header.LastDataPageNumber = header.Number
	// FIXME: looks like i need kind of a flag to indicate that the data page is empty
	if d.source.Header.RecordCount == 0 {
		d.source.Header.FirstDataPageNumber = header.Number
	}

	// Write 00 size of DataPageHeader to the file
	if _, err := d.codec.WriteDataPageHeader(header, d.source); err != nil {
		return err
	}

	return nil
}

// AppendLogRecordToCurrentDataPage writes a log record to the buffer
func (d *DataFileWriter) AppendLogRecordToCurrentDataPage(record *domain.LogRecord) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.currentDataPageHeader == nil {
		return internal_errors.DataPageNotSelected
	}

	recordSize, err := d.codec.WriteLogRecord(record, d.logsBuffer)
	if err != nil {
		return err
	}

	d.currentDataPageHeader.PageSize += uint64(recordSize)
	d.currentDataPageHeader.RecordCount++
	d.source.Header.RecordCount++

	// Flush the buffer if it exceeds 1MB
	if d.logsBuffer.Len() >= d.bufferFlushSizeBytes {
		return d.flushBuffer()
	}

	return nil
}

// flushBuffer compresses and writes the buffer to the data file
func (d *DataFileWriter) flushBuffer() error {
	if d.logsBuffer.Len() == 0 {
		return nil
	}
	d.logger.Debugf("Flushing buffer to data file %s", d.source.Header)
	// Compress and write the data

	if n, err := d.source.Write(d.logsBuffer.Bytes()); err != nil {
		// reshaping the buffer
		d.logsBuffer = bytes.NewBuffer(d.logsBuffer.Bytes()[n:])
		return err
	}
	// Reset the buffer
	d.logsBuffer.Reset()
	d.logger.Debugf("Flushed buffer to data file %s", d.source.Header)
	return nil
}

// Source returns the data file
func (d *DataFileWriter) Source() *domain.DataFile {
	return d.source
}
