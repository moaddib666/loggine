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
	"os"
	"path"
	"sync"
	"time"
)

type DataFileWriter struct {
	source                *domain.DataFile
	currentDataPageHeader *domain.DataPageHeader
	codec                 ports.Serializer
	compressorFactory     ports.CompressionFactoryMethod
	compressionType       compression_types.CompressionType
	logger                *logrus.Entry
	logsBuffer            *bytes.Buffer
	mu                    sync.Mutex
	flushErrChan          chan error
}

// TODO: refactor this to use data pages instead of templating it here

// NewDataFileWriter creates a new DataFileWriter
func NewDataFileWriter(codec ports.Serializer, compressor ports.CompressionFactoryMethod, logger *logrus.Entry) *DataFileWriter {
	return &DataFileWriter{
		codec:             codec,
		compressorFactory: compressor,
		logger:            logger,
		logsBuffer:        &bytes.Buffer{},
		flushErrChan:      make(chan error, 1),
	}
}

// Open opens the data file for writing
func (d *DataFileWriter) Open(basedir, fileName string) error {
	f, err := os.OpenFile(path.Join(basedir, fileName), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	d.source = domain.NewDataFile(domain.NewEmptyDataFileHeader(), f)
	_, err = d.codec.ReadFileHeader(d.source.Header, f)
	if err != nil {
		return err
	}
	_, err = d.source.Seek(0, io.SeekEnd)
	return err
}

// Create creates a new data file
func (d *DataFileWriter) Create(basedir string, id uint32, y, m, day uint64) error {
	header := domain.NewDataFileHeader(1, id, y, m, day)
	fh, err := os.OpenFile(path.Join(basedir, header.String()), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	d.source = domain.NewDataFile(header, fh)
	return nil
}

// Close flushes any remaining data and closes the file
func (d *DataFileWriter) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.source == nil {
		return nil
	}
	if d.currentDataPageHeader != nil {
		if err := d.updateCurrentDataPageHeader(); err != nil {
			return err
		}
	}
	_, err := d.source.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = d.codec.WriteFileHeader(d.source.Header, d.source)
	if err != nil {
		return err
	}
	return d.source.Close()
}

// updateCurrentDataPageHeader updates the current data page header
func (d *DataFileWriter) updateCurrentDataPageHeader() error {

	if err := d.flushBuffer(); err != nil {
		return err
	}

	if d.currentDataPageHeader == nil {
		return internal_errors.DataPageNotSelected
	}
	var err error
	if d.currentDataPageHeader.CompressionAlgorithm != compression_types.None {
		_, err = d.source.Seek(int64(d.currentDataPageHeader.CompressedPageSize), io.SeekEnd)
	} else {
		_, err = d.source.Seek(int64(d.currentDataPageHeader.PageSize), io.SeekEnd)
	}
	if err != nil {
		return err
	}
	_, err = d.codec.WriteDataPageHeader(d.currentDataPageHeader, d.source)

	// seek to the end of the file
	return err
}

// CreateDataPage creates a new data page in the data file
func (d *DataFileWriter) CreateDataPage(pageNumber uint32) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if pageNumber >= domain.MaxDataPagesInDataFile || pageNumber <= d.source.Header.LastDataPageNumber {
		return internal_errors.DataPageNumberOutOfRange
	}

	// Flush the current data page if it exists
	if d.currentDataPageHeader != nil {
		if err := d.updateCurrentDataPageHeader(); err != nil {
			return err
		}
	}

	// Create a new data page header
	d.currentDataPageHeader = domain.NewEmptyDataPageHeader()
	d.currentDataPageHeader.Number = pageNumber

	_, err := d.source.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	_, err = d.codec.WriteDataPageHeader(d.currentDataPageHeader, d.source)
	if err != nil {
		return err
	}

	d.source.Header.LastDataPageNumber = pageNumber
	return nil
}

// WriteLogRecord writes a log record to the buffer
func (d *DataFileWriter) WriteLogRecord(record *domain.LogRecord) error {
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
	if d.logsBuffer.Len() >= 1024*1024 {
		return d.flushBuffer()
	}

	return nil
}

// flushBuffer compresses and writes the buffer to the data file
func (d *DataFileWriter) flushBuffer() error {
	if d.logsBuffer.Len() == 0 {
		return nil
	}

	writer := d.compressorFactory(d.compressionType)

	// Create a copy of the buffer to avoid blocking writes
	bufferCopy := bytes.NewBuffer(d.logsBuffer.Bytes())
	d.logsBuffer.Reset()

	// Compress and write the data
	compSizeChan := make(chan int64, 1)
	go func() {
		compSize, err := writer.CompressStream(bufferCopy, d.source)
		if err != nil {
			d.flushErrChan <- err
			return
		}
		compSizeChan <- compSize
	}()

	// Wait for the compression to complete
	select {
	case compSize := <-compSizeChan:
		d.currentDataPageHeader.CompressedPageSize += uint64(compSize)
	case err := <-d.flushErrChan:
		return err
	case <-time.After(30 * time.Second):
		return errors.New("flush timeout")
	}

	return nil
}

// NewDataFileWriterFactory creates a new DefaultDataFileFactory
func NewDataFileWriterFactory(baseDir string, codec ports.Serializer, compressorFactory ports.CompressionFactoryMethod, logger *logrus.Entry) ports.DataFileWriterFactory {
	return &DefaultDataFileFactory{
		codec:             codec,
		compressorFactory: compressorFactory,
		logger:            logger,
		baseDir:           baseDir,
	}
}
