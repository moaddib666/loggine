package datafile

import (
	"LogDb/internal/domain"
	"LogDb/internal/internal_errors"
	"LogDb/internal/ports"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

type Reader struct {
	codec  ports.Serializer
	fh     *os.File
	header *domain.DataFileHeader

	currentDataPage *domain.DataPageHeader
	dataPageReader  io.Reader

	mu     sync.Mutex
	offset int64
}

// NewDataFileReader creates a new DataFileReader
func NewDataFileReader(codec ports.Serializer) *Reader {
	return &Reader{
		codec: codec,
	}
}
func (d *Reader) Open(fileName string) error {
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0600)
	if err == nil {
		d.fh = file
	}
	return err

}

func (d *Reader) GetHeader() (*domain.DataFileHeader, error) {
	if d.header == nil {
		d.fh.Seek(0, io.SeekStart)
		d.header = &domain.DataFileHeader{}
		n, err := d.codec.ReadFileHeader(d.header, d.fh)
		if err != nil {
			return nil, err
		}
		d.offset = int64(n)
		d.fh.Seek(d.offset, io.SeekStart)
	}
	return d.header, nil
}

func (d *Reader) Close() error {
	return d.fh.Close()
}

func (d *Reader) Query(filterSet ports.FilterSet) (*domain.QueryResult, error) {
	tStar := time.Now()
	_, err := d.fh.Seek(domain.DataFileHeaderSize, io.SeekStart)
	if err != nil {
		return nil, err
	}
	pageHeader, err := d.GetHeader()
	if err != nil {
		return nil, err
	}
	pageDate := uint64(pageHeader.Time().Unix())
	if filterSet.IsAfter(pageDate) {
		return nil, io.EOF
	}

	if filterSet.IsBefore(pageDate) {
		return nil, io.EOF
	}
	result, err := d.query(filterSet)
	result.SpentTime(time.Since(tStar))
	return result, err
}

// query reads data from the file
func (d *Reader) query(filterSet ports.FilterSet) (*domain.QueryResult, error) {
	var result domain.QueryResult = domain.QueryResult{}
	for {
		if err := d.readNextDataPage(); err != nil {
			if errors.Is(err, io.EOF) {
				return &result, nil
			}
			return nil, err
		}
		if err := d.readRecords(result, filterSet); err != nil {
			if errors.Is(err, internal_errors.PageEndReached) {
				continue
			}
			if errors.Is(err, internal_errors.RecordsOutOfRange) {
				return &result, nil
			}
			if errors.Is(err, internal_errors.RecordsLimitReached) {
				return &result, nil
			}
			return nil, err
		}
	}
}

// readNextDataPage reads next data page from file
func (d *Reader) readNextDataPage() error {
	// read data page header
	// read data page
	if d.currentDataPage != nil {
		d.fh.Seek(d.offset, io.SeekStart)
	}
	d.currentDataPage = &domain.DataPageHeader{}
	n, err := d.codec.ReadDataPageHeader(d.currentDataPage, d.fh)
	if err != nil {
		return err
	}
	d.offset += int64(n) + int64(d.currentDataPage.PageSize)
	d.dataPageReader = io.LimitReader(d.fh, int64(d.currentDataPage.PageSize))
	return nil
}

// readRecords reads records from the current data page
func (d *Reader) readRecords(result domain.QueryResult, filterSet ports.FilterSet) error {
	for {
		// TODO: use limit from filterSet
		//if len(result.Records) >= limit {
		//	return internal_errors.RecordsLimitReached
		//}
		meta := domain.RecordMeta{}
		_, err := d.codec.ReadLogRecordMeta(&meta, d.dataPageReader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return internal_errors.PageEndReached
			}
			return err
		}
		if filterSet.IsAfter(meta.Timestamp) {
			result.Miss()
			return internal_errors.RecordsOutOfRange
		}
		if filterSet.IsBefore(meta.Timestamp) {
			result.Miss()
			continue
		}

		// read labels
		labels := make([]domain.Label, meta.LabelsCount)
		for i := 0; i < int(meta.LabelsCount); i++ {
			label := domain.Label{}
			_, err := d.codec.ReadLogLabel(&label, d.dataPageReader)
			if err != nil {
				return err
			}
			labels[i] = label
		}

		// read message
		message := make([]byte, meta.MessageSize)
		_, err = d.codec.ReadLogRecordMessage(message, d.dataPageReader)
		if err != nil {
			return err
		}
		result.Hit(domain.LogRecord{
			Timestamp: time.Unix(int64(meta.Timestamp), 0),
			Labels:    labels,
			Message:   message,
		})
	}
}
