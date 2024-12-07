package datastor

import (
	"LogDb/internal/domain"
	"LogDb/internal/internal_errors"
	"LogDb/internal/ports"
	"github.com/sirupsen/logrus"
	"io"
)

var _ ports.DataFileReader = &DataFileReader{}

type DataFileReader struct {
	source                    *domain.DataFile
	currentDataPageHeader     *domain.DataPageHeader
	numberOfDataPageBytesRead int64
	currentDataPageReader     io.ReadSeeker
	codec                     ports.Serializer
	logger                    *logrus.Entry // Use logger for debug logs
}

// GetHeader returns the data file header read from disk and cached in memory
func (d *DataFileReader) GetHeader() (*domain.DataFileHeader, error) {
	if d.source.Header == nil {
		d.logger.Debug("File header not found in memory. Reading from disk.")
		d.source.Header = domain.NewEmptyDataFileHeader()
		ret, err := d.source.Seek(0, io.SeekStart)
		d.logger.Debugf("Seek operation: Seek(0, SeekStart) returned position %d, error: %v", ret, err)
		if err != nil {
			return nil, err
		}

		if _, err := d.codec.ReadFileHeader(d.source.Header, d.source); err != nil {
			return nil, err
		}

		d.logger.Debugf("File header read from disk. Seeking back to position: %d", ret)
		ret, err = d.source.Seek(ret, io.SeekStart)
		d.logger.Debugf("Seek operation: Seek(%d, SeekStart) returned position %d, error: %v", ret, ret, err)
		if err != nil {
			return nil, err
		}
	}
	return d.source.Header, nil
}

// SelectDataPage returns the data page from the data file
func (d *DataFileReader) SelectDataPage(pageNumber uint32) error {
	d.logger.Debugf("Request to get data page number: %d", pageNumber)
	header, err := d.GetHeader()
	if err != nil {
		return err
	}

	if pageNumber < 0 || pageNumber > domain.MaxDataPagesInDataFile || pageNumber < header.FirstDataPageNumber || pageNumber > header.LastDataPageNumber {
		return internal_errors.DataPageNumberOutOfRange
	}
	_, err = d.GetCurrentDataPageHeader()
	if err != nil {
		return err
	}
	if d.currentDataPageHeader.Number > pageNumber {
		d.logger.Debugf("Current page number (%d) is greater than requested (%d). Seeking first page.", d.currentDataPageHeader.Number, pageNumber)
		err = d.FirstDataPage()
		if err != nil {
			return err
		}
	}
	for d.currentDataPageHeader.Number < pageNumber {
		d.logger.Debugf("Current page number (%d) is less than requested (%d). Loading next page.", d.currentDataPageHeader.Number, pageNumber)
		_, err = d.NextDataPage()
		if err != nil {
			return err
		}
	}
	if d.currentDataPageHeader.Number != pageNumber {
		d.logger.Errorf("Failed to load the requested page number: %d", pageNumber)
		return internal_errors.DataPageNumberOutOfRange
	}
	d.logger.Debugf("Successfully loaded data page number: %d", pageNumber)
	return nil
}

// CreateDataPage creates a new data page in the data file
func (d *DataFileReader) CreateDataPage(pageNumber uint32) error {
	d.logger.Debugf("Creating a new data page with number: %d", pageNumber)
	fileHeader, err := d.GetHeader()
	if err != nil {
		return err
	}
	if pageNumber <= fileHeader.LastDataPageNumber {
		d.logger.Errorf("Attempt to create a data page with number %d in the past", pageNumber)
		return internal_errors.AttemptToWriteToDataInPast
	}
	_, err = d.GetCurrentDataPageHeader()
	if err != nil {
		return err
	}
	ret, err := d.source.Seek(0, io.SeekEnd)
	d.logger.Debugf("Seek operation: Seek(0, SeekEnd) returned position %d, error: %v", ret, err)
	if err != nil {
		return err
	}
	d.currentDataPageHeader = domain.NewEmptyDataPageHeader()
	d.currentDataPageHeader.Number = pageNumber
	d.codec.WriteDataPageHeader(d.currentDataPageHeader, d.source)
	d.source.Header.LastDataPageNumber = pageNumber
	d.logger.Debugf("New data page with number %d created successfully.", pageNumber)
	return nil
}

// readDataPage reads data page from the data file
func (d *DataFileReader) readDataPage() error {
	d.logger.Debugf("Reading data page header from the file '%s'", d.source.File.Name())

	// Read the data page header
	_, err := d.codec.ReadDataPageHeader(d.currentDataPageHeader, d.source)
	if err != nil {
		return err
	}

	// Get the current position (right after reading the page header)
	currentPosition, err := d.source.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// Log the current position for debugging
	d.logger.Debugf("Current position after reading page header: %d", currentPosition)

	// Create a new SectionReader from the current position with the size of the current page
	if d.currentDataPageHeader.CompressedPageSize != 0 {
		d.currentDataPageReader = io.NewSectionReader(d.source, currentPosition, int64(d.currentDataPageHeader.CompressedPageSize))
	} else {
		d.currentDataPageReader = io.NewSectionReader(d.source, currentPosition, int64(d.currentDataPageHeader.PageSize))
	}
	// Reset the number of bytes read
	d.numberOfDataPageBytesRead = 0

	return nil
}

// FirstDataPage returns the first data page in the data file
func (d *DataFileReader) FirstDataPage() error {
	d.logger.Debug("Seeking the first data page.")
	d.currentDataPageHeader = domain.NewEmptyDataPageHeader()
	ret, err := d.source.Seek(int64(domain.DataFileHeaderSize), io.SeekStart)
	d.logger.Debugf("Seek operation: Seek(%d, SeekStart) returned position %d, error: %v", domain.DataFileHeaderSize, ret, err)
	if err != nil {
		return err
	}
	err = d.readDataPage()
	if err != nil {
		return err
	}
	return nil
}

// GetCurrentDataPageHeader returns the current data page in the data file
func (d *DataFileReader) GetCurrentDataPageHeader() (*domain.DataPageHeader, error) {
	if d.currentDataPageHeader == nil {
		return d.currentDataPageHeader, d.FirstDataPage()
	}
	return d.currentDataPageHeader, nil
}

// NextDataPage returns the next data page in the data file
func (d *DataFileReader) NextDataPage() (*domain.DataPageHeader, error) {
	if d.currentDataPageHeader == nil {
		return d.currentDataPageHeader, d.FirstDataPage()
	}
	if d.currentDataPageHeader.Number >= domain.MaxDataPagesInDataFile || d.currentDataPageHeader.Number >= d.source.Header.LastDataPageNumber {
		return nil, internal_errors.NoDataPagesLeft
	}
	// TODO: understand if any messages was read and minus thaier size from the page size to seek to the next page
	d.source.Seek(int64(d.currentDataPageHeader.PageSize), io.SeekCurrent)
	err := d.readDataPage()
	if err != nil {
		return nil, err
	}
	return d.currentDataPageHeader, nil
}

// GetDataPageReader returns the reader for the current data page
func (d *DataFileReader) GetDataPageReader() io.ReadSeeker {
	return d.currentDataPageReader
}

// Close closes the data file
func (d *DataFileReader) Close() error {
	d.logger.Debug("Closing the data file.")
	if d.source == nil {
		return nil
	}
	return d.source.Close()
}

// DataFileReaderFactory creates a new DataFileReader
type DataFileReaderFactory struct {
	logger *logrus.Entry
	repo   ports.DataFileRepository
}

// NewDataFileManager creates a new DataFileReader
func (d *DataFileReaderFactory) NewDataFileManager(fileName string) (ports.DataFileReader, error) {
	df, err := d.repo.Open(fileName)
	return d.FromDataFile(df), err
}

// FromDataFileHeader creates a new DataFileReader from a DataFileHeader
func (d *DataFileReaderFactory) FromDataFileHeader(dfh *domain.DataFileHeader) ports.DataFileReader {
	df, err := d.repo.Open(dfh.String())
	if err != nil {
		return nil
	}
	return d.FromDataFile(df)
}

// FromDataFile creates a new DataFileReader from a DataFile
func (d *DataFileReaderFactory) FromDataFile(df *domain.DataFile) ports.DataFileReader {
	return &DataFileReader{
		source: df,
		codec:  d.repo.Codec(),
		logger: d.logger,
	}
}

// NewDataFileManagerFactory creates a new DataFileReaderFactory
func NewDataFileManagerFactory(repo ports.DataFileRepository) *DataFileReaderFactory {
	logger := logrus.WithField("component", "DataFileReader")
	return &DataFileReaderFactory{
		repo:   repo,
		logger: logger,
	}
}
