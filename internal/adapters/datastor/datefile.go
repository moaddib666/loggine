package datastor

import (
	"LogDb/internal/domain"
	"LogDb/internal/internal_errors"
	"LogDb/internal/ports"
	"github.com/sirupsen/logrus"
	"io"
	"os"
)

//type dataPageReaderWithCallback struct {
//	reader io.ReadSeeker
//	inc    func(n int64)
//	set    func(m int64)
//	closed bool
//}
//
//func (d *dataPageReaderWithCallback) Read(p []byte) (n int, err error) {
//	if d.closed {
//		return 0, io.EOF
//	}
//	n, err = d.reader.Read(p)
//	d.inc(int64(n))
//	return n, err
//}
//
//func (d *dataPageReaderWithCallback) Seek(offset int64, whence int) (int64, error) {
//	ret, err := d.reader.Seek(offset, whence)
//	d.set(ret)
//	return ret, err
//}
//
//func (d *dataPageReaderWithCallback) Close() error {
//	if d.closed {
//		return nil
//	}
//	d.closed = true
//	return nil
//}

// newDataReaderWithCallback creates a new dataPageReaderWithCallback
//func newDataReaderWithCallback(reader io.ReadSeeker, inc, set func(n int64)) io.ReadSeekCloser {
//	return &dataPageReaderWithCallback{
//		reader: reader,
//		inc:    inc,
//		set:    set,
//	}
//}

type DataFileReader struct {
	source                    *domain.DataFile
	currentDataPageHeader     *domain.DataPageHeader
	numberOfDataPageBytesRead int64
	currentDataPageReader     io.ReadSeeker
	codec                     ports.Serializer
	logger                    *logrus.Entry // Use logger for debug logs
}

// incrementNumberOfDataPageBytesRead increments the number of bytes read from the data page
//func (d *DataFileReader) incrementNumberOfDataPageBytesRead(n int64) {
//	d.numberOfDataPageBytesRead += n
//}

// setNumberOfDataPageBytesRead sets the number of bytes read from the data page
//func (d *DataFileReader) setNumberOfDataPageBytesRead(n int64) {
//	d.numberOfDataPageBytesRead = n
//}

// writeHeader writes data file header to the writer
func (d *DataFileReader) writeHeader() error {
	d.logger.Debugf("Seeking to the start of the file to write the header.")
	ret, err := d.source.Seek(0, io.SeekStart)
	d.logger.Debugf("Seek operation: Seek(0, SeekStart) returned position %d, error: %v", ret, err)
	if err != nil {
		return err
	}
	if _, err := d.codec.WriteFileHeader(d.source.Header, d.source); err != nil {
		return err
	}
	d.logger.Debugf("File header written. Seeking back to position: %d", ret)
	ret, err = d.source.Seek(ret, io.SeekStart)
	d.logger.Debugf("Seek operation: Seek(%d, SeekStart) returned position %d, error: %v", ret, ret, err)
	if err != nil {
		return err
	}
	return nil
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
	d.logger.Debug("Reading data page header from the file.")

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
	//d.currentDataPageReader = d.source
	d.currentDataPageReader = io.NewSectionReader(d.source, currentPosition, int64(d.currentDataPageHeader.PageSize))
	//d.currentDataPageReader = newDataReaderWithCallback(
	//	io.NewSectionReader(d.source, currentPosition, int64(d.currentDataPageHeader.PageSize)),
	//	d.incrementNumberOfDataPageBytesRead, d.setNumberOfDataPageBytesRead,
	//)

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
	//needShift := int64(d.currentDataPageHeader.PageSize) - d.numberOfDataPageBytesRead
	//if needShift > 0 {
	//	d.logger.Debugf("Seeking to the next data page. Need to shift %d bytes", needShift)
	//	ret, err := d.source.Seek(needShift, io.SeekCurrent)
	//	logrus.Debugf("Seek to next data page %d", ret)
	//	if err != nil {
	//		return nil, err
	//	}
	//} else {
	//	d.logger.Debugf("All records from the current data page were read.")
	//}
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

// DataFileManagerFactory creates a new DataFileReader
type DataFileManagerFactory struct {
	Codec  ports.Serializer
	logger *logrus.Entry
}

// NewDataFileManager creates a new DataFileReader
func (d *DataFileManagerFactory) NewDataFileManager(fileName string) (ports.DataFileManager, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	h := domain.NewEmptyDataFileHeader()
	_, err = d.Codec.ReadFileHeader(h, f)
	if err != nil {
		return nil, err
	}

	return d.FromDataFile(domain.NewDataFile(h, f)), nil

}

// FromDataFile creates a new DataFileReader from a DataFile
func (d *DataFileManagerFactory) FromDataFile(df *domain.DataFile) ports.DataFileManager {
	return &DataFileReader{
		source: df,
		codec:  d.Codec,
		logger: d.logger,
	}
}

// NewDataFileManagerFactory creates a new DataFileManagerFactory
func NewDataFileManagerFactory(codec ports.Serializer) *DataFileManagerFactory {
	logger := logrus.WithField("component", "DataFileReader")
	return &DataFileManagerFactory{
		Codec:  codec,
		logger: logger,
	}
}
