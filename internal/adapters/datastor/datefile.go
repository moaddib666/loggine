package datastor

import (
	"LogDb/internal/domain"
	"LogDb/internal/internal_errors"
	"LogDb/internal/ports"
	"io"
	"os"
)

type DataFileManager struct {
	source *domain.DataFile
	dph    *domain.DataPageHeader

	dpDataStart int64

	cursor uint64
	codec  ports.Serializer
}

// writeHeader writes data file header to the writer
func (d *DataFileManager) writeHeader() error {
	ret, err := d.source.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	if _, err := d.codec.WriteFileHeader(d.source.Header, d.source); err != nil {
		return err
	}
	if _, err := d.source.Seek(ret, io.SeekStart); err != nil {
		return err
	}
	return nil
}

// GetHeader returns the data file header read from disk and cached in memory
func (d *DataFileManager) GetHeader() (*domain.DataFileHeader, error) {
	if d.source.Header == nil {
		d.source.Header = domain.NewEmptyDataFileHeader()
		ret, err := d.source.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}

		if _, err := d.codec.ReadFileHeader(d.source.Header, d.source); err != nil {
			return nil, err
		}

		if _, err := d.source.Seek(ret, io.SeekStart); err != nil {
			return nil, err
		}
	}
	return d.source.Header, nil
}

// GetDataPage returns the data page from the data file
func (d *DataFileManager) GetDataPage(pageNumber uint32) (*domain.DataPage, error) {
	header, err := d.GetHeader()
	if err != nil {
		return nil, err
	}

	if pageNumber < 0 || pageNumber > domain.MaxDataPagesInDataFile || pageNumber < header.FirstDataPageNumber || pageNumber > header.LastDataPageNumber {
		return nil, internal_errors.DataPageNumberOutOfRange
	}
	page, err := d.GetCurrentDataPage()
	if err != nil {
		return nil, err
	}
	if page.Header.Number > pageNumber {
		page, err = d.FirstDataPage()
		if err != nil {
			return nil, err
		}
	}
	for page.Header.Number < pageNumber {
		page, err = d.NextDataPage()
		if err != nil {
			return nil, err
		}
	}
	if page.Header.Number != pageNumber {
		return nil, internal_errors.DataPageNumberOutOfRange
	}
	return page, nil
}

// CreateDataPage creates a new data page in the data file
func (d *DataFileManager) CreateDataPage(pageNumber uint32) (*domain.DataPage, error) {
	fileHeader, err := d.GetHeader()
	if err != nil {
		return nil, err
	}
	if pageNumber <= fileHeader.LastDataPageNumber {
		return nil, internal_errors.AttemptToWriteToDataInPast
	}
	_, err = d.GetCurrentDataPage()
	if err != nil {
		return nil, err
	}
	ret, err := d.source.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	d.dph = domain.NewEmptyDataPageHeader()
	d.dph.Number = pageNumber
	d.dpDataStart += int64(domain.DataPageHeaderSize) + ret
	d.codec.WriteDataPageHeader(d.dph, d.source)
	d.source.Header.LastDataPageNumber = pageNumber
	return domain.NewDataPage(d.dph, d.source), nil
}

// readDataPage reads data page from the data file
func (d *DataFileManager) readDataPage() (*domain.DataPage, error) {
	_, err := d.codec.ReadDataPageHeader(d.dph, d.source)
	if err != nil {
		return nil, err
	}
	return domain.NewDataPage(d.dph, d.source), nil
}

// FirstDataPage returns the first data page in the data file
func (d *DataFileManager) FirstDataPage() (*domain.DataPage, error) {
	d.dph = domain.NewEmptyDataPageHeader()
	_, err := d.source.Seek(int64(domain.DataFileHeaderSize), io.SeekStart)
	if err != nil {
		return nil, err
	}
	dataPage, err := d.readDataPage()
	if err != nil {
		return nil, err
	}
	d.dpDataStart = int64(domain.DataFileHeaderSize) + int64(domain.DataPageHeaderSize)
	return dataPage, nil

}

// GetCurrentDataPage returns the current data page in the data file
func (d *DataFileManager) GetCurrentDataPage() (*domain.DataPage, error) {
	if d.dph == nil {
		return d.FirstDataPage()
	}
	_, err := d.source.Seek(d.dpDataStart, io.SeekStart)
	if err != nil {
		return nil, err
	}
	page := domain.NewDataPage(d.dph, d.source)

	return page, nil
}

// NextDataPage returns the next data page in the data file
func (d *DataFileManager) NextDataPage() (*domain.DataPage, error) {
	currentDataPage, err := d.GetCurrentDataPage()
	if err != nil {
		return nil, err
	}
	if currentDataPage.Header.Number >= domain.MaxDataPagesInDataFile || currentDataPage.Header.Number >= d.source.Header.LastDataPageNumber {
		return nil, internal_errors.DataPageNumberOutOfRange
	}

	d.source.Seek(int64(currentDataPage.Header.PageSize), io.SeekCurrent)

	currentDataPage, err = d.readDataPage()
	if err != nil {
		return nil, err
	}
	d.dpDataStart += int64(domain.DataPageHeaderSize) + int64(currentDataPage.Header.PageSize)
	return currentDataPage, nil
}

// Close closes the data file
func (d *DataFileManager) Close() error {
	if d.source == nil {
		return nil
	}
	if err := d.writeHeader(); err != nil {
		return err
	}
	return d.source.Close()
}

// DataFileManagerFactory creates a new DataFileManager
type DataFileManagerFactory struct {
	Codec ports.Serializer
}

// NewDataFileManager creates a new DataFileManager
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

// FromDataFile creates a new DataFileManager from a DataFile
func (d *DataFileManagerFactory) FromDataFile(df *domain.DataFile) ports.DataFileManager {
	return &DataFileManager{
		source: df,
		codec:  d.Codec,
	}
}

// NewDataFileManagerFactory creates a new DataFileManagerFactory
func NewDataFileManagerFactory(codec ports.Serializer) *DataFileManagerFactory {
	return &DataFileManagerFactory{
		Codec: codec,
	}
}
