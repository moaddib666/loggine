package datastor

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const defaultFileExt = ".chunk"

var _ ports.DataStorage = new(PersistentStorage)

type StorageWriteCursor struct {
	sync.Mutex
	DataFile *domain.DataFile
	DataPage *domain.DataPage
}

// SetDataFile sets the data file for the cursor
func (s *StorageWriteCursor) SetDataFile(df *domain.DataFile) {
	s.DataFile = df
}

// SetDataPage sets the data page for the cursor
func (s *StorageWriteCursor) SetDataPage(dp *domain.DataPage) {
	s.DataPage = dp
}

type PersistentStorage struct {
	primaryIndex ports.Index
	indexes      []ports.Index
	baseDir      string
	codec        ports.Serializer
	writeCursor  *StorageWriteCursor
	fileExt      string
}

// NewPersistentStorage creates a new persistent storage
func NewPersistentStorage(baseDir string, codec ports.Serializer, primaryIndex ports.Index, indexes ...ports.Index) *PersistentStorage {
	// create base dir if not exist
	if err := os.MkdirAll(baseDir, os.ModePerm); err != nil {
		panic(err)
	}
	stor := &PersistentStorage{
		baseDir:      baseDir,
		codec:        codec,
		primaryIndex: primaryIndex,
		indexes:      indexes,
		writeCursor: &StorageWriteCursor{
			DataFile: nil,
			DataPage: nil,
			Mutex:    sync.Mutex{},
		},
		fileExt: defaultFileExt,
	}
	stor.initIndexes()
	return stor
}

// GetFileExt returns the file extension
func (p *PersistentStorage) GetFileExt() string {
	return p.fileExt
}

// iniIndexes initializes the indexes
func (p *PersistentStorage) initIndexes() {
	// Bind the primary index to the storage
	err := p.primaryIndex.BindStorage(p)
	if err != nil {
		panic(err)
	}
	// Bind the secondary indexes to the storage
	for _, index := range p.indexes {
		err = index.BindStorage(p)
		if err != nil {
			panic(err)
		}
	}
}

func (p *PersistentStorage) GetDataFilesHeaders() ([]*domain.DataFileHeader, error) {
	var dataFiles []*domain.DataFileHeader
	// Scan the baseDir for all files ending with .chunk
	discoverFromPath := filepath.Join(p.baseDir, "*"+p.GetFileExt())
	log.Printf("Discovering files from path: %s", discoverFromPath)
	files, err := filepath.Glob(discoverFromPath)
	if err != nil {
		return nil, fmt.Errorf("failed to scan baseDir: %w", err)
	}

	// Iterate over each file and load the DataFileHeader
	for _, file := range files {
		var df domain.DataFileHeader
		fh, err := os.OpenFile(file, os.O_RDONLY, 0600)
		defer fh.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to open data file %s: %w", file, err)
		}
		_, err = p.codec.ReadFileHeader(&df, fh)
		if err != nil {
			return nil, fmt.Errorf("failed to read data file header: %w", err)
		}
		dataFiles = append(dataFiles, &df)
	}

	return dataFiles, nil

}

// deleteFromIndex deletes the data file from the indexes
func (p *PersistentStorage) deleteFromIndex(df *domain.DataFileHeader) error {
	// Delete from the primary index
	err := p.primaryIndex.DeleteDataFile(df)
	if err != nil {
		return fmt.Errorf("failed to delete data file from primary index: %w", err)
	}

	// Delete from the secondary indexes
	for _, index := range p.indexes {
		err = index.DeleteDataFile(df)
		if err != nil {
			return fmt.Errorf("failed to delete data file from secondary index: %w", err)
		}
	}

	return nil
}

// updateIndex updates the indexes with the new data file
func (p *PersistentStorage) updateIndex(df *domain.DataFileHeader) error {
	// TODO now headers not updated in index just added and removed
	// Update the primary index
	err := p.primaryIndex.AddDataFile(df)
	if err != nil {
		return fmt.Errorf("failed to update primary index: %w", err)
	}

	// Update the secondary indexes
	for _, index := range p.indexes {
		err = index.AddDataFile(df)
		if err != nil {
			return fmt.Errorf("failed to update secondary index: %w", err)
		}
	}
	return nil
}

func (p *PersistentStorage) GetDataFile(name string) (*domain.DataFile, error) {
	fpath := filepath.Join(p.baseDir, name+p.GetFileExt())
	fh, err := os.OpenFile(fpath, os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file %s: %w", name, err)
	}
	df := &domain.DataFile{
		Header: &domain.DataFileHeader{},
		File:   fh,
	}
	_, err = p.codec.ReadFileHeader(df.Header, fh)
	if err != nil {
		return nil, fmt.Errorf("failed to read data file header: %w", err)
	}
	return df, nil
}

func (p *PersistentStorage) CreateDataFile(name string, id uint32, y, m, d uint64) (*domain.DataFile, error) {
	fpath := filepath.Join(p.baseDir, name+p.GetFileExt())
	fh, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to create data file %s: %w", name, err)
	}
	df := &domain.DataFile{
		Header: &domain.DataFileHeader{
			Version: 1,
			Id:      id,
			Year:    y,
			Month:   m,
			Day:     d,
		},
		File: fh,
	}
	_, err = p.codec.WriteFileHeader(df.Header, fh)
	if err != nil {
		return nil, fmt.Errorf("failed to write data file header: %w", err)
	}
	return df, nil
}

func (p *PersistentStorage) GetDataPage(pageNumber uint32, df *domain.DataFile) (*domain.DataPage, error) {
	var dph domain.DataPageHeader
	for {
		// Read the data page header
		_, err := p.codec.ReadDataPageHeader(&dph, df.File)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to read data page header: %w", err)
		}
		if dph.Number < pageNumber {
			// Skip the data page
			_, err = df.File.Seek(int64(dph.PageSize), io.SeekCurrent)
			if err != nil {
				return nil, fmt.Errorf("failed to seek data page: %w", err)
			}
			continue
		}
		if dph.Number == pageNumber {

			return &domain.DataPage{
				Header:          &dph,
				ReadWriteSeeker: df.File,
			}, nil
		}
	}
	if dph.Number < pageNumber {
		return p.CreateDataPage(df, pageNumber)
	}
	return nil, fmt.Errorf("data page not found: %d", pageNumber)
}

func (p *PersistentStorage) CreateDataPage(df *domain.DataFile, pageNumber uint32) (*domain.DataPage, error) {
	// Create a new data page
	dph := &domain.DataPageHeader{
		Number:      pageNumber,
		PageSize:    0,
		RecordCount: 0,
	}
	_, err := p.codec.WriteDataPageHeader(dph, df.File)
	if err != nil {
		return nil, fmt.Errorf("failed to write data page header: %w", err)
	}
	df.Header.LastDataPageNumber = pageNumber
	return &domain.DataPage{
		Header:          dph,
		ReadWriteSeeker: df.File,
	}, nil
}

// updateDataPageHeader updates the data page header in the file
func (p *PersistentStorage) updateDataPageHeader(dp *domain.DataPage) error {
	// Seek to the start of the data page
	_, err := dp.ReadWriteSeeker.Seek(-int64(domain.DataPageHeaderSize)-int64(dp.Header.PageSize), io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to seek data page: %w", err)
	}
	// Write the data page header
	_, err = p.codec.WriteDataPageHeader(dp.Header, dp.ReadWriteSeeker)
	if err != nil {
		return fmt.Errorf("failed to write data page header: %w", err)
	}
	// Seek back to the end of the data page
	_, err = dp.ReadWriteSeeker.Seek(int64(dp.Header.PageSize), io.SeekCurrent)
	return nil
}

// updateDataFileHeader updates the data file header in the file
func (p *PersistentStorage) updateDataFileHeader(df *domain.DataFile) error {
	// Seek to the start of the file
	_, err := df.File.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek data file: %w", err)
	}
	// Write the data file header
	_, err = p.codec.WriteFileHeader(df.Header, df.File)
	if err != nil {
		return fmt.Errorf("failed to write data file header: %w", err)
	}
	// Seek back to the end of the file
	_, err = df.File.Seek(0, io.SeekEnd)
	return nil
}

// createNewDataFileForRecord creates a new data file and data page for the log record
func (p *PersistentStorage) createNewDataFileForRecord(record *domain.LogRecord) (*domain.DataFile, *domain.DataPage, error) {
	// Create a new data file
	header := &domain.DataFileHeader{
		Version:            1,
		Id:                 uuid.New().ID(),
		Year:               uint64(record.Timestamp.Year()),
		Month:              uint64(record.Timestamp.Month()),
		Day:                uint64(record.Timestamp.Day()),
		LastDataPageNumber: record.DataPageNumber(),
	}
	df, err := p.CreateDataFile(header.String(), header.Id, header.Year, header.Month, header.Day)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create data file: %w", err)
	}
	// Create a new data page
	dp, err := p.CreateDataPage(df, record.DataPageNumber())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create data page: %w", err)
	}
	return df, dp, nil
}

// closeDataFile closes the data file
func (p *PersistentStorage) closeDataFile(df *domain.DataFile) error {
	err := df.File.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync data file: %w", err)
	}
	if err := df.File.Close(); err != nil {
		return fmt.Errorf("failed to close data file: %w", err)
	}
	return p.updateIndex(df.Header)
}
func (p *PersistentStorage) StoreLogRecord(record *domain.LogRecord) error {
	p.writeCursor.Lock()
	defer p.writeCursor.Unlock()
	if p.writeCursor.DataFile == nil {
		// Create a new data file
		df, created, err := p.primaryIndex.GetDataFileForWrite(record)
		if err != nil {
			return err
		}
		var dp *domain.DataPage
		if created {
			dp, err = p.CreateDataPage(df, record.DataPageNumber())
			if err != nil {
				return fmt.Errorf("failed to create data page: %w", err)
			}
		} else {
			dp, err = p.GetDataPage(record.DataPageNumber(), df)
		}
		//df, dp, err := p.createNewDataFileForRecord(record)
		if err != nil {
			return fmt.Errorf("failed to create data file and data page: %w", err)
		}
		p.writeCursor.SetDataFile(df)
		p.writeCursor.SetDataPage(dp)
	} else if p.writeCursor.DataFile.Header.Year != uint64(record.Timestamp.UTC().Year()) || p.writeCursor.DataFile.Header.Month != uint64(record.Timestamp.UTC().Month()) || p.writeCursor.DataFile.Header.Day != uint64(record.Timestamp.UTC().Day()) || p.writeCursor.DataFile.Header.LastDataPageNumber > record.DataPageNumber() {
		// Create a new data file
		if p.writeCursor.DataPage != nil {
			// Update the data page header
			err := p.updateDataPageHeader(p.writeCursor.DataPage)
			if err != nil {
				return fmt.Errorf("failed to update data page header: %w", err)
			}
		}
		err := p.updateDataFileHeader(p.writeCursor.DataFile)
		if err != nil {
			return fmt.Errorf("failed to update data file header: %w", err)
		}
		err = p.closeDataFile(p.writeCursor.DataFile)
		if err != nil {
			return fmt.Errorf("failed to close data file: %w", err)
		}
		df, dp, err := p.createNewDataFileForRecord(record)
		if err != nil {
			return fmt.Errorf("failed to create data file and data page: %w", err)
		}
		p.writeCursor.SetDataFile(df)
		p.writeCursor.SetDataPage(dp)
	}

	if p.writeCursor.DataPage == nil {
		// Create a new data page
		panic("Unexpected state: data page is nil")
	}

	if p.writeCursor.DataPage.Header.Number != record.DataPageNumber() {
		// Update prev data page header
		err := p.updateDataPageHeader(p.writeCursor.DataPage)
		// Create a new data page
		dp, err := p.CreateDataPage(p.writeCursor.DataFile, record.DataPageNumber())
		if err != nil {
			return fmt.Errorf("failed to create data page: %w", err)
		}
		p.writeCursor.SetDataPage(dp)
	}
	// Seek to the end of the data page/file
	_, err := p.writeCursor.DataPage.ReadWriteSeeker.Seek(0, io.SeekEnd)
	// Write the log record to the data page
	n, err := p.codec.WriteLogRecord(record, p.writeCursor.DataPage.ReadWriteSeeker)
	if err != nil {
		return fmt.Errorf("failed to write log record: %w", err)
	}
	p.writeCursor.DataPage.Header.PageSize += uint64(n)
	p.writeCursor.DataPage.Header.RecordCount++
	p.writeCursor.DataFile.Header.LastDataPageNumber = record.DataPageNumber()
	p.writeCursor.DataFile.Header.RecordCount++
	return nil
}

func (p *PersistentStorage) Query(filterSet ports.FilterSet) (*domain.QueryResult, error) {
	//TODO implement me
	panic("implement me")
}

func (p *PersistentStorage) Close() error {
	// Close the write cursor
	if p.writeCursor.DataPage != nil {
		err := p.updateDataPageHeader(p.writeCursor.DataPage)
		if err != nil {
			return fmt.Errorf("failed to update data page header: %w", err)
		}
	}
	if p.writeCursor.DataFile != nil {
		err := p.updateDataFileHeader(p.writeCursor.DataFile)
		if err != nil {
			return fmt.Errorf("failed to update data file header: %w", err)
		}
		err = p.closeDataFile(p.writeCursor.DataFile)
		if err != nil {
			return fmt.Errorf("failed to close data file: %w", err)
		}
	}
	return nil
}
