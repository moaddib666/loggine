package datastor

import (
	"LogDb/internal/domain"
	"LogDb/internal/internal_errors"
	"LogDb/internal/ports"
	"errors"
	"fmt"
	"time"
)

var _ ports.DataStorage = new(PersistentStorage)

type PersistentStorage struct {
	// File system
	//baseDir string
	//fileExt string

	// Indexes
	primaryIndex ports.Index
	indexes      []ports.Index

	// Writer
	memTable ports.MemTable

	// Reader
	dataPageReaderFactory  ports.DataPageReaderFactory
	dataFileManagerFactory ports.DataFileReaderFactory
}

// NewPersistentStorage creates a new persistent storage
func NewPersistentStorage(memTable ports.MemTable, dataFileManagerFactory ports.DataFileReaderFactory, dataPageReaderFactory ports.DataPageReaderFactory, primaryIndex ports.Index, indexes ...ports.Index) *PersistentStorage {
	storage := &PersistentStorage{
		primaryIndex:           primaryIndex,
		indexes:                indexes,
		memTable:               memTable,
		dataFileManagerFactory: dataFileManagerFactory,
		dataPageReaderFactory:  dataPageReaderFactory,
	}
	storage.initIndexes()
	return storage
}

// GetFileExt returns the file extension
//func (p *PersistentStorage) GetFileExt() string {
//	return defaultFileExt
//}

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

// StoreLogRecord stores the log record in the persistent storage
func (p *PersistentStorage) StoreLogRecord(record *domain.LogRecord) error {
	return p.memTable.Add(record)
}

// Query queries the log records in the persistent storage
func (p *PersistentStorage) Query(query ports.PreparedQuery) (*domain.QueryResult, error) {
	// Query the primary index
	query.Begin()
	defer query.End()
	idxOperations, err := p.primaryIndex.GetDataFilesForRead(query)
	if err != nil {
		query.SetError(fmt.Errorf("failed to query primary index: %w", err))
		return query.Result()
	}
	// Query the secondary indexes if any

	// Iterate over the data files
	for _, idxOp := range idxOperations {
		dataFileManager, err := p.dataFileManagerFactory.NewDataFileManager(idxOp.GetDataFileHeader().String())
		if err != nil {
			query.SetError(fmt.Errorf("failed to get data file header: %w", err))
			return query.Result()
		}
		defer dataFileManager.Close()
		for {
			dataPageHeader, err := dataFileManager.NextDataPage()
			if err != nil {
				if errors.Is(err, internal_errors.NoDataPagesLeft) {
					break
				}
				query.SetError(fmt.Errorf("failed to get data page: %w", err))
				return query.Result()
			}
			if dataPageHeader.RecordCount < 1 {
				continue
			}
			// Initialize the data page reader
			pageReader := p.dataPageReaderFactory.NewDataPageReader(dataPageHeader, dataFileManager.GetDataPageReader())

			// Process each record in the page
			for i := 0; i < int(dataPageHeader.RecordCount); i++ {
				if !pageReader.Scan() {
					break
				}

				meta := pageReader.Metadata()
				labels, err := pageReader.Labels()
				if err != nil {
					fmt.Printf("Error reading labels: %v\n", err)
					break
				}
				message, err := pageReader.Message()
				if err != nil {
					fmt.Printf("Error reading message: %v\n", err)
					break
				}
				logRecord := &domain.LogRecord{
					SchemaVersion: meta.SchemaVersion,
					Labels:        labels,
					Message:       message,
					Timestamp:     time.Unix(int64(meta.Timestamp), 0),
				}
				err = query.Next(logRecord)
				if err != nil {
					query.SetError(fmt.Errorf("failed to process record: %w", err))
					break
				}
			}
		}

	}
	for _, idxOp := range idxOperations {
		idxOp.Done()
	}
	return query.Result()
}

func (p *PersistentStorage) Close() error {
	return nil
}
