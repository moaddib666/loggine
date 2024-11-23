package datastor

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"github.com/sirupsen/logrus"
	"io"
)

type DefaultDataFileFactory struct {
	codec             ports.Serializer
	compressorFactory ports.CompressionFactoryMethod
	logger            *logrus.Entry
	repo              ports.DataFileRepository
}

// NewDataFileWriterFactory creates a new DefaultDataFileFactory
func NewDataFileWriterFactory(repo ports.DataFileRepository, compressorFactory ports.CompressionFactoryMethod, logger *logrus.Entry) ports.DataFileWriterFactory {
	return &DefaultDataFileFactory{
		codec:             repo.Codec(),
		compressorFactory: compressorFactory,
		logger:            logger,
		repo:              repo,
	}
}

// FromDataFile creates a new DataFileWriter from a DataFile
func (f *DefaultDataFileFactory) FromDataFile(dataFile *domain.DataFile) (ports.DataFileWriter, error) {
	return NewDataFileWriter(dataFile, f.codec, f.logger), nil
}

//// New creates a new instance of DataFileWriter
//func (f *DefaultDataFileFactory) New() (ports.DataFileWriter, error) {
//	dataFileHeader := domain.NewEmptyDataFileHeader()
//	dataFile, err := domain.NewWriteOnlyDataFile(dataFileHeader, f.constructDataFileLocation(dataFileHeader.String()))
//	if err != nil {
//		f.logger.WithError(err).Error("failed to create data file")
//		return nil, err
//	}
//	if err := f.init(dataFile); err != nil {
//		return nil, err
//	}
//	return NewDataFileWriter(dataFile, f.codec, f.logger), nil
//}

// init initializes the data file writer
func (f *DefaultDataFileFactory) init(dataFile *domain.DataFile) error {
	if _, err := dataFile.Seek(0, 0); err != nil {
		f.logger.WithError(err).Error("failed to seek to the beginning of the file)")
	}
	if _, err := f.codec.WriteFileHeader(dataFile.Header, dataFile); err != nil {
		f.logger.WithError(err).Error("failed to write data file header")
		return err
	}
	if _, err := dataFile.Seek(0, io.SeekEnd); err != nil {
		f.logger.WithError(err).Error("failed to seek to the end of the file")
		return err
	}
	return nil
}

// Create creates a new data file with the given ID and date
func (f *DefaultDataFileFactory) Create(y, m, day uint64) (ports.DataFileWriter, error) {
	dataFile, err := f.repo.Create(y, m, day)
	if err != nil {
		f.logger.WithError(err).Error("failed to create data file")
		return nil, err
	}
	if err := f.init(dataFile); err != nil {
		return nil, err
	}
	dataFileWriter := NewDataFileWriter(dataFile, f.codec, f.logger)
	return dataFileWriter, nil
}

// Open opens an existing data file for writing
func (f *DefaultDataFileFactory) Open(fileName string) (ports.DataFileWriter, error) {
	dataFile, err := f.repo.Open(fileName)
	if err != nil {
		f.logger.WithError(err).Error("failed to open data file")
		return nil, err
	}
	// Read the header
	if _, err := dataFile.Seek(0, 0); err != nil {
		f.logger.WithError(err).Error("failed to seek to the beginning of the file")
		return nil, err
	}

	if _, err := f.codec.ReadFileHeader(dataFile.Header, dataFile); err != nil {
		f.logger.WithError(err).Error("failed to read data file header")
		return nil, err
	}
	// Seek to the end of the file
	if _, err := dataFile.Seek(0, io.SeekEnd); err != nil {
		f.logger.WithError(err).Error("failed to seek to the end of the file")
		return nil, err
	}
	dataFileWriter := NewDataFileWriter(dataFile, f.codec, f.logger)
	return dataFileWriter, nil
}
