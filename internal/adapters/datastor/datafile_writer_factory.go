package datastor

import (
	"LogDb/internal/ports"
	"github.com/sirupsen/logrus"
)

type DefaultDataFileFactory struct {
	codec             ports.Serializer
	compressorFactory ports.CompressionFactoryMethod
	logger            *logrus.Entry
	baseDir           string
}

// NewDefaultDataFileFactory creates a new DefaultDataFileFactory
func NewDefaultDataFileFactory(baseDir string, codec ports.Serializer, compressorFactory ports.CompressionFactoryMethod, logger *logrus.Entry) ports.DataFileWriterFactory {
	return &DefaultDataFileFactory{
		codec:             codec,
		compressorFactory: compressorFactory,
		logger:            logger,
		baseDir:           baseDir,
	}
}

// New creates a new instance of DataFileWriter
func (f *DefaultDataFileFactory) New() ports.DataFileWriter {
	return NewDataFileWriter(f.codec, f.compressorFactory, f.logger)
}

// Create creates a new data file with the given ID and date
func (f *DefaultDataFileFactory) Create(id uint32, y, m, day uint64) (ports.DataFileWriter, error) {
	dataFileWriter := f.New()
	err := dataFileWriter.Create(f.baseDir, id, y, m, day)
	if err != nil {
		return nil, err
	}
	return dataFileWriter, nil
}

// Open opens an existing data file for writing
func (f *DefaultDataFileFactory) Open(fileName string) (ports.DataFileWriter, error) {
	dataFileWriter := f.New()
	err := dataFileWriter.Open(f.baseDir, fileName)
	if err != nil {
		return nil, err
	}
	return dataFileWriter, nil
}
