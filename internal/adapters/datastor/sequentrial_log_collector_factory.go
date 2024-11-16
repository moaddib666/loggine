package datastor

import (
	"LogDb/internal/ports"
)

var _ ports.DataStorageWritableFactory = &SequentialLogCollectorFactory{}

type SequentialLogCollectorFactory struct {
	dfwf ports.DataFileWriterFactory
	dphf ports.DataPageHeaderFactory
}

func (s *SequentialLogCollectorFactory) NewDataStorageWritable() (ports.DataStorageWritable, error) {
	return NewSequentialLogCollector(
		s.dfwf,
		s.dphf,
	), nil
}

func NewSequentialLogCollectorFactory(dfwf ports.DataFileWriterFactory, dphf ports.DataPageHeaderFactory) *SequentialLogCollectorFactory {
	return &SequentialLogCollectorFactory{
		dfwf: dfwf,
		dphf: dphf,
	}
}
