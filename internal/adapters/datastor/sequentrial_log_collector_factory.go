package datastor

import (
	"LogDb/internal/ports"
)

var _ ports.DataStorageWritableFactory = &SequentialLogCollectorFactory{}

type SequentialLogCollectorFactory struct {
	dfwf       ports.DataFileWriterFactory
	dphf       ports.DataPageHeaderFactory
	propagator ports.DataFilesChangesPropagator
}

func (s *SequentialLogCollectorFactory) NewDataStorageWritable() (ports.DataStorageWritable, error) {
	return NewSequentialLogCollector(
		s.dfwf,
		s.dphf,
		s.propagator,
	), nil
}

func NewSequentialLogCollectorFactory(dfwf ports.DataFileWriterFactory, dphf ports.DataPageHeaderFactory, propagator ports.DataFilesChangesPropagator) *SequentialLogCollectorFactory {
	return &SequentialLogCollectorFactory{
		dfwf:       dfwf,
		dphf:       dphf,
		propagator: propagator,
	}
}
