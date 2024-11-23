package ports

import "LogDb/internal/domain"

// DataFilesChangesPropagator defines the interface for a data files changes propagator.
type DataFilesChangesPropagator interface {
	DataFileDeleted(header *domain.DataFileHeader)
	DataFileCreated(header *domain.DataFileHeader)
}

// DataFilesChangesSubscriber defines the interface for a data files changes subscriber.
type DataFilesChangesSubscriber interface {
	OnDataFileDeleted(func(header *domain.DataFileHeader))
	OnDataFileCreated(func(header *domain.DataFileHeader))
}
