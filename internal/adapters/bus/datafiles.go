package bus

import (
	"LogDb/internal/domain"
	log "github.com/sirupsen/logrus"
	"sync"
)

// DataFilesManager implements both DataFilesChangesPropagator and DataFilesChangesSubscriber interfaces.
type DataFilesManager struct {
	mu            sync.Mutex
	onFileDeleted []func(header *domain.DataFileHeader)
	onFileCreated []func(header *domain.DataFileHeader)
}

// DataFileDeleted notifies all subscribers about a deleted data file.
func (m *DataFilesManager) DataFileDeleted(header *domain.DataFileHeader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	log.Debugf("Notifying about dataFile deleted %s", header)
	for _, callback := range m.onFileDeleted {
		callback(header)
	}
}

// DataFileCreated notifies all subscribers about a created data file.
func (m *DataFilesManager) DataFileCreated(header *domain.DataFileHeader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	log.Debugf("Notifying about dataFile created %s", header)
	for _, callback := range m.onFileCreated {
		callback(header)
	}
}

// OnDataFileDeleted registers a callback function to be executed when a data file is deleted.
func (m *DataFilesManager) OnDataFileDeleted(callback func(header *domain.DataFileHeader)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onFileDeleted = append(m.onFileDeleted, callback)
}

// OnDataFileCreated registers a callback function to be executed when a data file is created.
func (m *DataFilesManager) OnDataFileCreated(callback func(header *domain.DataFileHeader)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onFileCreated = append(m.onFileCreated, callback)
}

// NewDataFilesManager creates a new DataFilesManager.
func NewDataFilesManager() *DataFilesManager {
	return &DataFilesManager{
		onFileDeleted: make([]func(header *domain.DataFileHeader), 0),
		onFileCreated: make([]func(header *domain.DataFileHeader), 0),
	}
}
