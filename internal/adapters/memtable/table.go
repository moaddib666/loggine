package memtable

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type Generic struct {
	activeChunk      ports.HeapChunk
	newChunk         func(maxSize, maxRecords int) ports.HeapChunk
	maxSize          int
	maxRecords       int
	maxFlushInterval time.Duration
	flushable        ports.Flushable
	lastFlushTime    time.Time
	rwMu             sync.RWMutex
	flushMu          sync.Mutex
	flushQueue       []ports.HeapChunk
}

func (mt *Generic) Flush() {
	mt.RotateChunk()
}

var _ ports.MemTable = &Generic{}

// NewMemTable creates a new MemTable with auto-flush routine.
func NewMemTable(maxSize, maxRecords int, newChunk func(maxSize int, maxRecords int) ports.HeapChunk, flushable ports.Flushable, maxFlushInterval time.Duration) *Generic {
	memTable := &Generic{
		activeChunk:      newChunk(maxSize, maxRecords),
		newChunk:         newChunk,
		maxSize:          maxSize,
		maxRecords:       maxRecords,
		flushable:        flushable,
		flushQueue:       make([]ports.HeapChunk, 0),
		lastFlushTime:    time.Now(),
		maxFlushInterval: maxFlushInterval,
	}

	go memTable.autoFlush()

	return memTable
}

// autoFlush monitors the MemTable and triggers flush if no writes occur for 5 seconds.
func (mt *Generic) autoFlush() {
	for {
		time.Sleep(5 * time.Second)
		mt.rwMu.RLock()
		if time.Since(mt.lastFlushTime) > mt.maxFlushInterval && mt.activeChunk.Size() > 0 {
			mt.rwMu.RUnlock()
			mt.RotateChunk()
		} else {
			mt.rwMu.RUnlock()
		}
	}
}

// Add inserts a LogRecord into the active chunk.
func (mt *Generic) Add(record *domain.LogRecord) error {
	mt.rwMu.Lock()
	defer mt.rwMu.Unlock()

	if mt.activeChunk.IsFull() {
		mt.RotateChunk()
	}

	return mt.activeChunk.Add(record)
}

// RotateChunk moves the current active chunk to the flush queue and creates a new active chunk.
func (mt *Generic) RotateChunk() {
	mt.rwMu.Lock()
	oldChunk := mt.activeChunk
	mt.activeChunk = mt.newChunk(mt.maxSize, mt.maxRecords)
	mt.lastFlushTime = time.Now()
	mt.rwMu.Unlock()

	// Make the old chunk immutable and add it to the flush queue
	oldChunk.MakeImmutable()
	mt.flushMu.Lock()
	mt.flushQueue = append(mt.flushQueue, oldChunk)
	mt.flushMu.Unlock()

	// Trigger asynchronous flushing
	go mt.flushChunks()
}

// flushChunks processes the flush queue asynchronously.
func (mt *Generic) flushChunks() {
	mt.flushMu.Lock()
	defer mt.flushMu.Unlock()
	// Update the last write time
	mt.lastFlushTime = time.Now()

	for len(mt.flushQueue) > 0 {
		chunk := mt.flushQueue[0]
		mt.flushQueue = mt.flushQueue[1:]

		if err := mt.flushable.FlushChunk(chunk); err != nil {
			log.WithError(err).Error("Failed to flush chunk")
		} else {
			log.Debug("Successfully flushed chunk")
		}
	}
}

// IsFull checks if the active chunk is full.
func (mt *Generic) IsFull() bool {
	mt.rwMu.RLock()
	defer mt.rwMu.RUnlock()

	return mt.activeChunk.IsFull()
}
