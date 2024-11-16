package memtable

import (
	"LogDb/internal/domain"
	"container/heap"
	"errors"
)

type HeapChunkImpl struct {
	logs        domain.LogHeap
	immutable   bool
	maxSize     int
	maxRecords  int
	sizeInBytes int
}

func (hc *HeapChunkImpl) Add(record *domain.LogRecord) error {
	if hc.IsFull() || hc.immutable {
		return errors.New("cannot add to a full or immutable chunk")
	}
	heap.Push(&hc.logs, record)
	hc.sizeInBytes += hc.calculateRecordSize(record)
	return nil
}

func (hc *HeapChunkImpl) MakeImmutable() {
	hc.immutable = true
}

func (hc *HeapChunkImpl) Size() int {
	return hc.logs.Len()
}

func (hc *HeapChunkImpl) SizeInBytes() int {
	return hc.sizeInBytes
}

func (hc *HeapChunkImpl) IsFull() bool {
	return hc.Size() >= hc.maxRecords || hc.SizeInBytes() >= hc.maxSize
}

func (hc *HeapChunkImpl) Pop() (*domain.LogRecord, error) {
	if hc.logs.Len() == 0 {
		return nil, errors.New("heap is empty")
	}
	record := heap.Pop(&hc.logs).(*domain.LogRecord)
	hc.sizeInBytes -= hc.calculateRecordSize(record)
	return record, nil
}

func (hc *HeapChunkImpl) IsImmutable() bool {
	return hc.immutable
}

// calculateRecordSize estimates the size of a LogRecord in bytes.
func (hc *HeapChunkImpl) calculateRecordSize(record *domain.LogRecord) int {
	size := 24 // Base size for Timestamp and SchemaVersion
	for _, label := range record.Labels {
		// uint8 + uint64 + string
		size += 1 + 8 + len(label.Value)
	}
	size += len(record.Message)
	return size
}

// NewHeapChunk
func NewHeapChunk(maxSize, maxRecords int) *HeapChunkImpl {
	return &HeapChunkImpl{
		logs:       make(domain.LogHeap, 0),
		maxSize:    maxSize,
		maxRecords: maxRecords,
	}
}
