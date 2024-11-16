package ports

import "LogDb/internal/domain"

type MemTable interface {
	// Add a new log record to the MemTable.
	Add(record *domain.LogRecord) error

	// RotateChunk Mark the current active chunk as read-only and create a new chunk.
	RotateChunk()

	// Flush all immutable chunks asynchronously.
	Flush()

	// IsFull Check if the MemTable is full based on memory usage or record count.
	IsFull() bool
}

type HeapChunk interface {
	// Add a new log record to the heap.
	Add(record *domain.LogRecord) error

	// MakeImmutable Make the heap chunk immutable (read-only).
	MakeImmutable()

	// Size Get the current size of the heap.
	Size() int

	// IsFull Check if the heap chunk is full based on the provided thresholds.
	IsFull() bool

	// Pop Get the next log record in sorted order (by timestamp).
	Pop() (*domain.LogRecord, error)

	// IsImmutable Check if the heap chunk is read-only.
	IsImmutable() bool

	SizeInBytes() int
}

type Flushable interface {
	// FlushChunk Flush a chunk of log records to persistent storage.
	FlushChunk(chunk HeapChunk) error
	// Close closes the data file writer
	Close() error
}
