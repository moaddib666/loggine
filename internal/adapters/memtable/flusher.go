package memtable

import (
	"LogDb/internal/ports"
	"sync"
)

var _ ports.Flushable = &Flusher{}

type Flusher struct {
	wStorageFactory ports.DataStorageWritableFactory
	wg              sync.WaitGroup
}

// FlushChunk Flush a chunk of log records to persistent storage.
func (f *Flusher) FlushChunk(chunk ports.HeapChunk) error {
	// Add to f.wg to wait for all FlushChunk calls to finish before closing the flusher
	f.wg.Add(1)
	defer f.wg.Done()
	wStorage, err := f.wStorageFactory.NewDataStorageWritable()
	if err != nil {
		return err
	}
	defer wStorage.Close()
	for {
		record, err := chunk.Pop()
		if err != nil {
			break
		}
		err = wStorage.StoreLogRecord(record)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes the data file writer
func (f *Flusher) Close() error {
	f.wg.Wait()
	return nil
}

// NewFlusher creates a new Flusher
func NewFlusher(f ports.DataStorageWritableFactory) ports.Flushable {
	return &Flusher{wStorageFactory: f}
}
