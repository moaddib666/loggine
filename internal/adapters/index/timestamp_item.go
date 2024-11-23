package index

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"errors"
	"sync/atomic"
	"time"
)

type operation struct {
	done     bool
	callback func()
}

// Done marks the operation as done.
func (o *operation) Done() error {
	if o.done {
		return errors.New("operation already done")
	}
	o.done = true
	if o.callback != nil {
		o.callback()
	}
	return nil
}

// PrimaryIndexItem represents a data file index in memory storage.
type PrimaryIndexItem struct {
	header         *domain.DataFileHeader // Data file header
	readOperations atomic.Int64           // Number of read operations in progress
	writeLock      atomic.Bool            // False - unlocked, true - locked
}

// GetHeader returns the header of the index.
func (p *PrimaryIndexItem) GetHeader() *domain.DataFileHeader {
	return p.header
}

// RequestReadAccess allows read access if no write operation is in progress.
func (p *PrimaryIndexItem) RequestReadAccess() (ports.IndexOperation, error) {
	if p.writeLock.Load() {
		return nil, errors.New("write operation is in progress")
	}
	p.readOperations.Add(1)
	return &operation{
		callback: p.onReadOperationDone,
	}, nil
}

// onReadOperationDone decrements the number of read operations.
func (p *PrimaryIndexItem) onReadOperationDone() {
	currentReads := p.readOperations.Add(-1)
	if currentReads < 0 {
		panic("read operations count went negative, invalid state")
	}
}

// onWriteOperationDone releases the write lock.
func (p *PrimaryIndexItem) onWriteOperationDone() {
	p.writeLock.Store(false)
}

// RequestWriteAccess allows write access if no read or write operations are in progress.
func (p *PrimaryIndexItem) RequestWriteAccess() (ports.IndexOperation, error) {
	if p.readOperations.Load() > 0 {
		return nil, errors.New("read operations are in progress")
	}
	if !p.writeLock.CompareAndSwap(false, true) {
		return nil, errors.New("write operation is already in progress")
	}
	return &operation{
		callback: p.onWriteOperationDone,
	}, nil
}

// AwaitReadAccess waits until no write operation is in progress, then allows read access.
func (p *PrimaryIndexItem) AwaitReadAccess() (ports.IndexOperation, error) {
	for {
		if !p.writeLock.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond) // Short sleep for efficient backoff
	}
	p.readOperations.Add(1)
	return &operation{
		callback: p.onReadOperationDone,
	}, nil
}

// AwaitWriteAccess waits until no read or write operations are in progress, then allows write access.
func (p *PrimaryIndexItem) AwaitWriteAccess() (ports.IndexOperation, error) {
	for {
		if p.readOperations.Load() == 0 && p.writeLock.CompareAndSwap(false, true) {
			break
		}
		time.Sleep(10 * time.Millisecond) // Short sleep for efficient backoff
	}
	return &operation{
		callback: p.onWriteOperationDone,
	}, nil
}
