package index

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"errors"
	"sync/atomic"
	"time"
)

type operation struct {
	dfh         *domain.DataFileHeader
	df          *domain.DataFile
	constructor func(header *domain.DataFileHeader, path string) (*domain.DataFile, error)
	done        bool
	callback    func()
}

func (o *operation) GetDataFileHeader() *domain.DataFileHeader {
	return o.dfh
}

func (o *operation) GetDataFile(path string) (*domain.DataFile, error) {
	if o.done {
		return nil, errors.New("operation already done")
	}
	return o.constructor(o.dfh, path)
}

// newReadOperation creates a new read operation.
func newReadOperation(dfh *domain.DataFileHeader, callback func()) ports.IndexOperation {
	return &operation{
		dfh:         dfh,
		constructor: domain.NewReadOnlyDataFile,
		callback:    callback,
	}
}

// newWriteOperation creates a new write operation.
func newWriteOperation(dfh *domain.DataFileHeader, callback func()) ports.IndexOperation {
	return &operation{
		dfh:         dfh,
		constructor: domain.NewReadWriteDataFile,
		callback:    callback,
	}
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
	if o.df != nil {
		_ = o.df.Close()
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
	return newReadOperation(p.header, p.onReadOperationDone), nil
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
	return newWriteOperation(p.header, p.onWriteOperationDone), nil
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
	return newReadOperation(p.header, p.onReadOperationDone), nil
}

// AwaitWriteAccess waits until no read or write operations are in progress, then allows write access.
func (p *PrimaryIndexItem) AwaitWriteAccess() (ports.IndexOperation, error) {
	for {
		if p.readOperations.Load() == 0 && p.writeLock.CompareAndSwap(false, true) {
			break
		}
		time.Sleep(10 * time.Millisecond) // Short sleep for efficient backoff
	}
	return newWriteOperation(p.header, p.onWriteOperationDone), nil
}

// NewIndexItem creates a new primary index item.
func NewIndexItem(header *domain.DataFileHeader) ports.IndexItem {
	return &PrimaryIndexItem{
		header: header,
	}
}
