package datastor

import (
	"io"
	"os"
	"sync"
	"time"
)

// TODO: make this more optimized make hot-cached file
// This file may have the same structure as data set, but we need to add the mapping to the minutes in header
// size(int64) * (minutes in a day) = 8 * 1440 bytes = 11520 bytes = 11.25 KB

// tempFileReader is a custom reader that wraps an *os.File and adds TTL functionality
type tempFileReader struct {
	file         *os.File
	ttl          time.Duration
	lastAccessed time.Time
	mu           sync.Mutex
	closed       bool
	closeCh      chan struct{}
}

// newTempFileReader creates a new tempFileReader with the specified TTL
func newTempFileReader(file *os.File, ttl time.Duration) *tempFileReader {
	tfr := &tempFileReader{
		file:         file,
		ttl:          ttl,
		lastAccessed: time.Now(),
		closeCh:      make(chan struct{}),
	}

	// Start the TTL monitoring goroutine
	go tfr.monitorTTL()

	return tfr
}

// monitorTTL periodically checks if the file should be closed due to inactivity
func (tfr *tempFileReader) monitorTTL() {
	ticker := time.NewTicker(time.Second) // Check every second
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tfr.mu.Lock()
			if tfr.closed {
				tfr.mu.Unlock()
				return
			}
			elapsed := time.Since(tfr.lastAccessed)
			if elapsed > tfr.ttl {
				tfr.closeUnlocked()
				tfr.mu.Unlock()
				return
			}
			tfr.mu.Unlock()
		case <-tfr.closeCh:
			return
		}
	}
}

// Read reads data from the file and updates the lastAccessed time
func (tfr *tempFileReader) Read(p []byte) (int, error) {
	tfr.mu.Lock()
	if tfr.closed {
		tfr.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	tfr.lastAccessed = time.Now()
	tfr.mu.Unlock()
	return tfr.file.Read(p)
}

// Seek seeks to a position in the file and updates the lastAccessed time
func (tfr *tempFileReader) Seek(offset int64, whence int) (int64, error) {
	tfr.mu.Lock()
	if tfr.closed {
		tfr.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	tfr.lastAccessed = time.Now()
	tfr.mu.Unlock()
	return tfr.file.Seek(offset, whence)
}

// Close closes the file and stops the TTL monitoring goroutine
func (tfr *tempFileReader) Close() error {
	tfr.mu.Lock()
	if tfr.closed {
		tfr.mu.Unlock()
		return io.ErrClosedPipe
	}
	tfr.closeUnlocked()
	tfr.mu.Unlock()
	return nil
}

// closeUnlocked closes the file without acquiring the mutex (expects mutex to be locked)
func (tfr *tempFileReader) closeUnlocked() {
	tfr.closed = true
	close(tfr.closeCh)
	tfr.file.Close()
	os.Remove(tfr.file.Name())
}
