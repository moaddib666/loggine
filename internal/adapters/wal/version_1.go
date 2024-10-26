package wal

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"sync"
	"time"
)

const (
	maxWALSize    = 100 * 1024 * 1024 // 100 MB
	maxRecords    = 1_000_000         // Max number of records
	flushInterval = 5 * time.Second   // Default flush interval
	fileExt       = ".wal"            // Default WAL file path
	dirtyFileExt  = ".wal.dirty"      // Dirty WAL file path
)

// V1Callback is a callback function that is called when a WAL file is flushed.
type V1Callback func(fileName string)

type V1WriterConfig struct {
	BaseDir       string
	MaxSize       int
	MaxRecords    int
	FlushInterval time.Duration
	FileExt       string
	DirtyFileExt  string
}

// V1Writer implements the WALRepository interface.
type V1Writer struct {
	cfg             *V1WriterConfig
	header          *domain.WALHeader
	codec           ports.Serializer
	logRecords      []*domain.LogRecord // Buffer for log records
	mu              sync.Mutex          // Mutex to protect the WAL
	file            *os.File            // WAL file
	timer           *time.Timer         // Timer for automatic flushing
	walSize         int                 // Current size of the WAL
	recordCount     int                 // Current number of records
	onFlushCallback V1Callback          // Callback function
}

// discover scans the base directory for existing WAL files
func (w *V1Writer) discover() error {
	files, err := os.ReadDir(w.cfg.BaseDir)
	wals := make([]string, 0)
	dirtyWals := make([]string, 0)

	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if path.Ext(file.Name()) == w.cfg.DirtyFileExt {
			log.Printf("Dirty WAL file: %s", file.Name())
			dirtyWals = append(dirtyWals, file.Name())
			continue
		}
		if path.Ext(file.Name()) == w.cfg.FileExt {
			wals = append(wals, file.Name())
			continue
		}
	}
	if len(dirtyWals) > 0 {
		return errors.New("found dirty WAL files, please recover them first")
	}
	for _, wal := range wals {
		w.onFlushCallback(wal)
	}
	return nil
}

// reset
func (w *V1Writer) reset() {
	w.logRecords = w.logRecords[:0]
	w.walSize = 0
	w.recordCount = 0
	w.timer.Reset(w.cfg.FlushInterval)
}

// new
func (w *V1Writer) new() error {
	var err error
	w.header = domain.NewWALHeader()
	w.file, err = os.OpenFile(w.dirtyFileName(), os.O_CREATE|os.O_RDWR, 0600)
	return err
}

// markDone
func (w *V1Writer) markDone() error {
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	if err := os.Rename(w.dirtyFileName(), w.fileName()); err != nil {
		return err
	}
	return nil
}

// fileName
func (w *V1Writer) fileName() string {
	return path.Join(w.cfg.BaseDir, fmt.Sprintf("%d%s", w.header.CreatedAt, w.cfg.FileExt))
}

// dirtyFileName
func (w *V1Writer) dirtyFileName() string {
	return path.Join(w.cfg.BaseDir, fmt.Sprintf("%d%s", w.header.CreatedAt, w.cfg.DirtyFileExt))
}

// NewWALRepository creates a new WAL datafile with the given WAL file.
func NewWALRepository(cfg *V1WriterConfig, codec ports.Serializer, callback V1Callback) (*V1Writer, error) {

	if cfg == nil {
		cfg = &V1WriterConfig{}
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = maxWALSize
	}
	if cfg.MaxRecords == 0 {
		cfg.MaxRecords = maxRecords
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = flushInterval
	}
	if cfg.FileExt == "" {
		cfg.FileExt = fileExt
	}
	if cfg.DirtyFileExt == "" {
		cfg.DirtyFileExt = dirtyFileExt
	}
	// create the base directory if it doesn't exist

	repo := &V1Writer{
		cfg:             cfg,
		walSize:         0,
		recordCount:     0,
		timer:           time.NewTimer(cfg.FlushInterval),
		logRecords:      make([]*domain.LogRecord, 0),
		onFlushCallback: callback,
		codec:           codec,
	}
	if _, err := os.Stat(cfg.BaseDir); os.IsNotExist(err) {
		if err := os.MkdirAll(cfg.BaseDir, 0700); err != nil {
			return nil, err
		}
	}
	err := repo.discover()
	if err != nil {
		return nil, err
	}
	repo.new()

	// Start the flush timer
	go repo.flushTimer()

	return repo, nil
}

// StoreRecord stores a new log record in the WAL.
func (w *V1Writer) StoreRecord(r *domain.LogRecord) error {
	// Serialize the log record (you may want to use your BinarySerializer here).
	recordSize := len(r.Message) // Simplified; actual size will depend on full serialization
	w.logRecords = append(w.logRecords, r)
	w.walSize += recordSize
	w.recordCount++

	// Check if we need to flush due to size or record count.
	if w.walSize >= w.cfg.MaxSize || w.recordCount >= w.cfg.MaxRecords {
		return w.Flush()
	}
	return nil
}

// Flush writes the log records to the WAL file and resets the buffer.
func (w *V1Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.logRecords) == 0 {
		return nil // Nothing to flush
	}
	w.codec.WriteWALHeader(w.header, w.file)
	// Sort By Timestamp oldest to newest
	sort.Slice(w.logRecords, func(i, j int) bool {
		return w.logRecords[i].Timestamp.Before(w.logRecords[j].Timestamp)
	})
	// Serialize and write log records to file
	for _, record := range w.logRecords {
		if _, err := w.codec.WriteLogRecord(record, w.file); err != nil {
			return err
		}
	}

	w.markDone()
	w.onFlushCallback(w.fileName())
	w.reset()
	return w.new()
}

// flushTimer triggers automatic flushing based on a timer.
func (w *V1Writer) flushTimer() {
	for {
		select {
		case <-w.timer.C:
			if err := w.Flush(); err != nil {
				log.Fatalf("Error flushing WAL: %v", err)
			}
			w.timer.Reset(w.cfg.FlushInterval)
		}
	}
}

// Close closes the WAL file and processes the contents.
func (w *V1Writer) Close() error {
	// Flush remaining data before closing
	if err := w.Flush(); err != nil {
		return err
	}
	if w.walSize == 0 {
		// Remove the empty dirty file if it exists
		if err := os.Remove(w.dirtyFileName()); err != nil {
			return err
		}
	}
	return nil
}

type V1Processor struct {
	dataFile *domain.DataFileHeader
	dataPage *domain.DataPageHeader

	walCursor      uint64
	dataFileCursor uint64

	walFileHandler  *os.File
	dataFileHandler *os.File
}
