package index

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sort"
	"sync"
	"time"
)

// Timestamp represents a primary index that is based on timestamps.
// the baseDir is the directory where the DataFiles are stored in format YYYY-MM-DD.00000000000.chunk
// each chunk contains a DataPages 60 * 24 each for a minute of the day
// each DataPage contains a []LogRecord
// Search algorithm is find the DataFile for the day, then find the DataPage for the minute, then search the LogRecord
// Example: we have a timestamp 2021-01-01 12:30:00 -> search in local index for 2021-01-01 -> if we don't have it create NewDataFile and add it to index if we already have it undertand last written DataPage/Minute Written in Datafile if it's  bigger then current log record then create new datafile if it's match current log recor use this data page if it's before current log record then search in the data page for the log record create new DataPage
// Use b-tree to build and store the index where leaf node are
// Timestamp is a simplified in-memory index that stores log records by day (YYYY-MM-DD).
type Timestamp struct {
	index          map[string][]ports.IndexItem // A map of dates to data files
	mu             sync.Mutex
	storage        ports.DataStorage
	repo           ports.DataFileRepository
	merger         ports.Merger
	dataCompressor ports.DataCompressor
}

func (t *Timestamp) GetDataFilesForRead(q ports.PreparedQuery) ([]ports.IndexOperation, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	fromDateTime := time.Unix(int64(q.FromDateTime()), 0)
	toDateTime := time.Unix(int64(q.ToDateTime()), 0)

	var items []ports.IndexOperation
	for _, idxItems := range t.index {
		// TODO: optimise search for the date range
		for _, idxItem := range idxItems {
			// if the data file is not in the range of the query, skip it
			dfHeader := idxItem.GetHeader()
			if dfHeader.Time().Before(fromDateTime) || dfHeader.Time().After(toDateTime) {
				continue
			}
			// This need to be check on exact datapage
			// Check last data page number - aka minute number
			//if dfHeader.LastDataPageNumber < uint32(q.From.Hour()*60+q.From.Minute()) {
			//	continue
			//}
			// Check first data page number - aka minute number
			// TODO: implement FirstDataPageNumber as not not in protocol
			//if dfHeader.FirstDataPageNumber > uint32(q.To.Hour()*60+q.To.Minute()) {
			//	continue
			//}
			// Request read access to the data file
			op, err := idxItem.RequestReadAccess()
			if err != nil {
				log.WithError(err).Errorf("Failed to request read access to data file %s", dfHeader)
			}
			items = append(items, op)
		}
	}
	return items, nil
}

// NewTimestamp creates a new Timestamp index.
func NewTimestamp(repo ports.DataFileRepository, merger ports.Merger, dataCompressor ports.DataCompressor) *Timestamp {
	return &Timestamp{
		repo:           repo,
		merger:         merger,
		index:          make(map[string][]ports.IndexItem),
		dataCompressor: dataCompressor,
	}
}

// BindStorage binds the index to a data storage.
func (t *Timestamp) BindStorage(storage ports.DataStorage) error {
	t.storage = storage
	return t.load()
}

// load loads the index from the data storage.
func (t *Timestamp) load() error {
	// Discover data files via glob pattern
	files, err := t.repo.ListAvailable()

	if err != nil {
		return err
	}
	// Iterate over each discovered file
	for _, dataFileHeader := range files {
		if err := t.AddDataFile(dataFileHeader); err != nil {
			return fmt.Errorf("failed to add data file header to index for file %s: %w", dataFileHeader, err)
		}
	}

	return nil
}

// addDataFile - adds a DataFileHeader to the index
func (t *Timestamp) addDataFile(header *domain.DataFileHeader) (ports.IndexItem, error) {
	if _, ok := t.index[header.Time().Format("2006-01-02")]; !ok {
		t.index[header.Time().Format("2006-01-02")] = make([]ports.IndexItem, 0)
	}
	item := NewIndexItem(header)
	t.index[header.Time().Format("2006-01-02")] = append(t.index[header.Time().Format("2006-01-02")], item)
	return item, nil
}

// AddDataFile - adds a DataFileHeader to the index
func (t *Timestamp) AddDataFile(header *domain.DataFileHeader) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, err := t.addDataFile(header)
	if err != nil {
		return err
	}
	return t.merge(header.Time().Format("2006-01-02"))
}

// merge merges the given index with the current index.
func (t *Timestamp) merge(key string) error {
	items, ok := t.index[key]
	if !ok {
		return nil
	}
	if len(items) < 2 {
		return nil
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].GetHeader().FirstDataPageNumber < items[j].GetHeader().FirstDataPageNumber
	})
	var target = items[0]
	// Merge the data files into a single data file
	for i := 1; i < len(items); i++ {
		var err error
		target, err = t.mergeSequentially(target, items[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// mergeSequentially merges two data files sequentially.
func (t *Timestamp) mergeSequentially(target, source ports.IndexItem) (ports.IndexItem, error) {
	targetOp, err := target.AwaitWriteAccess()
	if err != nil {
		return nil, err
	}
	defer targetOp.Done()

	sourceOp, err := source.AwaitWriteAccess()
	if err != nil {
		return nil, err
	}
	defer sourceOp.Done()

	// Merge the data files
	targetDataFile, err := targetOp.GetDataFile(t.repo.GetDataFileFullPath(target.GetHeader().String()))
	if err != nil {
		return nil, err
	}

	sourceDataFile, err := sourceOp.GetDataFile(t.repo.GetDataFileFullPath(source.GetHeader().String()))
	if err != nil {
		return nil, err
	}

	mergedDataFile, err := t.merger.MergeDataFiles(targetDataFile, sourceDataFile)

	if err != nil {
		return nil, err
	}
	// Add a new data file to the index
	// Remove the source data file from the index
	// Remove the target data file from the index
	err = t.deleteDataFile(targetDataFile.Header)
	if err != nil {
		return nil, err
	}
	err = t.deleteDataFile(sourceDataFile.Header)
	if err != nil {
		return nil, err
	}
	return t.addDataFile(mergedDataFile.Header)
}

// deleteDataFile - deletes a DataFileHeader from the index
func (t *Timestamp) deleteDataFile(df *domain.DataFileHeader) error {
	if _, ok := t.index[df.Time().Format("2006-01-02")]; !ok {
		return nil
	}
	for i, idxItem := range t.index[df.Time().Format("2006-01-02")] {
		if idxItem.GetHeader().Id == df.Id {
			//op, err := idxItem.AwaitWriteAccess()
			//defer op.Done()
			//if err != nil {
			//	return err
			//}
			//t.mu.Lock()
			t.index[df.Time().Format("2006-01-02")] = append(t.index[df.Time().Format("2006-01-02")][:i], t.index[df.Time().Format("2006-01-02")][i+1:]...)
			//t.mu.Unlock()
			return t.repo.DeleteByHeader(df)
		}
	}
	return nil
}

// Compress compresses the data files in the index.
func (t *Timestamp) Compress() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var dates []string
	for date := range t.index {
		dates = append(dates, date)
	}
	sort.Strings(dates) // Sort dates in ascending order

	// Compress all except the newest date as merging is approaching
	for _, date := range dates[:len(dates)-1] { // Exclude the last (newest) date
		idxItems := t.index[date]

		for i := 0; i < len(idxItems); i++ {
			idxItem := idxItems[i]
			dfh := idxItem.GetHeader()
			if dfh.Compressed {
				continue
			}
			if err := t.compressDataFile(idxItem); err != nil {
				return err
			}
		}
	}
	return nil
}

// compressDataFile compresses a data file
func (t *Timestamp) compressDataFile(idxItem ports.IndexItem) error {
	op, err := idxItem.AwaitWriteAccess()
	if err != nil {
		return err
	}
	defer op.Done()
	df, err := op.GetDataFile(t.repo.GetDataFileFullPath(idxItem.GetHeader().String()))
	if err != nil {
		return err
	}
	if _, err := t.dataCompressor.CompressDataFile(df); err != nil {
		return err
	}
	return nil
}
