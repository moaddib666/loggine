package index

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"github.com/google/uuid"
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
	codec   ports.Serializer
	baseDir string
	index   map[string][]*domain.DataFileHeader // A map of dates to data files
	mu      sync.Mutex
	storage ports.DataStorage
}

func (t *Timestamp) GetDataFilesForRead(q ports.PreparedQuery) ([]*domain.DataFile, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	fromDateTime := time.Unix(int64(q.FromDateTime()), 0)
	toDateTime := time.Unix(int64(q.ToDateTime()), 0)

	var files []*domain.DataFile
	for _, dfs := range t.index {
		// TODO: optimise search for the date range
		for _, df := range dfs {
			// if the data file is not in the range of the query, skip it
			if df.Time().Before(fromDateTime) || df.Time().After(toDateTime) {
				continue
			}
			// This need to be check on exact datapage
			// Check last data page number - aka minute number
			//if df.LastDataPageNumber < uint32(q.From.Hour()*60+q.From.Minute()) {
			//	continue
			//}
			// Check first data page number - aka minute number
			// TODO: implement FirstDataPageNumber as not not in protocol
			//if df.FirstDataPageNumber > uint32(q.To.Hour()*60+q.To.Minute()) {
			//	continue
			//}
			fh, err := t.storage.GetDataFile(df.String())
			if err != nil {
				return nil, err
			}
			files = append(files, fh)
		}
	}
	return files, nil
}

func (t *Timestamp) GetDataFileForWrite(record *domain.LogRecord) (*domain.DataFile, bool, error) {
	var df *domain.DataFile
	var err error
	dfh, exist := t.GetDataFile(record.Timestamp)
	if !exist {
		dfh = &domain.DataFileHeader{
			Version: 1,
			Id:      uuid.New().ID(),
			Year:    uint64(record.Timestamp.Year()),
			Month:   uint64(record.Timestamp.Month()),
			Day:     uint64(record.Timestamp.Day()),
		}
		df, err = t.storage.CreateDataFile(dfh.String(), dfh.Id, dfh.Year, dfh.Month, dfh.Day)
	} else {
		df, err = t.storage.GetDataFile(dfh.String())
	}
	return df, !exist, err
}

// NewTimestamp creates a new Timestamp index.
func NewTimestamp(baseDir string, codec ports.Serializer) *Timestamp {
	return &Timestamp{
		baseDir: baseDir,
		codec:   codec,
		index:   make(map[string][]*domain.DataFileHeader),
	}
}

// BindStorage binds the index to a data storage.
func (t *Timestamp) BindStorage(storage ports.DataStorage) error {
	t.storage = storage
	return t.load()
}

// load loads the index from the data storage.
func (t *Timestamp) load() error {
	// Iterate over the data storage and load the index
	headers, err := t.storage.GetDataFilesHeaders()
	if err != nil {
		return err
	}
	for _, header := range headers {
		t.AddDataFile(header)
	}
	return nil
}

// GetDataFile - returns the DataFileHeader for the given date
func (t *Timestamp) GetDataFile(ts time.Time) (*domain.DataFileHeader, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	dataFiles, ok := t.index[ts.Format("2006-01-02")]
	minute := uint32(ts.Hour()*60 + ts.Minute())
	for _, df := range dataFiles {
		if df.LastDataPageNumber > minute {
			continue
		}
		if df.LastDataPageNumber <= minute {
			return df, ok
		}
	}
	return nil, false
}

// AddDataFile - adds a DataFileHeader to the index
func (t *Timestamp) AddDataFile(df *domain.DataFileHeader) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.index[df.Time().Format("2006-01-02")]; !ok {
		t.index[df.Time().Format("2006-01-02")] = make([]*domain.DataFileHeader, 0)
	}
	t.index[df.Time().Format("2006-01-02")] = append(t.index[df.Time().Format("2006-01-02")], df)
	return nil
}

// DeleteDataFile - deletes a DataFileHeader from the index
func (t *Timestamp) DeleteDataFile(df *domain.DataFileHeader) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.index[df.Time().Format("2006-01-02")]; !ok {
		return nil
	}
	for i, d := range t.index[df.Time().Format("2006-01-02")] {
		if d.Id == df.Id {
			t.index[df.Time().Format("2006-01-02")] = append(t.index[df.Time().Format("2006-01-02")][:i], t.index[df.Time().Format("2006-01-02")][i+1:]...)
			return nil
		}
	}
	return nil
}
