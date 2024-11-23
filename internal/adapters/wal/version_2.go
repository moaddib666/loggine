package wal

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"sort"
	"sync"
	"time"
)

type V2WriterConfig struct {
	BaseDir       string
	MaxSize       int
	MaxRecords    int
	FlushInterval time.Duration
	MaxTimeToLive time.Duration
}

type DateHolder struct {
	header      *domain.DataFileHeader
	next        *DateHolder
	mu          sync.Mutex
	firstMinute *MinuteHolder
	lastMinute  *MinuteHolder
}

// IsEmpty returns true if the DateHolder has no MinuteHolder.
func (d *DateHolder) IsEmpty() bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.firstMinute == nil
}

// addMinuteHolder adds a new MinuteHolder to the DateHolder, maintaining order by header.Number DESC (newest first).
func (d *DateHolder) addMinuteHolder(header *domain.DataPageHeader) *MinuteHolder {
	mh := &MinuteHolder{
		header: header,
	}

	// Case 1: No MinuteHolder exists yet.
	if d.firstMinute == nil {
		d.firstMinute = mh
		d.lastMinute = mh
		return mh
	}

	// Case 2: Insert at the beginning if the new holder is the newest.
	if mh.header.Number > d.firstMinute.header.Number {
		mh.next = d.firstMinute
		d.firstMinute = mh
		return mh
	}

	// Case 3: Insert at the end if the new holder is the oldest.
	if mh.header.Number <= d.lastMinute.header.Number {
		d.lastMinute.next = mh
		d.lastMinute = mh
		return mh
	}

	// Case 4: Insert in the middle, maintaining descending order.
	prev := d.firstMinute
	for current := d.firstMinute.next; current != nil; current = current.next {
		if mh.header.Number > current.header.Number {
			prev.next = mh
			mh.next = current
			return mh
		}
		prev = current
	}

	return mh
}

// AddMinuteHolder adds a new MinuteHolder to the DateHolder.
func (d *DateHolder) AddMinuteHolder(header *domain.DataPageHeader) *MinuteHolder {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.addMinuteHolder(header)

}

// getMinuteHolder returns the MinuteHolder with the given number.
func (d *DateHolder) getMinuteHolder(number uint32) *MinuteHolder {
	d.mu.Lock()
	defer d.mu.Unlock()

	for m := d.firstMinute; m != nil; m = m.next {
		if m.header.Number == number {
			return m
		}
	}
	return nil
}

// GetMinuteHolder returns the MinuteHolder with the given number.
func (d *DateHolder) GetMinuteHolder(number uint32) *MinuteHolder {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.getMinuteHolder(number)
}

// getOrCreateMinuteHolder returns the MinuteHolder with the given number or creates a new one.
func (d *DateHolder) getOrCreateMinuteHolder(number uint32) *MinuteHolder {
	if mh := d.getMinuteHolder(number); mh != nil {
		return mh
	}
	return d.addMinuteHolder(domain.NewDataPageHeaderForMinute(number))
}

// GetOrCreateMinuteHolder returns the MinuteHolder with the given number or creates a new one.
func (d *DateHolder) GetOrCreateMinuteHolder(number uint32) *MinuteHolder {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.getOrCreateMinuteHolder(number)
}

// deleteMinuteHolder deletes the MinuteHolder with the given number.
func (d *DateHolder) deleteMinuteHolder(minuteNumber uint32) {
	// Case 1: No MinuteHolder exists.
	if d.firstMinute == nil {
		return
	}

	// Case 2: Delete the first MinuteHolder.
	if d.firstMinute.header.Number == minuteNumber {
		d.firstMinute = d.firstMinute.next
		if d.firstMinute == nil {
			// If the list is now empty, update lastMinute as well.
			d.lastMinute = nil
		}
		return
	}

	// Traverse the list to find the target MinuteHolder.
	prev := d.firstMinute
	for current := d.firstMinute.next; current != nil; current = current.next {
		if current.header.Number == minuteNumber {
			// Case 3: Delete the last MinuteHolder.
			if current == d.lastMinute {
				d.lastMinute = prev
			}
			// Case 4: Delete a middle MinuteHolder.
			prev.next = current.next
			return
		}
		prev = current
	}
}

// DeleteMinuteHolder deletes the MinuteHolder with the given number.
func (d *DateHolder) DeleteMinuteHolder(number uint32) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.deleteMinuteHolder(number)
}

type MinuteHolder struct {
	header    *domain.DataPageHeader
	next      *MinuteHolder
	logs      []*domain.LogRecord
	updatedAt time.Time
}

// GetLogs returns the logs in the MinuteHolder.
func (m *MinuteHolder) GetLogs() []*domain.LogRecord {
	m.Sort()
	return m.logs
}

// AppendLogRecord appends a log record to the MinuteHolder.
func (m *MinuteHolder) AppendLogRecord(record *domain.LogRecord) {
	m.logs = append(m.logs, record)
	m.updatedAt = time.Now()
	m.header.RecordCount++
}

// Sort sorts the logs in the MinuteHolder.
func (m *MinuteHolder) Sort() {
	sort.Slice(m.logs, func(i, j int) bool {
		return m.logs[i].Timestamp.Before(m.logs[j].Timestamp)
	})
}

type V2Writer struct {
	mu              sync.Mutex
	first           *DateHolder
	last            *DateHolder
	cfg             *V2WriterConfig
	closed          bool
	dataFileFactory ports.DataFileWriterFactory
}

// deleteDateHolder deletes the DateHolder with the given date.
func (h *V2Writer) deleteDateHolder(year, month, day uint64) {
	// Case 1: No DateHolder exists.
	if h.first == nil {
		return
	}

	// Case 2: Delete the first DateHolder.
	if h.first.header.Year == year && h.first.header.Month == month && h.first.header.Day == day {
		h.first = h.first.next
		if h.first == nil {
			// If the list is now empty, update last as well.
			h.last = nil
		}
		return
	}

	// Traverse the list to find the target DateHolder.
	prev := h.first
	for current := h.first.next; current != nil; current = current.next {
		if current.header.Year == year && current.header.Month == month && current.header.Day == day {
			// Case 3: Delete the last DateHolder.
			if current == h.last {
				h.last = prev
			}
			// Case 4: Delete a middle DateHolder.
			prev.next = current.next
			return
		}
		prev = current
	}
}

// DeleteDateHolder deletes the DateHolder with the given date.
func (h *V2Writer) DeleteDateHolder(year, month, day uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.deleteDateHolder(year, month, day)
}

// addDateHolder adds a new DateHolder to the V2Writer.
func (h *V2Writer) addDateHolder(header *domain.DataFileHeader) *DateHolder {
	dh := &DateHolder{
		header: header,
	}
	if h.first == nil {
		h.first = dh
		h.last = dh
	} else {
		h.last.next = dh
		h.last = dh
	}
	return dh
}

// AddDateHolder adds a new DateHolder to the V2Writer.
func (h *V2Writer) AddDateHolder(header *domain.DataFileHeader) *DateHolder {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.addDateHolder(header)
}

// getDateHolder returns the DateHolder with the given date.
func (h *V2Writer) getDateHolder(year, month, day uint64) *DateHolder {
	for d := h.first; d != nil; d = d.next {
		if d.header.Year == year && d.header.Month == month && d.header.Day == day {
			return d
		}
	}
	return nil
}

// GetDateHolder returns the DateHolder with the given date.
func (h *V2Writer) GetDateHolder(year, month, day uint64) *DateHolder {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.getDateHolder(year, month, day)
}

// getOrCreateDateHolder returns the DateHolder with the given date or creates a new one.
func (h *V2Writer) getOrCreateDateHolder(year, month, day uint64) *DateHolder {
	if dh := h.getDateHolder(year, month, day); dh != nil {
		return dh
	}
	return h.addDateHolder(domain.NewDataFileHeader(1, uuid.New().ID(), year, month, day))
}

// GetOrCreateDateHolder returns the DateHolder with the given date or creates a new one.
func (h *V2Writer) GetOrCreateDateHolder(year, month, day uint64) *DateHolder {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.getOrCreateDateHolder(year, month, day)
}

// StoreRecord appends a log record to the V2Writer.
func (h *V2Writer) StoreRecord(record *domain.LogRecord) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	dh := h.getOrCreateDateHolder(uint64(record.Timestamp.Year()), uint64(record.Timestamp.Month()), uint64(record.Timestamp.Day()))
	mh := dh.getOrCreateMinuteHolder(uint32(record.Timestamp.Hour()*60 + record.Timestamp.Minute()))
	mh.AppendLogRecord(record)
	return nil
}

// flush flushes the V2Writer accept force argument to force flush even if the time to live is not reached.
func (h *V2Writer) flush() {
	log.Debugf("Flushing V2Writer")
	// Iterate over all DateHolders
	for d := h.first; d != nil; d = d.next {
		log.Debugf("Flushing DateHolder: %d-%d-%d", d.header.Year, d.header.Month, d.header.Day)
		if d.IsEmpty() {
			log.Debugf("DateHolder is empty")
			continue
		}

		dataFileWriter, err := h.dataFileFactory.Create(d.header.Year, d.header.Month, d.header.Day)
		if err != nil {
			return
		}

		// The problem is that the minutes are DESC ordered, so we need to iterate from the last minute to the first but we don't have a pointer to the prev minute,
		// so we need to create an array of minutes and sort it in ASC order
		minutes := make([]*MinuteHolder, 0)
		for m := d.firstMinute; m != nil; m = m.next {
			if time.Since(m.updatedAt) < h.cfg.MaxTimeToLive && !h.closed {
				continue
			}
			minutes = append(minutes, m)
		}
		sort.Slice(minutes, func(i, j int) bool {
			return minutes[i].header.Number < minutes[j].header.Number
		})
		log.Debugf("Flushing %d MinuteHolders", len(minutes))

		//TODO: Here We Need To Create A New DataFile or find the last suitable for write and write Header to it i think DataFileWriter will be a good choice
		// Iterate over all MinuteHolders
		for _, m := range minutes {
			logs := m.GetLogs()
			err := dataFileWriter.CreateDataPage(m.header.Number)
			if err != nil {
				return
			}
			log.Debugf("Flushing %d logs from MinuteHolder: %d", len(logs), m.header.Number)
			for _, logRecord := range logs {
				err := dataFileWriter.WriteLogRecord(logRecord)
				if err != nil {
					return
				}
			}
			log.Debugf("Deleting MinuteHolder: %d", m.header.Number)
			d.deleteMinuteHolder(m.header.Number)
		}
		//TODO: Close the DataFile and flush it notifying IndexItem that new data is available
		if d.IsEmpty() {
			log.Debugf("DateHolder is empty, deleting it")
			h.deleteDateHolder(d.header.Year, d.header.Month, d.header.Day)
		}
	}
	log.Debugf("V2Writer flushed")
}

// Flush flushes the V2Writer.
func (h *V2Writer) Flush() {
	h.mu.Lock()
	defer h.mu.Unlock()
	log.Debugf("Periodic flush")
	h.flush()
	log.Debugf("Periodic flush done")
}

// Close closes the V2Writer.
func (h *V2Writer) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	log.Debugf("Closing V2Writer")
	h.closed = true
	h.flush()

}

// NewV2Writer creates a new V2Writer.
func NewV2Writer(cfg *V2WriterConfig, factory ports.DataFileWriterFactory) *V2Writer {
	wal := &V2Writer{
		cfg:             cfg,
		mu:              sync.Mutex{},
		dataFileFactory: factory,
	}
	go func() {
		for range time.Tick(cfg.FlushInterval) {
			wal.Flush()
		}
	}()
	return wal
}
