package datastor

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"time"
)

type Cursor struct {
	minuteStart int64 // unix timestamp
	minuteEnd   int64 // unix timestamp
	dayStart    int64 // unix timestamp
	dayEnd      int64 // unix timestamp
}

// Next creates a cursor for the next minute
func (c *Cursor) Next() *Cursor {
	return &Cursor{
		minuteStart: c.minuteEnd,
		minuteEnd:   c.minuteEnd + 60,
	}
}

// New creates a new cursor for the given time
func (c *Cursor) New(t time.Time) *Cursor {
	t = t.Truncate(time.Minute)
	// minute start in unix timestamp
	unixMinute := t.Unix()
	// day start in unix timestamp
	t = t.Truncate(time.Hour * 24)
	unixDay := t.Unix()
	return &Cursor{
		minuteStart: unixMinute,
		minuteEnd:   unixMinute + 60, // 60 seconds in a minute
		dayStart:    unixDay,
		dayEnd:      unixDay + 1440*60, // 1440 minutes in a day (24 hours)
	}
}

// NewCurrent creates a new cursor for the current minute
func (c *Cursor) NewCurrent() *Cursor {
	return c.New(time.Now())
}

// NewCursor creates a new cursor for the given time
func NewCursor() *Cursor {
	return &Cursor{
		minuteStart: 0,
		minuteEnd:   60,
		dayStart:    0,
		dayEnd:      1440 * 60,
	}
}

// IsAfterDataPage checks if the cursor is after the given cursor
func (c *Cursor) IsAfterDataPage(t time.Time) bool {
	return c.minuteEnd < t.Unix()
}

// IsAfterDataFile checks if the cursor is after the given cursor
func (c *Cursor) IsAfterDataFile(t time.Time) bool {
	return c.dayEnd < t.Unix()
}

var _ ports.DataStorageWritable = &SequentialLogCollector{}

type SequentialLogCollector struct {
	dff    ports.DataFileWriterFactory
	dpf    ports.DataPageHeaderFactory
	dfw    ports.DataFileWriter
	cursor *Cursor
}

// Close closes the data file writer
func (s *SequentialLogCollector) Close() error {
	if s.dfw != nil {
		return s.dfw.Close()
	}
	return nil
}

func NewSequentialLogCollector(dff ports.DataFileWriterFactory, dpf ports.DataPageHeaderFactory) *SequentialLogCollector {
	return &SequentialLogCollector{
		dff:    dff,
		dpf:    dpf,
		cursor: NewCursor(),
	}
}

// StoreLogRecord accepts a log record and writes it to the data file
func (s *SequentialLogCollector) StoreLogRecord(record *domain.LogRecord) error {
	//FIXME: Ensure Timestamp is UTC
	record.Timestamp = record.Timestamp.UTC()
	if s.cursor.IsAfterDataFile(record.Timestamp) {
		if s.dfw != nil {
			if err := s.dfw.Close(); err != nil {
				return err
			}
		}
		if dfw, err := s.dff.Create(uint64(record.Timestamp.Year()), uint64(record.Timestamp.Month()), uint64(record.Timestamp.Day())); err != nil {
			return err
		} else {
			s.dfw = dfw
		}
		// FIXME: DRY
		pageHeader := s.dpf.FromLogRecord(record)
		if err := s.dfw.AppendDataPage(pageHeader); err != nil {
			return err
		}
		s.cursor = s.cursor.New(record.Timestamp)
	} else if s.cursor.IsAfterDataPage(record.Timestamp) {
		s.cursor = s.cursor.New(record.Timestamp)
		pageHeader := s.dpf.FromLogRecord(record)
		if err := s.dfw.AppendDataPage(pageHeader); err != nil {
			return err
		}
	}

	return s.dfw.AppendLogRecordToCurrentDataPage(record)
}
