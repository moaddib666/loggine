package domain

import (
	"time"
)

type LogChunk struct {
	Records []LogRecord
}

type LogRecord struct {
	Timestamp     time.Time `json:"timestamp"`
	SchemaVersion uint64    `json:"schema_version"`
	Labels        []Label   `json:"labels"`
	Message       []byte    `json:"message"`
}

// NewEmptyLogRecord creates a new LogRecord with the current time
func NewEmptyLogRecord() *LogRecord {
	return &LogRecord{Timestamp: time.Now().UTC(), SchemaVersion: 1, Labels: []Label{}, Message: []byte{}}
}

// AddLabel a new label to the record
func (r *LogRecord) AddLabel(label Label) {
	r.Labels = append(r.Labels, label)
}

// DataPageNumber returns the number of data pages needed to store the record
func (r *LogRecord) DataPageNumber() uint32 {
	return uint32(r.Timestamp.Hour()*60 + r.Timestamp.Minute())
}
