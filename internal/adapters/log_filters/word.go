package log_filters

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"bytes"
)

var _ ports.Filter = &WordFilter{}

// WordFilter filters LogRecords that contain a specific word in the message
type WordFilter struct {
	Word []byte
}

// FilterLogRecord checks if the word is present in the LogRecord's message
func (wf *WordFilter) FilterLogRecord(record domain.LogRecord) bool {
	return bytes.Contains(record.Message, wf.Word)
}

// NewWordFilter creates a new WordFilter
func NewWordFilter(word string) *WordFilter {
	return &WordFilter{Word: []byte(word)}
}
