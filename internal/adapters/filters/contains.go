package filters

import (
	"LogDb/internal/domain"
	"bytes"
)

type ContainsFilter struct {
	value []byte
}

// IsMatch returns true if the ContainsFilter's value is contained in the record's message.
func (c *ContainsFilter) IsMatch(record *domain.LogRecord) bool {
	for _, b := range record.Labels {
		if bytes.Contains(b.Value, c.value) {
			return true
		}
	}
	return bytes.Contains(record.Message, c.value)
}

// NewContains creates a new ContainsFilter with the given value.
func NewContains(value []byte) *ContainsFilter {
	return &ContainsFilter{value: value}
}
