package filters

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
)

type NotFilter struct {
	filter ports.Filter
}

// IsMatch returns true if the NotFilter's filter does not match the record.
func (n *NotFilter) IsMatch(record *domain.LogRecord) bool {
	return !n.filter.IsMatch(record)
}

// NewNot creates a new NotFilter with the given filter.
func NewNot(filter ports.Filter) *NotFilter {
	return &NotFilter{filter: filter}
}
