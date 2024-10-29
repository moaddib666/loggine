package filters

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
)

type AndFilter struct {
	filters []ports.Filter
}

// IsMatch returns true if all filters in the AndFilter match the record.
func (a *AndFilter) IsMatch(record *domain.LogRecord) bool {
	for _, filter := range a.filters {
		if !filter.IsMatch(record) {
			return false
		}
	}
	return true
}

// AddFilter adds a filter to the AndFilter.
func (a *AndFilter) AddFilter(filter ports.Filter) {
	a.filters = append(a.filters, filter)
}

// NewAnd creates a new AndFilter with the given filters.
func NewAnd(filters ...ports.Filter) *AndFilter {
	return &AndFilter{filters: filters}
}
