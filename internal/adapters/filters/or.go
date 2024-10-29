package filters

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
)

type OrFilter struct {
	filters []ports.Filter
}

// IsMatch returns true if any filter in the OrFilter matches the record.
func (o *OrFilter) IsMatch(record *domain.LogRecord) bool {
	for _, filter := range o.filters {
		if filter.IsMatch(record) {
			return true
		}
	}
	return false
}

// AddFilter adds a filter to the OrFilter.
func (o *OrFilter) AddFilter(filter ports.Filter) {
	o.filters = append(o.filters, filter)
}

// NewOr creates a new OrFilter with the given filters.
func NewOr(filters ...ports.Filter) *OrFilter {
	return &OrFilter{filters: filters}
}
