package filters

import (
	"LogDb/internal/domain"
)

// NotSetFilter is a filter that always returns true.
type notSetFilter struct{}

func (n *notSetFilter) IsMatch(_ *domain.LogRecord) bool {
	return true
}

// NotSet creates a new NotSetFilter.
var NotSet = &notSetFilter{}
