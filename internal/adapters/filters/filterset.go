package filters

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
)

type FilterSet struct {
	filter          ports.Filter
	timeStampFilter ports.TimeStampFilter
}

func (c *FilterSet) IsBefore(timestamp uint64) bool {
	return c.timeStampFilter.IsBefore(timestamp)
}

func (c *FilterSet) IsAfter(timestamp uint64) bool {
	return c.timeStampFilter.IsAfter(timestamp)
}

func (c *FilterSet) IsMatch(record *domain.LogRecord) bool {
	return c.filter.IsMatch(record)
}
