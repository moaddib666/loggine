package ports

import "LogDb/internal/domain"

type TimeStampFilter interface {
	IsBefore(timestamp uint64) bool
	IsAfter(timestamp uint64) bool
}

type FilterSet interface {
	TimeStampFilter
	IsMatch(record domain.LogRecord) bool
}
