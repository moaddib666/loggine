package ports

type TimeStampFilter interface {
	IsBefore(timestamp uint64) bool
	IsAfter(timestamp uint64) bool
}

type FilterSet interface {
	TimeStampFilter
}
