package filters

import (
	"time"
)

// TimeRangeFilter represents a filter that filters log records by time range.
type TimeRangeFilter struct {
	startTime uint64
	endTime   uint64
}

func (t *TimeRangeFilter) FilterByTimeStamp(recordTimestamp uint64) bool {
	return recordTimestamp >= t.startTime && recordTimestamp <= t.endTime
}

// IsBefore returns true if the given timestamp is before the start time of the filter.
func (t *TimeRangeFilter) IsBefore(timestamp uint64) bool {
	return timestamp < t.startTime
}

// IsAfter returns true if the given timestamp is after the end time of the filter.
func (t *TimeRangeFilter) IsAfter(timestamp uint64) bool {
	return timestamp > t.endTime
}

// NewTimeRangeFilter creates a new TimeRangeFilter with the given start and end times.
func NewTimeRangeFilter(startTime uint64, endTime uint64) *TimeRangeFilter {
	return &TimeRangeFilter{startTime: startTime, endTime: endTime}
}

// NewDateRangeFilter creates a new TimeRangeFilter with the given start and end times.
func NewDateRangeFilter(startTime time.Time, endTime time.Time) *TimeRangeFilter {
	return &TimeRangeFilter{startTime: uint64(startTime.Unix()), endTime: uint64(endTime.Unix())}
}

// allTimeFilter is a filter that always returns true.
type allTimeFilter struct{}

func (a allTimeFilter) IsBefore(timestamp uint64) bool {
	return true
}

func (a allTimeFilter) IsAfter(timestamp uint64) bool {
	return true
}

var AllTimeRange = &allTimeFilter{}
