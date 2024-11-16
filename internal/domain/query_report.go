package domain

import (
	"github.com/google/uuid"
	"time"
)

type QueryReport struct {
	Id           uuid.UUID     `json:"id"`
	ScannedItems int           `json:"scanned_items"`
	Miss         int           `json:"miss"`
	Hits         int           `json:"hits"`
	ElapsedTime  time.Duration `json:"elapsed_time"`
}

// NewQueryReport creates a new query_types report with the given ID and count.
func NewQueryReport(count, hits int, startTime time.Time) QueryReport {
	return QueryReport{
		Id:           uuid.New(),
		ScannedItems: count,
		Hits:         hits,
		ElapsedTime:  time.Since(startTime),
	}
}
