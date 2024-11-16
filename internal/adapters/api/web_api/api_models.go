package web_api

import (
	"time"
)

// StoreResult represents the result of an insert operation
type StoreResult struct {
	Success        bool   `json:"success"`
	RecordInserted int    `json:"record_inserted"`
	Error          string `json:"error,omitempty"`
}

type Record struct {
	Timestamp    time.Time         `json:"timestamp"`
	Message      string            `json:"message"`
	StringLabels map[string]string `json:"string_labels"`
}

// StoreRequest represents a request to search records
type StoreRequest struct {
	Record      *Record `json:"record"`
	ShardingKey string  `json:"sharding_key"`
}

// StoreBatchRequest represents a batch insert request
type StoreBatchRequest struct {
	Records     []*Record `json:"records"`
	ShardingKey string    `json:"sharding_key"`
}

// SearchRequest represents a request to search records
type SearchRequest struct {
	FromTime           time.Time `json:"from_time"`
	ToTime             time.Time `json:"to_time"`
	ShardingKey        string    `json:"sharding_key"`
	MessageMustContain string    `json:"message_contains"`
	Limit              int       `json:"limit"`
}

type SearchReport struct {
	TotalRecords int     `json:"total_records"`
	TimeTaken    float64 `json:"time_taken"`
}

type SearchResult struct {
	Records []*Record     `json:"records"`
	Report  *SearchReport `json:"report"`
}

// NewSearchResult creates a new instance of SearchResult
func NewSearchResult() *SearchResult {
	return &SearchResult{
		Records: make([]*Record, 0),
		Report:  &SearchReport{},
	}
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error"`
}
