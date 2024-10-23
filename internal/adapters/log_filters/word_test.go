package log_filters_test

import (
	"LogDb/internal/adapters/log_filters"
	"LogDb/internal/domain"
	"testing"
	"time"
)

var logRecord = domain.LogRecord{
	Timestamp: time.Now(),
	Labels:    []byte("INFO"),
	Message:   []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."),
}

// Strings 6201060	       208.0 ns/op
// Bytes   7169828	       175.6 ns/op
var filterWord = "proident"

func BenchmarkWordFilter_FilterLogRecord(b *testing.B) {
	// Create a WordFilter that filters on the word "keyword"
	wordFilter := log_filters.NewWordFilter(filterWord)

	// Reset the timer before running the benchmark loop
	b.ResetTimer()

	// Run the benchmark loop
	for i := 0; i < b.N; i++ {
		wordFilter.FilterLogRecord(logRecord)
	}
}
