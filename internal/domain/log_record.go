package domain

import (
	"time"
)

type LogChunk struct {
	Records []LogRecord
}

type LogRecord struct {
	Timestamp time.Time
	Labels    []string
	Message   string
}
