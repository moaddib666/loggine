package log_transformers

import (
	"LogDb/internal/adapters/transformers"
	"LogDb/internal/domain"
	"fmt"
	"strings"
	"time"
)

const apacheCombinedLogTimeFormat = "27/Dec/2037:12:00:00 +0000"

// ApacheLogTransformer implements the LogTransformer interface for Apache logs
type ApacheLogTransformer struct {
}

// ToString converts a LogRecord to an Apache combined log string format
func (t *ApacheLogTransformer) ToString(record *domain.LogRecord) string {
	timestampStr := record.Timestamp.Format(apacheCombinedLogTimeFormat)
	return fmt.Sprintf("%s - - [%s] %s", record.Labels, timestampStr, record.Message)
}

// FromString converts an Apache combined log string to a LogRecord
func (t *ApacheLogTransformer) FromString(str string) *domain.LogRecord {
	record := domain.NewEmptyLogRecord()

	// Split the log entry into parts using spaces, keeping quoted parts together
	parts := strings.Split(str, " ")

	// Extract the IP address (labels)
	record.AddLabel(transformers.StringToLabel(parts[0]))

	// Extract the timestamp (remove square brackets and parse)
	timestampStr := strings.Trim(parts[3], "[]")
	parsedTime, err := time.Parse(apacheCombinedLogTimeFormat, timestampStr)
	if err != nil {
		parsedTime = time.Now()
	}
	record.Timestamp = parsedTime

	// Extract the HTTP method, request path, and protocol from the quoted part
	requestParts := strings.Split(strings.Trim(parts[5], "\""), " ")
	if len(requestParts) >= 2 {
		httpMethod := requestParts[0]
		requestPath := requestParts[1]
		record.AddLabel(transformers.StringToLabel(httpMethod))
		record.AddLabel(transformers.StringToLabel(requestPath))
	}
	record.Message = []byte(str) // Store the entire log entry in the message

	// The remaining part is the status code, response size, referrer, user agent, and response time
	// These parts can be added to labels or further extended into LogRecord fields

	return record
}

// FromBytes converts a byte array to a LogRecord (assuming the byte array is a string)
func (t *ApacheLogTransformer) FromBytes(b []byte) *domain.LogRecord {
	str := string(b)
	return t.FromString(str)
}

// NewApacheLogTransformer creates a new ApacheLogTransformer
func NewApacheLogTransformer() *ApacheLogTransformer {
	return &ApacheLogTransformer{}
}
