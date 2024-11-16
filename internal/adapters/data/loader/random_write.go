package loader

import (
	"LogDb/internal/domain"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"time"
)

// RandomLogsLoader generates random log records and calls the callback function.
func RandomLogsLoader(cb callback, limit int) {
	for i := 0; i < limit; i++ {
		// Generate a random log record
		record := constructLogRecord(randomTimestamp())
		err := cb(record)
		if err != nil {
			log.Fatalf("Error appending record: %v", err)
		}
		// Sleep for a random interval between records to simulate random log generation
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
}

// constructLogRecord creates a log record with a random message and labels.
func constructLogRecord(ts time.Time) *domain.LogRecord {
	return &domain.LogRecord{
		Timestamp:     ts,
		SchemaVersion: uint64(rand.Intn(10)), // Random schema version between 0 and 9
		Labels:        generateRandomLabels(),
		Message:       randomLogMessageGenerator(),
	}
}

// randomStringGenerator generates a random string of the specified length.
func randomStringGenerator(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// randomLogMessageGenerator generates a random log message of length 100 bytes.
func randomLogMessageGenerator() []byte {
	return []byte(randomStringGenerator(100))
}

// generateRandomLabels creates a slice of random labels for the log record.
func generateRandomLabels() []domain.Label {
	labelCount := rand.Intn(5) + 1 // Generate between 1 and 5 labels
	labels := make([]domain.Label, labelCount)
	for i := 0; i < labelCount; i++ {
		val := []byte(randomStringGenerator(10))
		labels[i] = domain.Label{
			Type:  domain.StringLabelType,
			Size:  uint64(len(val)),
			Value: val,
		}
	}
	return labels
}

// randomTimestamp generates a random timestamp within the past 24 hours.
func randomTimestamp() time.Time {
	now := time.Now()
	randomOffset := time.Duration(rand.Intn(24*60*60)) * time.Second
	return now.Add(-randomOffset)
}
