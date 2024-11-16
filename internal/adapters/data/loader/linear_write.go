package loader

import (
	log "github.com/sirupsen/logrus"
	"time"
)

// LimitLogsLoader generates log records within a specific time frame.
func LimitLogsLoader(dateStart time.Time, dateEnd time.Time, cb callback, limit int, chunkSize int) {
	// Calculate the total duration
	totalDuration := dateEnd.Sub(dateStart)
	if totalDuration <= 0 {
		log.Fatalf("Invalid date range: start time must be before end time")
	}

	// Generate logs within the specified time frame
	recordsGenerated := 0
	currentTime := dateStart

	for recordsGenerated < limit {
		for i := 0; i < chunkSize && recordsGenerated < limit; i++ {
			// Generate a random timestamp within the current minute
			randomOffset := calculateLinearTimeOffset(recordsGenerated)
			timestamp := currentTime.Add(randomOffset)

			// Construct and append the log record
			record := constructLogRecord(timestamp)
			err := cb(record)
			if err != nil {
				log.Fatalf("Error appending record: %v", err)
			}
			recordsGenerated++
		}
		// Move to the next minute in the sequence
		currentTime = currentTime.Add(time.Minute)
		if currentTime.After(dateEnd) {
			break
		}
	}
}

// calculateLinearTimeOffset - instead of randomOffset := time.Duration(rand.Intn(60)) * time.Second we need to calculatae the time offset in entire minute so that logs been sorted by time
func calculateLinearTimeOffset(recordsGenerated int) time.Duration {
	// Must not exceed 60 seconds
	return time.Duration(recordsGenerated) * time.Second % 60 * time.Second
}

// LinearLogWriter accepts date start and date end and write logs ech minute from date start to date end
func LinearLogWriter(dateStart time.Time, dateEnd time.Time, cb callback) {
	// Switch Time TO UTC
	dateStart = dateStart.UTC()
	dateEnd = dateEnd.UTC()
	// Calculate the total duration
	totalDuration := dateEnd.Sub(dateStart)
	if totalDuration <= 0 {
		log.Fatalf("Invalid date range: start time must be before end time")
	}

	// Generate logs within the specified time frame
	currentTime := dateStart

	for currentTime.Before(dateEnd) {
		// Construct and append the log record
		record := constructLogRecord(currentTime)
		err := cb(record)
		if err != nil {
			log.Fatalf("Error appending record: %v", err)
		}
		// Move to the next minute in the sequence
		currentTime = currentTime.Add(time.Minute)
	}
}
