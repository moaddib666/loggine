package main

import (
	"LogDb/internal/adapters"
	"LogDb/internal/domain"
	"log"
	"time"
)

func main() {

	storage := adapters.NewSingleFileStorage("log.db")
	err := storage.AddLogRecord(domain.LogRecord{
		Timestamp: time.Now(),
		Labels:    []string{"INFO"},
		Message:   "Hello, World!",
	})
	if err != nil {
		log.Fatalf("Error adding log record: %v", err)
	}

	reader := storage.GetReader()
	for {
		record, _, err := reader.ScanLogRecord()
		if err != nil {
			log.Fatalf("Error scanning log record: %v", err)
		}
		log.Printf("Record: %+v\n", record)
	}
}
