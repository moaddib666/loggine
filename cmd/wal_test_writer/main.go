package main

import (
	"LogDb/internal/adapters/data/loader"
	"LogDb/internal/adapters/serializer"
	"LogDb/internal/adapters/wal"
	"LogDb/internal/domain"
	"log"
	"time"
)

const ApacheLogsFile = "assets/logfiles.log"

func main() {
	codec := &serializer.BinarySerializer{}
	repo, err := wal.NewWALRepository(
		&wal.V1WriterConfig{
			BaseDir:       ".wal",
			MaxSize:       10 * 1024 * 1024,
			MaxRecords:    100_000,
			FlushInterval: 5 * time.Second,
		},
		codec,
		func(fileName string) {
			log.Printf("WAL file %s has been flushed", fileName)
		},
	)
	if err != nil {
		panic(err)
	}
	defer repo.Close()

	loader.LoadApacheLogsFromFile(ApacheLogsFile, func(record *domain.LogRecord) error {
		return repo.StoreRecord(record)
	}, 1)

}
