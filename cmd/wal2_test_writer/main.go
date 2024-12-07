package main

import (
	"LogDb/internal/adapters/compression"
	"LogDb/internal/adapters/data/loader"
	"LogDb/internal/adapters/datastor"
	"LogDb/internal/adapters/serializer"
	"LogDb/internal/adapters/wal"
	"LogDb/internal/domain"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

func init() {
	// Set up the logger
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
}
func main() {
	codec := serializer.Default
	dataFileFactory := datastor.NewDataFileWriterFactory(".wal", codec, compression.Factory, log.NewEntry(log.New()))

	repo := wal.NewV2Writer(&wal.V2WriterConfig{
		BaseDir:       ".wal",
		MaxSize:       10 * 1024 * 1024,
		MaxRecords:    100_000,
		FlushInterval: 5 * time.Second,
		MaxTimeToLive: 10 * time.Second,
	}, dataFileFactory)
	loader.LimitLogsLoader(time.Now(), time.Now().Add(1*time.Hour), func(record *domain.LogRecord) error {
		return repo.StoreRecord(record)
	}, 100_000, 100)
	time.Sleep(20 * time.Second)
	repo.Close()
}
