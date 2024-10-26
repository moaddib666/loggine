package main

import (
	"LogDb/internal/adapters/data/loader"
	"LogDb/internal/adapters/datastor"
	"LogDb/internal/adapters/index"
	"LogDb/internal/adapters/serializer"
	"LogDb/internal/domain"
	"fmt"
	"runtime"
	"time"
)

func MemoryUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func init() {
	go func() {
		for {
			MemoryUsage()
			time.Sleep(1 * time.Second)
		}
	}()
}

const BaseDir = ".storage"
const ApacheLogsFile = "assets/logfiles.log"

func main() {
	codec := serializer.Default
	idx := index.NewTimestamp(BaseDir, codec)
	stor := datastor.NewPersistentStorage(BaseDir, codec, idx)
	defer stor.Close()
	writeTimeLimitRecords(5*time.Minute, 1*time.Second, 10_000, stor)
}

func writeLimitRecords(n int, storage *datastor.PersistentStorage) {
	loader.LoadApacheLogsFromFile(ApacheLogsFile, func(record *domain.LogRecord) error {
		return storage.StoreLogRecord(record)
	}, n)
}

func writeTimeLimitRecords(timeWindow, interval time.Duration, chunkSize int, storage *datastor.PersistentStorage) {
	stopAt := time.Now().Add(timeWindow)
	for {
		if time.Now().After(stopAt) {
			break
		}
		loader.LoadApacheLogsFromFile(ApacheLogsFile, func(record *domain.LogRecord) error {
			return storage.StoreLogRecord(record)
		}, chunkSize)
		time.Sleep(interval)

	}
}
