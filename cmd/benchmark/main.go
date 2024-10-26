package main

import (
	"LogDb/internal/adapters"
	"LogDb/internal/adapters/filters"
	"LogDb/internal/adapters/log_transformers"
	"LogDb/internal/adapters/presenters"
	"LogDb/internal/ports"
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
)

const ApacheLogsFile = "assets/logfiles.log"

func main() {
	var filterSet ports.FilterSet
	storage := adapters.NewSingleFileStorage("log.db")
	presenter := presenters.NewStringPresenter()
	// 2024-10-24 19:57:04 - 2024-10-24 19:57:04
	// startTime = {uint64} 1729789020
	// endTime = {uint64} 1729789025
	// recordTimestamp = {uint64} 1729788624
	startTime := time.Date(2024, 10, 24, 21, 13, 15, 0, time.Local)
	endTime := time.Date(2024, 10, 24, 21, 13, 15, 0, time.Local)
	filterSet = filters.NewDateRangeFilter(startTime, endTime)
	//filterSet = filters.NewDisableTimeRangeFilter()
	//loadApacheLogsFromFile(ApacheLogsFile, storage, 10_000)
	//loadApacheLogsFromFile(ApacheLogsFile, storage, 0)
	defer storage.Flush()

	//filter := log_filters.NewWordFilter("149.37.193.161")
	result, err := adapters.Query(storage, filterSet)
	if err != nil {
		log.Fatalf("Error querying storage: %v", err)
	}
	for _, record := range result.Records {
		fmt.Print(presenter.Present(record))
	}
	fmt.Printf("Report %s scanned %d records %d hits in %v seconds\n", result.Report.Id, result.Report.Count, result.Report.Hits, result.Report.ElapsedTime.Seconds())
}

func loadApacheLogsFromFile(fileName string, storage *adapters.SingleFileStorage, limit int) {

	fh, error := os.Open(fileName)
	if error != nil {
		log.Fatalf("Error opening file: %v", error)
	}
	defer fh.Close()

	transformer := log_transformers.NewApacheLogTransformer()
	scanner := bufio.NewScanner(fh)
	var count int
	for scanner.Scan() {
		if limit > 0 && count >= limit {
			break
		}
		line := scanner.Bytes()
		record := transformer.FromBytes(line)
		storage.AddLogRecord(record)
		count++
	}
	fmt.Println("Loaded logs from file")
	os.Exit(0)
}

func init() {
	go func() {
		for {
			MemoryUsage()
			time.Sleep(1 * time.Second)
		}
	}()
}
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

// Report 77690955-9078-48c8-9d09-378c04615279 scanned 10000 records 887 hits in 0.235979201 seconds
// Report f1609296-9580-4424-8ade-fb05f6fafdf3 scanned 10000 records 887 hits in 0.234413362 seconds
