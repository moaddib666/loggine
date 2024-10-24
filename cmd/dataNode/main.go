package main

import (
	"LogDb/internal/adapters/filters"
	"LogDb/internal/adapters/log_transformers"
	"LogDb/internal/adapters/presenters"
	"LogDb/internal/adapters/repository"
	"LogDb/internal/adapters/serializer"
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
)

const ApacheLogsFile = "assets/logfiles.log"

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

func main() {

	codec := &serializer.BinarySerializer{}
	writer := repository.NewDataFileWriter(codec, time.Now())
	err := writer.Open()
	if err != nil {
		log.Fatalln(err)
	}
	defer writer.Close()
	// Write some data
	loadApacheLogsFromFile(ApacheLogsFile, writer, 10_000)

	reader := repository.NewDataFileReader(codec)
	err = reader.Open("2024-10-25.514848382-work.chunk")
	if err != nil {
		log.Fatalln(err)
	}
	defer reader.Close()
	filterSet := filters.NewDateRangeFilter(time.Now().Add(-time.Hour), time.Now().Add(+time.Hour))
	// Read some data
	result, err := reader.Query(filterSet)
	if err != nil {
		log.Fatalf("Error querying storage: %v", err)
	}
	presenter := presenters.NewStringPresenter()
	for _, record := range result.Records {
		fmt.Println(presenter.Present(record))
	}
	fmt.Printf("report: %s scanned %d records %d hits in %v seconds\n", result.Report.Id, result.Report.Count, result.Report.Hits, result.Report.ElapsedTime.Seconds())

}

func loadApacheLogsFromFile(fileName string, storage *repository.DataFileWriter, limit int) {

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
		err := storage.AppendLogRecord(record)
		if err != nil {
			log.Fatalf("Error appending record: %v", err)
		}
		count++
	}
	fmt.Printf("Loaded %d logs from file\n", count)
	os.Exit(0)
}
