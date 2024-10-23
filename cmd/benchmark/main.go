package main

import (
	"LogDb/internal/adapters"
	"LogDb/internal/adapters/log_transformers"
	"LogDb/internal/adapters/presenters"
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

const ApacheLogsFile = "assets/logfiles.log"

func main() {

	storage := adapters.NewSingleFileStorage("log.db")
	presenter := presenters.NewStringPresenter()
	loadApacheLogsFromFile(ApacheLogsFile, storage, 0)
	defer storage.Flush()

	//filter := log_filters.NewWordFilter("149.37.193.161")
	reader := storage.GetReader()
	for {
		record, _, err := reader.ScanLogRecord()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatalf("Error scanning log record: %v", err)
		}
		//if filter.FilterLogRecord(record) {
		fmt.Print(presenter.Present(record))
		//fmt.Printf("[%s] labels[%+v] %s\n", record.Timestamp.Format(time.RFC850), record.Labels, record.Message)
		//}
	}
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
