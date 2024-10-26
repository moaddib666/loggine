package loader

import (
	"LogDb/internal/adapters/log_transformers"
	"LogDb/internal/domain"
	"bufio"
	"fmt"
	"log"
	"os"
)

type callback func(record *domain.LogRecord) error

func LoadApacheLogsFromFile(fileName string, cb callback, limit int) {

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
		err := cb(record)
		if err != nil {
			log.Fatalf("Error appending record: %v", err)
		}
		count++
	}
	fmt.Printf("Loaded %d logs from file\n", count)
}
