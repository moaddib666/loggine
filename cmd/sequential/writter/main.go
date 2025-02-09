package main

import (
	"LogDb/internal/adapters/compression"
	"LogDb/internal/adapters/data/loader"
	"LogDb/internal/adapters/datastor"
	"LogDb/internal/adapters/serializer"
	"LogDb/internal/domain"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

func init() {
	// Set up the logger
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}
func main() {
	codec := serializer.Default
	dataFileFactory := datastor.NewDataFileWriterFactory(".lsm", codec, compression.Factory, log.NewEntry(log.StandardLogger()))
	dataPageHeaderFactory := datastor.NewDataPageHeaderFactory()
	repo := datastor.NewSequentialLogCollector(dataFileFactory, dataPageHeaderFactory)
	defer repo.Close()
	recordWritten := 0
	var offset = 3 * time.Hour
	loader.LinearLogWriter(time.Now().Add(-(time.Hour*1 + offset)), time.Now().Add(-(offset)), func(record *domain.LogRecord) error {
		//log.Debugf("Storing record timestamp %s", record.Timestamp)
		recordWritten++
		return repo.StoreLogRecord(record)
	})
	log.Infof("Wrote %d records", recordWritten)
}
