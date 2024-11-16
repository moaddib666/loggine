package main

import (
	"LogDb/internal/adapters/api/web_api"
	"LogDb/internal/adapters/compression"
	"LogDb/internal/adapters/datastor"
	"LogDb/internal/adapters/filters"
	"LogDb/internal/adapters/filters/label_conditions"
	"LogDb/internal/adapters/index"
	"LogDb/internal/adapters/memtable"
	"LogDb/internal/adapters/query"
	"LogDb/internal/adapters/serializer"
	"LogDb/internal/ports"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	time2 "time"
)

const BaseDir = ".storage"

func main() {
	r := gin.Default()
	codec := serializer.Default
	idx := index.NewTimestamp(BaseDir, codec)
	dataFileFactory := datastor.NewDataFileWriterFactory(".lsm", codec, compression.Factory, log.NewEntry(log.StandardLogger()))
	dataPageHeaderFactory := datastor.NewDataPageHeaderFactory()
	sequentialWriter := datastor.NewSequentialLogCollectorFactory(
		dataFileFactory,
		dataPageHeaderFactory,
	)
	flusher := memtable.NewFlusher(sequentialWriter)
	defer flusher.Close()
	memTable := memtable.NewMemTable(1024*1024*1024, 1_000_000, func(maxSize, maxRecords int) ports.HeapChunk {
		return memtable.NewHeapChunk(maxSize, maxRecords)
	}, flusher, 60*time2.Second)

	storage := datastor.NewPersistentStorage(BaseDir, codec, memTable, idx)
	defer storage.Close()
	queryBuilderFactory := query.NewQueryBuilderFactory()
	queryProcessor := query.NewPreparer(filters.Factory, label_conditions.Factory)

	api := web_api.NewWebApi(storage, queryBuilderFactory, queryProcessor) // Initialize storage
	api.RegisterRoutes(r)
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	err := r.Run(":8080")
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
