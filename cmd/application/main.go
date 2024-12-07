package main

import (
	"LogDb/internal/adapters/api/web_api"
	"LogDb/internal/adapters/bus"
	"LogDb/internal/adapters/compression"
	"LogDb/internal/adapters/datastor"
	"LogDb/internal/adapters/filters"
	"LogDb/internal/adapters/filters/label_conditions"
	"LogDb/internal/adapters/index"
	"LogDb/internal/adapters/memtable"
	"LogDb/internal/adapters/merge"
	"LogDb/internal/adapters/monitoring"
	"LogDb/internal/adapters/query"
	"LogDb/internal/adapters/serializer"
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"os"
	time2 "time"
)

const BaseDir = ".storage"
const DataFileExt = "chunk"

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
}

func main() {
	prometheusExporter := monitoring.NewPrometheusAdapter()
	prometheusExporter.StartHTTPServer("9090")
	r := gin.Default()
	codec := serializer.Default
	repo := datastor.NewDataFileRepository(BaseDir, codec, DataFileExt)
	dataFileFactory := datastor.NewDataFileWriterFactory(repo, compression.Factory, log.NewEntry(log.StandardLogger()))
	dataPageHeaderFactory := datastor.NewDataPageHeaderFactory()

	dataFileManagerFactory := datastor.NewDataFileManagerFactory(repo)
	dataPageReaderFactory := datastor.NewDataPageReaderFactory(repo.Codec(), domain.SmallChunks)

	merger := merge.NewMerger(
		dataFileFactory,
		dataFileManagerFactory,
		dataPageReaderFactory,
		repo,
	)

	idx := index.NewTimestamp(repo, merger)
	dataFilesChangesBus := bus.NewDataFilesManager()
	dataFilesChangesBus.OnDataFileCreated(
		func(header *domain.DataFileHeader) {
			_ = idx.AddDataFile(header)
		},
	)

	sequentialWriter := datastor.NewSequentialLogCollectorFactory(
		dataFileFactory,
		dataPageHeaderFactory,
		dataFilesChangesBus,
	)
	flusher := memtable.NewFlusher(sequentialWriter)
	defer flusher.Close()
	memTable := memtable.NewMemTable(1024*1024*1024, 1_000_000, func(maxSize, maxRecords int) ports.HeapChunk {
		return memtable.NewHeapChunk(maxSize, maxRecords)
	}, flusher, 60*time2.Second)

	storage := datastor.NewPersistentStorage(memTable, dataFileManagerFactory, dataPageReaderFactory, idx)
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
