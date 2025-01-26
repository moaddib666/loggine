package main

import (
	"LogDb/internal/adapters/compression"
	"LogDb/internal/adapters/compressor"
	"LogDb/internal/adapters/datastor"
	"LogDb/internal/adapters/serializer"
	"LogDb/internal/domain/compression_types"
	"LogDb/internal/ports"
	log "github.com/sirupsen/logrus"
)

const BaseDir = ".compression"
const DataFileExt = "chunk"

var targetFile = "2024-11-22.1648502973"

func main() {

	codec := serializer.Default
	repo := datastor.NewDataFileRepository(BaseDir, codec, DataFileExt)

	dataFileFactory := datastor.NewDataFileWriterFactory(repo, log.NewEntry(log.StandardLogger()))
	dataFileManagerFactory := datastor.NewDataFileManagerFactory(repo)

	fc := compressor.NewDataFileCompressor(
		repo,
		dataFileFactory,
		dataFileManagerFactory,
		compression.Factory,
		compression_types.Zstd,
	)

	// Compress a data file
	err := compressDataFile(targetFile, fc, repo)
	if err != nil {
		panic(err)
	}

}

// compressDataFile compresses a data file
func compressDataFile(name string, compressor ports.DataCompressor, repo ports.DataFileRepository) error {
	df, err := repo.Open(name)
	if err != nil {
		return err
	}
	_, err = compressor.CompressDataFile(df)
	return err
}
