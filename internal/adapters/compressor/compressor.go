package compressor

import (
	"LogDb/internal/domain"
	"LogDb/internal/domain/compression_types"
	"LogDb/internal/internal_errors"
	"LogDb/internal/ports"
	"errors"
	"io"
)

type DataFileCompressor struct {
	repo            ports.DataFileRepository
	dfWriterFactory ports.DataFileWriterFactory
	dfReaderFactory ports.DataFileReaderFactory
	codec           ports.Serializer
	compression     ports.CompressionFactoryMethod
	compressionType compression_types.CompressionType
}

// CompressDataFile compresses a data file.
func (d *DataFileCompressor) CompressDataFile(df *domain.DataFile) (*domain.DataFile, error) {
	if df.Header.Compressed {
		return nil, internal_errors.DataFileAlreadyCompressed
	}
	targetDataFileHeader := *df.Header
	targetDataFileHeader.MarkCompressed()
	targetDf, err := d.repo.CreateTempFromHeader(&targetDataFileHeader)
	if err != nil {
		return nil, err
	}
	targetDfWriter, err := d.dfWriterFactory.FromDataFile(targetDf)
	if err != nil {
		return nil, err
	}
	defer targetDfWriter.Close()
	sourceDfReader := d.dfReaderFactory.FromDataFile(df)

	var dataPagesHeaders []domain.DataPageHeader

	for {
		selectedDataPageHeader, err := sourceDfReader.NextDataPage()
		if err != nil {
			if errors.Is(err, internal_errors.NoDataPagesLeft) {
				break
			}
			return nil, err
		}
		if _, err := d.codec.WriteDataPageHeader(selectedDataPageHeader, targetDfWriter.Source()); err != nil {
			return nil, err
		}

		// seek 0 curent to understand how much data we has been written
		pos, err := targetDfWriter.Source().Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		if _, err := d.compression(d.compressionType).CompressStream(sourceDfReader.GetDataPageReader(), targetDfWriter.Source()); err != nil {
			return nil, err
		}

		if newPos, err := targetDfWriter.Source().Seek(0, io.SeekCurrent); err != nil {
			return nil, err
		} else {
			selectedDataPageHeader.CompressedPageSize = uint64(newPos - pos)
		}
		// 341 for first page
		// 917 total size
		selectedDataPageHeader.CompressionAlgorithm = d.compressionType
		dataPagesHeaders = append(dataPagesHeaders, *selectedDataPageHeader)
	}
	// Now we need to update the headers with the new data pages
	// Seek to the beginning of the file + header size
	if _, err := targetDfWriter.Source().Seek(int64(domain.DataFileHeaderSize), io.SeekStart); err != nil {
		return nil, err
	}
	for _, dp := range dataPagesHeaders {
		if _, err := d.codec.WriteDataPageHeader(&dp, targetDfWriter.Source()); err != nil {
			return nil, err
		}
		// Skip the compressed data
		if _, err := targetDfWriter.Source().Seek(int64(dp.CompressedPageSize), io.SeekCurrent); err != nil {
			return nil, err
		}
	}
	_ = sourceDfReader.Close()

	if err := d.repo.DeleteByHeader(df.Header); err != nil {
		return nil, err
	}

	if err := d.repo.MakePermanentFromHeader(targetDf); err != nil {
		return nil, err
	}
	df.Header.MarkCompressed()
	return targetDf, nil
}

// NewDataFileCompressor creates a new DataFileCompressor.
func NewDataFileCompressor(
	repo ports.DataFileRepository,
	dfWriterFactory ports.DataFileWriterFactory,
	dfReaderFactory ports.DataFileReaderFactory,
	compressionFactoryMethod ports.CompressionFactoryMethod,
	compressionType compression_types.CompressionType,
) *DataFileCompressor {
	return &DataFileCompressor{
		repo:            repo,
		dfWriterFactory: dfWriterFactory,
		dfReaderFactory: dfReaderFactory,
		codec:           repo.Codec(),
		compression:     compressionFactoryMethod,
		compressionType: compressionType,
	}
}
