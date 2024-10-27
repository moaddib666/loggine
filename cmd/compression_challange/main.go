package main

import (
	"LogDb/internal/domain/compression_types"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"LogDb/internal/adapters/compression"
	"LogDb/internal/ports"
)

var compressors = []compression_types.CompressionType{compression_types.Gzip, compression_types.Lz4, compression_types.Snappy, compression_types.Zstd}

const compressedDir = "./compressed_files"
const decompressedDir = "./decompressed_files"

func main() {
	// Parse file path from command line argument
	inputFilePath := flag.String("file", "", "Path to the file to compress")
	flag.Parse()

	if *inputFilePath == "" {
		log.Fatal("Please provide a valid file path using the -file flag.")
	}

	// Ensure directories for compressed and decompressed files exist
	createDirectoryIfNotExist(compressedDir)
	createDirectoryIfNotExist(decompressedDir)

	// Open the input file for reading
	inputFile, err := os.Open(*inputFilePath)
	if err != nil {
		log.Fatalf("Failed to open input file: %v", err)
	}
	defer inputFile.Close()

	// Iterate over available compressors and compress the file
	for _, c := range compressors {
		compressor := compression.Factory(c)
		if err != nil {
			log.Fatalf("Failed to create compressor for type %v: %v", c, err)
		}

		// Compress and save to file
		compressFile(inputFile, *inputFilePath, compressor, c)

		// Decompress and verify integrity
		decompressFile(inputFile, *inputFilePath, compressor, c)
	}
}

// compressFile compresses the file content and writes the compressed data to a new file
func compressFile(inputFile *os.File, inputFilePath string, compressor ports.Compression, cType compression_types.CompressionType) {
	start := time.Now()

	// Reset file pointer to the beginning of the file
	_, err := inputFile.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatalf("Failed to reset file pointer: %v", err)
	}

	// Create compressed file with appropriate extension
	ext := getFileExtension(cType)
	compressedFilePath := filepath.Join(compressedDir, filepath.Base(inputFilePath)+ext)
	compressedFile, err := os.Create(compressedFilePath)
	if err != nil {
		log.Fatalf("Failed to create compressed file: %v", err)
	}
	defer compressedFile.Close()

	// Compress the file using CompressStream
	_, err = compressor.CompressStream(inputFile, compressedFile)
	if err != nil {
		log.Fatalf("Failed to compress file: %v", err)
	}

	// Measure compression time
	compressionTime := time.Since(start)

	// Get the original file size
	originalFileInfo, err := os.Stat(inputFilePath)
	if err != nil {
		log.Fatalf("Failed to get file info: %v", err)
	}
	originalSize := originalFileInfo.Size()

	// Get the compressed file size
	compressedFileInfo, err := os.Stat(compressedFilePath)
	if err != nil {
		log.Fatalf("Failed to get compressed file info: %v", err)
	}
	compressedSize := compressedFileInfo.Size()

	// Calculate compression ratio
	compressionRatio := float64(originalSize) / float64(compressedSize)

	fmt.Printf("Compressed %s with %v:\n", inputFilePath, cType)
	fmt.Printf("Original Size: %d bytes, Compressed Size: %d bytes\n", originalSize, compressedSize)
	fmt.Printf("Compression Ratio: %.2f\n", compressionRatio)
	fmt.Printf("Compression Time: %s\n", compressionTime)
}

// decompressFile decompresses the compressed file and verifies its integrity with the original content
func decompressFile(inputFile *os.File, inputFilePath string, compressor ports.Compression, cType compression_types.CompressionType) {
	// Reset the input file pointer again to ensure file consistency
	_, err := inputFile.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatalf("Failed to reset file pointer: %v", err)
	}

	ext := getFileExtension(cType)
	compressedFilePath := filepath.Join(compressedDir, filepath.Base(inputFilePath)+ext)

	// Open the compressed file for reading
	compressedFile, err := os.Open(compressedFilePath)
	if err != nil {
		log.Fatalf("Failed to open compressed file: %v", err)
	}
	defer compressedFile.Close()

	// Create decompressed file
	decompressedFilePath := filepath.Join(decompressedDir, filepath.Base(inputFilePath)+"_decompressed")
	decompressedFile, err := os.Create(decompressedFilePath)
	if err != nil {
		log.Fatalf("Failed to create decompressed file: %v", err)
	}
	defer decompressedFile.Close()

	// Decompress the file using DecompressStream
	_, err = compressor.DecompressStream(compressedFile, decompressedFile)
	if err != nil {
		log.Fatalf("Failed to decompress file: %v", err)
	}

	fmt.Printf("Decompression with %v succeeded for %s\n\n", cType, inputFilePath)
}

// createDirectoryIfNotExist ensures the provided directory exists, creating it if necessary
func createDirectoryIfNotExist(dir string) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatalf("Failed to create directory: %v", err)
	}
}

// getFileExtension returns the appropriate file extension for each compression type
func getFileExtension(cType compression_types.CompressionType) string {
	switch cType {
	case compression_types.Gzip:
		return ".gz"
	case compression_types.Lz4:
		return ".lz4"
	case compression_types.Snappy:
		return ".snappy"
	case compression_types.Zstd:
		return ".zstd"
	default:
		return ".bin"
	}
}
