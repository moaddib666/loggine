package main

import (
	"LogDb/internal/adapters/datastor"
	"LogDb/internal/adapters/serializer"
	"LogDb/internal/internal_errors"
	"LogDb/internal/ports"
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

func init() {
	//logrus.SetLevel(logrus.DebugLevel)
}

type ReadMode int // ReadMode is the mode in which the data file manager is opened.
// None - Don't read any records at all
// Full - Reads all records in the file. with labels and messages.
// Labels - Reads only the labels section of each record.
// Scan - Reads only the metadata section of each record.
func (r ReadMode) String() string {
	if r < None || r > Scan {
		return "Unknown"
	}
	return [...]string{"None", "Full", "Labels", "Scan"}[r]
}

const (
	None ReadMode = iota
	Full
	Labels
	Scan
)

var readMode = None

// SetReadMode sets the read mode for the data file manager.
func SetReadMode(mode ReadMode) {
	readMode = mode
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

// readChunk reads a chunk file and processes it, given the necessary factories.
func readChunk(fileName string, factory ports.DataFileManagerFactory, dataPageReaderFactory ports.DataPageReaderFactory) error {
	// Initialize the data file manager
	manager, err := factory.NewDataFileManager(fileName)
	if err != nil {
		return err
	}
	defer manager.Close()

	// Get the file header
	fileHeader, err := manager.GetHeader()
	if err != nil {
		return err
	}
	fmt.Printf("File header: %v\n", fileHeader)

	// Process each data page in the file
	for {
		dataPageHeader, err := manager.NextDataPage()
		if err != nil {
			if errors.Is(err, internal_errors.NoDataPagesLeft) {
				break
			}
			return err
		}

		fmt.Printf("DataPage Header: Number=%d, RecordCount=%d, PageSize=%d, CompressedPageSize=%d\n",
			dataPageHeader.Number, dataPageHeader.RecordCount, dataPageHeader.PageSize, dataPageHeader.CompressedPageSize)

		if dataPageHeader.RecordCount < 1 {
			continue
		}

		// Initialize the data page reader
		pageReader := dataPageReaderFactory.NewDataPageReader(dataPageHeader, manager.GetDataPageReader())

		// Process each record in the page
		sTime := time.Now()
		nMessages := processDataPage(pageReader, int(dataPageHeader.RecordCount))
		fmt.Printf("Read %s %d messages in %v \n", readMode, nMessages, time.Since(sTime))
	}

	return nil
}

// processDataPage processes all records in a data page and returns the number of messages read.
func processDataPage(pageReader ports.DataPageReader, recordCount int) int {
	nMessages := 0
	for i := 0; i < recordCount; i++ {
		// TODO:add read mode in pageReader
		nMessages++
		if readMode == None {
			break
		}

		if !pageReader.Scan() {
			break
		}

		meta := pageReader.Metadata()
		if readMode == Scan {
			continue
		}

		labels, err := pageReader.Labels()
		if err != nil {
			fmt.Printf("Error reading labels: %v\n", err)
			break
		}
		if readMode == Labels {
			continue
		}

		message, err := pageReader.Message()
		if err != nil {
			fmt.Printf("Error reading message: %v\n", err)
			break
		}

		if false { // Set to true to print the record
			fmt.Printf("Record %d: Metadata=%v, Labels=%v, Message=%s\n", i, meta, labels, message)
		}

	}

	return nMessages
}

// processAllChunks processes all chunk files from the provided directory using the provided factories.
func processAllChunks(directory string, factory ports.DataFileManagerFactory, dataPageReaderFactory ports.DataPageReaderFactory) error {
	// Find all files in the directory with a .chunk extension
	files, err := filepath.Glob(filepath.Join(directory, "*.chunk"))
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return fmt.Errorf("no chunk files found in directory %s", directory)
	}

	for _, file := range files {
		fmt.Printf("Processing file: %s\n", file)
		err := readChunk(file, factory, dataPageReaderFactory)
		if err != nil {
			fmt.Printf("Error processing file %s: %v\n", file, err)
		}
	}

	return nil
}

func main() {
	// Initialize the codec and factories
	codec := serializer.Default
	factory := datastor.NewDataFileManagerFactory(codec)
	dataPageReaderFactory := datastor.NewDataPageReaderFactory(codec)

	// Process all chunks in the .storage directory
	err := processAllChunks(".storage", factory, dataPageReaderFactory)
	panicOnError(err)
}
