package inspector

import (
	"LogDb/internal/adapters/compression"
	"LogDb/internal/domain"
	"LogDb/internal/domain/compression_types"
	"LogDb/internal/ports"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"reflect"
	"text/tabwriter"
	"time"
)

type FileConsistencyInspector struct {
	fh     *os.File
	codec  ports.Serializer
	report *InspectionReport
}

func NewFileConsistencyInspector(fh *os.File, codec ports.Serializer) *FileConsistencyInspector {
	return &FileConsistencyInspector{
		fh:    fh,
		codec: codec,
		report: &InspectionReport{
			Header:    nil,
			DataPages: []*domain.DataPageHeader{},
		},
	}
}

// Inspect performs a full consistency check, including header validation and record size verification
func (f *FileConsistencyInspector) Inspect() error {
	fmt.Println("========= Inspecting File Header and Content =========")
	_, err := f.InspectHeader()
	if err != nil {
		return err
	}

	err = f.InspectDataPages()
	if err != nil {
		return err
	}

	fmt.Println("Inspection completed successfully.")
	return nil
}

// Report generates a quick view of the file structure without checking the content consistency
func (f *FileConsistencyInspector) Report() (*InspectionReport, error) {
	fmt.Println("\n=========== Quick File Structure Report ===========")

	// Print the header section
	err := f.ReportHeader()
	if err != nil {
		return nil, err
	}

	// Print the data pages section
	err = f.ReportDataPages()
	if err != nil {
		return nil, err
	}

	// Enhanced console output in a table format
	fmt.Println("\n=========== File Header ===========")
	fmt.Printf(" %-20s: %d\n", "Version", f.report.Header.Version)
	fmt.Printf(" %-20s: %d\n", "Id", f.report.Header.Id)
	fmt.Printf(" %-20s: %d\n", "Record Count", f.report.Header.RecordCount)
	fmt.Printf(" %-20s: %d\n", "Data Pages Count", len(f.report.DataPages))
	fmt.Printf(" %-20s: %d\n", "Checksum", f.report.Header.Checksum)
	fmt.Printf(" %-20s: %s\n", "Date", f.report.Header.Time().Format(time.RFC3339))
	fmt.Println("============================================")
	fmt.Printf(" %-20s: %d\n", "First Data Page Number", f.report.Header.FirstDataPageNumber)
	fmt.Printf(" %-20s: %d\n", "Last Data Page Number", f.report.Header.LastDataPageNumber)
	startMinute := f.report.Header.Time().Add(time.Duration(f.report.Header.FirstDataPageNumber) * time.Minute)
	endMinute := f.report.Header.Time().Add(time.Duration(f.report.Header.LastDataPageNumber) * time.Minute)
	// Print the first and last data page timestamps
	fmt.Printf(" %-20s: %s\n", "First Data Page Time", startMinute.Format(time.RFC3339))
	fmt.Printf(" %-20s: %s\n", "Last Data Page Time", endMinute.Format(time.RFC3339))
	// Total minutes in the file
	totalMinutes := f.report.Header.LastDataPageNumber - f.report.Header.FirstDataPageNumber + 1
	fmt.Printf(" %-20s: %d\n", "Total Minutes", totalMinutes)

	// Visual separator between the header and the data pages
	fmt.Println("\n=========== Data Pages ===========")

	// Use tabwriter for aligned output of data pages
	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "Page Number\tPage Size\tRecord Count\tCompression Type\tCompressed Page Size")

	for _, page := range f.report.DataPages {
		fmt.Fprintf(writer, "%d\t%d\t%d\t%s\t%d\t\n", page.Number, page.PageSize, page.RecordCount, compression_types.CompressionType(page.CompressionAlgorithm), page.CompressedPageSize)
	}

	// Flush the tabwriter buffer to output the table
	writer.Flush()

	fmt.Println("============================================")
	return f.report, nil
}

// InspectHeader checks the file header for consistency
func (f *FileConsistencyInspector) InspectHeader() (*domain.DataFileHeader, error) {
	_, err := f.fh.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	header := &domain.DataFileHeader{}
	headerBytes := make([]byte, domain.DataFileHeaderSize)
	_, err = f.fh.Read(headerBytes)
	if err != nil {
		return nil, err
	}

	_, err = f.codec.ReadFileHeader(header, bytes.NewReader(headerBytes))
	if err != nil {
		return nil, err
	}
	printHexdumpWithTitle("Data File Header", headerBytes, header)

	f.report.Header = header // Save header for reporting
	return header, nil
}

// ReportHeader reads the file header without checking content
func (f *FileConsistencyInspector) ReportHeader() error {
	_, err := f.fh.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	header := &domain.DataFileHeader{}
	headerBytes := make([]byte, domain.DataFileHeaderSize)
	_, err = f.fh.Read(headerBytes)
	if err != nil {
		return err
	}

	_, err = f.codec.ReadFileHeader(header, bytes.NewReader(headerBytes))
	if err != nil {
		return err
	}
	f.report.Header = header // Save header for reporting
	return nil
}

// InspectDataPages inspects each data page and its records for consistency
func (f *FileConsistencyInspector) InspectDataPages() error {
	fmt.Println("\n========= Inspecting Data Pages and Validating Sizes =========")
	pageNumber := 0
	for {
		fmt.Printf("\n-- Data Page %d --\n", pageNumber)
		err := f.InspectDataPage()
		if err == io.EOF {
			fmt.Println("End of file reached.")
			break
		}
		if err != nil {
			return err
		}
		pageNumber++
	}
	return nil
}

// ReportDataPages quickly reads data page headers and skips record inspection
func (f *FileConsistencyInspector) ReportDataPages() error {
	pageNumber := 0
	for {
		err := f.ReportDataPage()
		if err == io.EOF {
			fmt.Println("End of file reached.")
			break
		}
		if err != nil {
			return err
		}
		pageNumber++
	}
	return nil
}

// InspectDataPage checks a single data page's consistency
func (f *FileConsistencyInspector) InspectDataPage() error {
	hd, err := f.InspectDataPageHeader(false)
	if err != nil {
		return err
	}

	fmt.Printf("Page Header: %+v\n", hd)
	fmt.Println("--------- Validating Data Page Records ---------")

	// Validate the records and ensure the sizes match the expected page size
	var decompressor ports.Compression
	var recordsReader io.Reader
	if hd.CompressionAlgorithm != compression_types.None {
		decompressor = compression.Factory(hd.CompressionAlgorithm)
		decompressedDataBuffer := bytes.NewBuffer(make([]byte, 0, hd.PageSize))
		compressedReader := io.LimitReader(f.fh, int64(hd.CompressedPageSize))

		_, err := decompressor.DecompressStream(compressedReader, decompressedDataBuffer)
		if err != nil {
			return err
		}
		recordsReader = bytes.NewReader(decompressedDataBuffer.Bytes())
	} else {
		recordsReader = io.LimitReader(f.fh, int64(hd.PageSize))
	}
	err = f.InspectRecords(hd.PageSize, hd.RecordCount, recordsReader)
	if err != nil {
		return err
	}

	fmt.Println("-------------------------------------")
	f.report.DataPages = append(f.report.DataPages, hd) // Add page header for reporting
	return nil
}

// ReportDataPage quickly reads a data page header and skips over the records
func (f *FileConsistencyInspector) ReportDataPage() error {
	hd, err := f.InspectDataPageHeader(true)
	if err != nil {
		return err
	}

	// Seek past the records to the next data page
	offset := int64(hd.PageSize)
	if hd.CompressionAlgorithm != compression_types.None {
		offset = int64(hd.CompressedPageSize)
	}
	_, err = f.fh.Seek(offset, io.SeekCurrent)
	if err != nil {
		return err
	}

	f.report.DataPages = append(f.report.DataPages, hd)
	return nil
}

// InspectDataPageHeader reads and returns a data page header
func (f *FileConsistencyInspector) InspectDataPageHeader(silent bool) (*domain.DataPageHeader, error) {
	headerBytes := make([]byte, domain.DataPageHeaderSize)
	header := &domain.DataPageHeader{}
	_, err := f.fh.Read(headerBytes)
	if err != nil {
		return nil, err
	}

	_, err = f.codec.ReadDataPageHeader(header, bytes.NewReader(headerBytes))
	if err != nil {
		return nil, err
	}
	if !silent {
		printHexdumpWithTitle("Data Page Header", headerBytes, header)
	}
	return header, nil
}

// InspectRecords validates the records in a data page to ensure they match the page size
func (f *FileConsistencyInspector) InspectRecords(pageSize, recordsCount uint64, reader io.Reader) error {
	var offset int64
	for i := uint64(0); i < recordsCount; i++ {
		fmt.Printf("\n-- Record %d --\n", i+1)
		n, err := f.InspectRecord(reader)
		if err != nil {
			return err
		}
		offset += n
	}
	if offset != int64(pageSize) {
		return fmt.Errorf("page size mismatch: expected %d, got %d", pageSize, offset)
	}
	return nil
}

// InspectRecord validates a single record's consistency
func (f *FileConsistencyInspector) InspectRecord(reader io.Reader) (int64, error) {
	recordHeaderBytes := make([]byte, domain.RecordMetaSize)
	recordMeta := &domain.RecordMeta{}
	_, err := reader.Read(recordHeaderBytes)
	if err != nil {
		return 0, err
	}
	_, err = f.codec.ReadLogRecordMeta(recordMeta, bytes.NewReader(recordHeaderBytes))
	if err != nil {
		return 0, err
	}
	printHexdumpWithTitle("Record Meta", recordHeaderBytes, recordMeta)

	// Read and validate labels
	labelBytes := make([]byte, recordMeta.LabelsSize)
	_, err = reader.Read(labelBytes)
	if err != nil {
		return 0, err
	}
	printHexdumpWithTitle("Record Labels", labelBytes, recordMeta)

	// Read and validate message
	messageBytes := make([]byte, recordMeta.MessageSize)
	_, err = reader.Read(messageBytes)
	if err != nil {
		return 0, err
	}
	printHexdumpWithTitle("Record Message", messageBytes, recordMeta)

	return int64(recordMeta.RecordSize), nil
}

// InspectionReport holds the file structure without inspecting the content in-depth
type InspectionReport struct {
	Header    *domain.DataFileHeader
	DataPages []*domain.DataPageHeader
}

// Helper function to print a titled hexdump of the data with object values
func printHexdumpWithTitle(title string, data []byte, object interface{}) {
	fmt.Printf("\n--- %s ---\n", title)
	hexDump := hex.Dump(data)
	fmt.Println(hexDump)

	// Reflect on the struct and print its fields
	fmt.Println("\n--- Field Values ---")
	printObjectFields(object)
	fmt.Println("---------------------------")
}

// Helper function to reflect on the struct fields and print them
func printObjectFields(obj interface{}) {
	val := reflect.ValueOf(obj)
	typ := reflect.TypeOf(obj)

	if val.Kind() == reflect.Ptr {
		val = val.Elem()
		typ = typ.Elem()
	}

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)
		fmt.Printf("%-20s : %v\n", fieldType.Name, field.Interface())
	}
}
