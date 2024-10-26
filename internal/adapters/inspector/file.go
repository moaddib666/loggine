package inspector

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
)

type FileConsistencyInspector struct {
	fh    *os.File
	codec ports.Serializer
}

func NewFileConsistencyInspector(fh *os.File, codec ports.Serializer) *FileConsistencyInspector {
	return &FileConsistencyInspector{
		fh:    fh,
		codec: codec,
	}
}

// InspectHeader checks the consistency of the file header
// hexdump header | string representation of header
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
	printHeaderHexdump(headerBytes, header)

	return header, nil
}

// InspectDataPages checks the consistency of the data pages
func (f *FileConsistencyInspector) InspectDataPages() error {
	for {
		err := f.InspectDataPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// InspectDataPage checks the consistency of a single data page
func (f *FileConsistencyInspector) InspectDataPage() error {
	hd, err := f.InspectDataPageHeader()
	if err != nil {
		return err
	}
	err = f.InspectRecords(hd.PageSize, hd.RecordCount)
	if err != nil {
		return err
	}
	return nil
}

// InspectDataPageHeader checks the consistency of a single data page header
func (f *FileConsistencyInspector) InspectDataPageHeader() (*domain.DataPageHeader, error) {
	headerBytes := make([]byte, domain.DataPageHeaderSize)
	header := &domain.DataPageHeader{}
	_, err := f.fh.Read(headerBytes)
	if err != nil {
		return nil, err
	}
	_, err = f.codec.ReadDataPageHeader(header, bytes.NewReader(headerBytes))
	return header, err
}

// InspectRecords checks the consistency of the records in a data page
func (f *FileConsistencyInspector) InspectRecords(pageSize, recordsCount uint64) error {
	var offset int64
	for i := uint64(0); i < recordsCount; i++ {
		n, err := f.InspectRecord()
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

// InspectRecord checks the consistency of a single record
func (f *FileConsistencyInspector) InspectRecord() (int64, error) {
	recordHeaderBytes := make([]byte, domain.RecordMetaSize)
	recordMeta := &domain.RecordMeta{}
	_, err := f.fh.Read(recordHeaderBytes)
	if err != nil {
		return 0, err
	}
	_, err = f.codec.ReadLogRecordMeta(recordMeta, bytes.NewReader(recordHeaderBytes))
	if err != nil {
		return 0, err
	}
	labelBytes := make([]byte, recordMeta.LabelsSize)
	_, err = f.fh.Read(labelBytes)
	if err != nil {
		return 0, err
	}
	for i := uint64(0); i < recordMeta.LabelsCount; i++ {
		label := &domain.Label{}
		_, err = f.codec.ReadLogLabel(label, bytes.NewReader(labelBytes))
		if err != nil {
			return 0, err
		}
	}
	messageBytes := make([]byte, recordMeta.MessageSize)
	_, err = f.fh.Read(messageBytes)
	if err != nil {
		return 0, err
	}
	return int64(domain.RecordMetaSize + recordMeta.RecordSize), nil
}

// Inspect checks the consistency of the file
func (f *FileConsistencyInspector) Inspect() error {
	_, err := f.InspectHeader()
	if err != nil {
		return err
	}
	return f.InspectDataPages()

}

func printHeaderHexdump(headerBytes []byte, header *domain.DataFileHeader) {
	// Hexdump formatting
	hexDump := hex.Dump(headerBytes)

	// Build string representation of the header fields
	headerString := fmt.Sprintf(
		"Version: %d\nId: %d\nRecordCount: %d\nYear: %d\nMonth: %d\nDay: %d\nLastDataPageNumber: %d\nChecksum: %d",
		header.Version,
		header.Id,
		header.RecordCount,
		header.Year,
		header.Month,
		header.Day,
		header.LastDataPageNumber,
		header.Checksum,
	)

	// Format and print side-by-side table of hexdump and header field string representation
	fmt.Println("Hexdump of Header | String Representation")
	fmt.Println(strings.Repeat("-", 80))

	hexLines := strings.Split(hexDump, "\n")
	headerLines := strings.Split(headerString, "\n")

	// Iterate through both and print them side by side
	for i := 0; i < len(hexLines) || i < len(headerLines); i++ {
		hexPart := ""
		strPart := ""

		if i < len(hexLines) {
			hexPart = hexLines[i]
		}
		if i < len(headerLines) {
			strPart = headerLines[i]
		}

		fmt.Printf("%-48s | %s\n", hexPart, strPart)
	}
	fmt.Println(strings.Repeat("-", 80))
}
