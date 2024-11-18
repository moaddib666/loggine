package domain

import (
	"LogDb/internal/domain/compression_types"
	"fmt"
	"io"
	"log"
	"unsafe"
)

func init() {
	log.Printf("Initialized with DataPageHeaderSize: %d\n", DataPageHeaderSize)
}

type DataPageHeader struct {
	Number               uint32                            // 4 bytes - Minute number in 24 hours (0-1439)
	PageSize             uint64                            // 8 bytes - Size of the page in bytes
	RecordCount          uint64                            // 8 bytes - Number of records in the page
	CompressionAlgorithm compression_types.CompressionType // 1 byte - Compression algorithm used
	CompressedPageSize   uint64                            // 8 bytes - Size of the compressed page in bytes
} // 20 bytes

// String returns the string representation of the header
func (h *DataPageHeader) String() string {
	return fmt.Sprintf("%d", h.Number)
}

const DataPageHeaderSize = int(unsafe.Sizeof(DataPageHeader{}.Number) +
	unsafe.Sizeof(DataPageHeader{}.PageSize) +
	unsafe.Sizeof(DataPageHeader{}.RecordCount) +
	unsafe.Sizeof(DataPageHeader{}.CompressionAlgorithm) +
	unsafe.Sizeof(DataPageHeader{}.CompressedPageSize),
) // 29 bytes

// NewEmptyDataPageHeader creates a new DataPageHeader.
func NewEmptyDataPageHeader() *DataPageHeader {
	return &DataPageHeader{}
}

// NewDataPageHeaderForMinute creates a new DataPageHeader.
func NewDataPageHeaderForMinute(number uint32) *DataPageHeader {
	return &DataPageHeader{
		Number: number,
	}
}

type DataPage struct {
	Header *DataPageHeader
	io.ReadWriteSeeker
}

// NewDataPage creates a new DataPage.
func NewDataPage(header *DataPageHeader, reader io.ReadWriteSeeker) *DataPage {
	return &DataPage{
		Header:          header,
		ReadWriteSeeker: reader,
	}
}

type ReadOnlyDataPage struct {
	Header *DataPageHeader
	io.ReadSeeker
}

// NewReadOnlyDataPage creates a new ReadOnlyDataPage.
func NewReadOnlyDataPage(header *DataPageHeader, reader io.ReadSeeker) *ReadOnlyDataPage {
	return &ReadOnlyDataPage{
		Header:     header,
		ReadSeeker: reader,
	}
}
