package domain

import (
	"fmt"
	"io"
	"log"
	"unsafe"
)

func init() {
	log.Printf("Initialized with DataPageHeaderSize: %d\n", DataPageHeaderSize)
}

type DataPageHeader struct {
	Number      uint32 // 4 bytes - Minute number in 24 hours (0-1439)
	PageSize    uint64 // 8 bytes - Size of the page in bytes
	RecordCount uint64 // 8 bytes - Number of records in the page
} // 20 bytes

// String returns the string representation of the header
func (h *DataPageHeader) String() string {
	return fmt.Sprintf("%d", h.Number)
}

const DataPageHeaderSize = int(unsafe.Sizeof(DataPageHeader{}.Number) +
	unsafe.Sizeof(DataPageHeader{}.PageSize) +
	unsafe.Sizeof(DataPageHeader{}.RecordCount))

type DataPage struct {
	Header *DataPageHeader
	io.ReadWriteSeeker
}
