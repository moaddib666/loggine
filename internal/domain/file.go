package domain

import (
	"fmt"
	"log"
	"os"
	"time"
	"unsafe"
)

func init() {
	log.Printf("Initialized with DataFileHeaderSize: %d\n", DataFileHeaderSize)
}

// DataFileHeader represents the header of the data file.
type DataFileHeader struct {
	Version            uint64    // 8 bytes
	Id                 uint32    // 4 bytes
	RecordCount        uint64    // 8 bytes
	Year               uint64    // 8 bytes
	Month              uint64    // 8 bytes
	Day                uint64    // 8 bytes
	LastDataPageNumber uint32    // 4 bytes (0 - 1439 pages / minutes)
	Reserved           [256]byte // 256 bytes reserved for future use
	Checksum           uint64    // 8 bytes
} // 312 bytes
const DataFileHeaderSize = int(unsafe.Sizeof(DataFileHeader{}.Version) +
	unsafe.Sizeof(DataFileHeader{}.Id) +
	unsafe.Sizeof(DataFileHeader{}.RecordCount) +
	unsafe.Sizeof(DataFileHeader{}.Year) +
	unsafe.Sizeof(DataFileHeader{}.Month) +
	unsafe.Sizeof(DataFileHeader{}.Day) +
	unsafe.Sizeof(DataFileHeader{}.LastDataPageNumber) +
	unsafe.Sizeof(DataFileHeader{}.Reserved) +
	unsafe.Sizeof(DataFileHeader{}.Checksum))

// Checksum calculates the checksum of the header.
func (h *DataFileHeader) UpdateChecksum() {
	h.Checksum = h.Month + h.Day + h.Year + h.RecordCount + uint64(h.LastDataPageNumber) + uint64(h.Id) + h.Version
}

// Time returns the year, month, and day as go time.
func (h *DataFileHeader) Time() time.Time {
	return time.Date(int(h.Year), time.Month(h.Month), int(h.Day), 0, 0, 0, 0, time.UTC)
}

// String returns the string representation of the header
// Example: "2024-10-25.4164052702"
func (h *DataFileHeader) String() string {
	return fmt.Sprintf("%04d-%02d-%02d.%d", h.Year, h.Month, h.Day, h.Id)
}

// DataFile represents a data file with a header and a set of pages.
type DataFile struct {
	Header *DataFileHeader
	*os.File
}

// NewDataFile creates a new DataFile.
func NewDataFile(header *DataFileHeader, f *os.File) *DataFile {
	return &DataFile{
		Header: header,
		File:   f,
	}
}
