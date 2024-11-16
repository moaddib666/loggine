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
	Version             uint64    // 8 bytes
	Id                  uint32    // 4 bytes
	RecordCount         uint64    // 8 bytes
	Year                uint64    // 8 bytes
	Month               uint64    // 8 bytes
	Day                 uint64    // 8 bytes
	LastDataPageNumber  uint32    // 4 bytes (0 - 1439 pages / minutes)
	FirstDataPageNumber uint32    // 4 bytes
	Reserved            [252]byte // 256 bytes reserved for future use
	Checksum            uint64    // 8 bytes
}

const DataFileHeaderSize = int(unsafe.Sizeof(DataFileHeader{}.Version) +
	unsafe.Sizeof(DataFileHeader{}.Id) +
	unsafe.Sizeof(DataFileHeader{}.RecordCount) +
	unsafe.Sizeof(DataFileHeader{}.Year) +
	unsafe.Sizeof(DataFileHeader{}.Month) +
	unsafe.Sizeof(DataFileHeader{}.Day) +
	unsafe.Sizeof(DataFileHeader{}.LastDataPageNumber) +
	unsafe.Sizeof(DataFileHeader{}.FirstDataPageNumber) +
	unsafe.Sizeof(DataFileHeader{}.Reserved) +
	unsafe.Sizeof(DataFileHeader{}.Checksum),
) // 312 bytes

const MaxDataPagesInDataFile = 1440

// NewDataFileHeader creates a new DataFileHeader.
func NewDataFileHeader(version uint64, id uint32, year uint64, month uint64, day uint64) *DataFileHeader {
	return &DataFileHeader{
		Version:             version,
		Id:                  id,
		Year:                year,
		Month:               month,
		Day:                 day,
		FirstDataPageNumber: 0,
		LastDataPageNumber:  0,
	}
}

// NewEmptyDataFileHeader creates a new DataFileHeader.
func NewEmptyDataFileHeader() *DataFileHeader {
	return &DataFileHeader{
		FirstDataPageNumber: 0,
		LastDataPageNumber:  0,
	}
}

// UpdateChecksum calculates the checksum of the header.
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

// NewDataFileFromPath creates a new DataFile from a file path.
func NewDataFileFromPath(header *DataFileHeader, path string, accessFlag int) (*DataFile, error) {
	f, err := os.OpenFile(path, accessFlag, 0600)
	if err != nil {
		return nil, err
	}
	return NewDataFile(header, f), nil
}

// NewWriteOnlyDataFile creates a new DataFile with write only access.
func NewWriteOnlyDataFile(header *DataFileHeader, path string) (*DataFile, error) {
	return NewDataFileFromPath(header, path, os.O_WRONLY|os.O_CREATE)
}

// NewReadOnlyDataFile creates a new DataFile with read only access.
func NewReadOnlyDataFile(header *DataFileHeader, path string) (*DataFile, error) {
	return NewDataFileFromPath(header, path, os.O_RDONLY)
}

// NewReadWriteDataFile creates a new DataFile with read write access.
func NewReadWriteDataFile(header *DataFileHeader, path string) (*DataFile, error) {
	return NewDataFileFromPath(header, path, os.O_RDWR|os.O_CREATE)
}
