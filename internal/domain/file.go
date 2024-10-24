package domain

import "time"

// DataFileHeader represents the header of the data file.
type DataFileHeader struct {
	Version     uint64    // 8 bytes
	Id          uint32    // 4 bytes
	RecordCount uint64    // 8 bytes
	Year        uint64    // 8 bytes
	Month       uint64    // 8 bytes
	Day         uint64    // 8 bytes
	Reserved    [256]byte // 256 bytes reserved for future use
	Checksum    uint64    // 8 bytes
} // Total size: 308 bytes

// Time returns the year, month, and day as go time.
func (h *DataFileHeader) Time() time.Time {
	return time.Date(int(h.Year), time.Month(h.Month), int(h.Day), 0, 0, 0, 0, time.UTC)
}

const DataFileHeaderSize = 312

// DataFile represents a data file with a header and a set of pages.
type DataFile struct {
	Header DataFileHeader
	Pages  []DataPage
}
