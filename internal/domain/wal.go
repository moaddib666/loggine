package domain

import (
	"github.com/google/uuid"
	"time"
)

// WALHeader represents the header of the WAL file.
type WALHeader struct {
	ID         uint32 // 4 bytes
	Version    uint8  // 1 byte
	CreatedAt  uint64 // 8 bytes (Unix timestamp)
	ReadCursor uint64 // 8 bytes (Offset of the last read record)
}

// DefaultWALVersion is the default version of the WAL file.
const DefaultWALVersion = 1

// WALHeaderSize is the size of the WAL header in bytes.
const WALHeaderSize = 4 + 1 + 8

// NewWALHeader creates a new WAL header with the given ID and creation time.
func NewWALHeader() *WALHeader {
	return &WALHeader{
		ID:        uuid.New().ID(),
		Version:   DefaultWALVersion,
		CreatedAt: uint64(time.Now().Unix()),
	}
}
