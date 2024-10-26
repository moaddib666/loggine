package ports

import "LogDb/internal/domain"

// WALRepository defines the interface for managing Write-Ahead Logs.
type WALRepository interface {
	// StoreRecord stores a new log record in the WAL.
	StoreRecord(r *domain.LogRecord) error

	// Flush writes the WAL contents to disk, typically called when max size/records are reached.
	Flush() error

	// Close closes the WAL and processes it, typically called at shutdown.
	Close() error
}
