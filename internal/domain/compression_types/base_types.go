package compression_types

type CompressionType uint8

// String returns the string representation of the compression type
func (c CompressionType) String() string {
	if c > Snappy {
		return "Unknown"
	}
	return [...]string{"None", "Zstd", "Gzip", "Lz4", "Snappy"}[c]
}

const (
	None CompressionType = iota
	Zstd
	Gzip
	Lz4
	Snappy
)
