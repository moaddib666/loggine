package compression

type Type string

const (
	Gzip   Type = "gzip"
	Lz4    Type = "lz4"
	Snappy Type = "snappy"
	Zstd   Type = "zstd"
	None   Type = "none"
)
