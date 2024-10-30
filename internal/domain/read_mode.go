package domain

type ReadMode int8 // ReadMode is the mode in which the data file manager is opened.
// None - Don't read any records at all
// Full - Reads all records in the file. with labels and messages.
// Labels - Reads only the labels section of each record.
// Scan - Reads only the metadata section of each record.
func (r ReadMode) String() string {
	if r < None || r > Full {
		return "Unknown"
	}
	return [...]string{"None", "Full"}[r]
}

const (
	// None -  Read sequentially line by line minimal memory usage
	None ReadMode = iota
	// Full - Reads all records in the file into memory no matter the size
	Full
	XSmallChunks
	// SmallChunks read in small chunks 10MB at a time
	SmallChunks
	// LargeChunks read in large chunks 100MB at a time
	LargeChunks
)
