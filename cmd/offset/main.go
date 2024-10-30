package main

import (
	"fmt"
	"io"
	"log"
	"os"
)

func main() {
	// Create a new file
	file, err := os.Create("example_file.txt")
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	// Write 100 bytes to the file
	data := make([]byte, 100)
	for i := range data {
		data[i] = byte(i)
	}
	n, err := file.Write(data)
	if err != nil || n != 100 {
		log.Fatalf("Failed to write 100 bytes: %v", err)
	}

	// Seek to position 50 from the start
	offset, err := file.Seek(50, io.SeekStart)
	if err != nil {
		log.Fatalf("Failed to seek to position 50 from start: %v", err)
	}
	fmt.Printf("Seek to position 50 from start: %d (expected 50)\n", offset)

	// Read the next 20 bytes from the file
	result, err := readNext20Bytes(file)
	if err != nil {
		log.Fatalf("Failed to read next 20 bytes: %v", err)
	}
	fmt.Printf("Read next 20 bytes: %v\n", result)

	// Seek to position 10 bytes forward from current position
	offset, err = file.Seek(10, io.SeekCurrent)
	if err != nil {
		log.Fatalf("Failed to seek 10 bytes from current position: %v", err)
	}
	fmt.Printf("Seek to position 10 from current (expected 60): %d\n", offset)

	// Seek to position 10 bytes backward from the end
	offset, err = file.Seek(-10, io.SeekEnd)
	if err != nil {
		log.Fatalf("Failed to seek 10 bytes from end: %v", err)
	}
	fmt.Printf("Seek to 10 bytes before the end (expected 90): %d\n", offset)

	// Clean up: remove the file after use
	err = os.Remove("example_file.txt")
	if err != nil {
		log.Fatalf("Failed to remove file: %v", err)
	}

}

// readNext20Bytes reads the next 20 bytes from the file starting from the current position.
func readNext20Bytes(file *os.File) ([]byte, error) {
	curPos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("failed to get current position: %w", err)
	}
	reader := io.NewSectionReader(file, curPos, 20)
	// seek 10 bytes from the current position
	offset, err := reader.Seek(10, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("failed to seek 10 bytes from current position: %w", err)
	}
	fmt.Printf("[NewSectionReader] Seeked 10 bytes from current position: %d\n", offset)
	return io.ReadAll(reader)
}
