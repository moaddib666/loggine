package main

import (
	"LogDb/internal/adapters/inspector"
	"LogDb/internal/adapters/serializer"
	"flag"
	"fmt"
	"os"
)

var filePath string

func init() {
	// Define the flag for the file to inspect (-f)
	flag.StringVar(&filePath, "f", "", "Path to the file to inspect")
}

func main() {
	// Parse the command-line flags
	flag.Parse()

	// Check if the file path was provided
	if filePath == "" {
		fmt.Println("Please provide a file to inspect using the -f flag")
		flag.Usage()
		return
	}

	// Open the file for inspection
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
		return
	}
	defer file.Close()

	// Create a new FileConsistencyInspector with the file and default serializer
	insp := inspector.NewFileConsistencyInspector(file, serializer.Default)

	// Inspect the file and handle any errors
	err = insp.Inspect()
	if err != nil {
		fmt.Printf("Error inspecting file: %v\n", err)
		return
	}

	fmt.Println("File inspection completed successfully.")
}
