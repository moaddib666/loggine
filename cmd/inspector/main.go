package main

import (
	"LogDb/internal/adapters/inspector"
	"LogDb/internal/adapters/serializer"
	"flag"
	"fmt"
	"os"
)

var (
	filePath   string
	inspectFlg bool
	reportFlg  bool
)

func init() {
	// Define the flag for the file to inspect (-f)
	flag.StringVar(&filePath, "f", "", "Path to the file to inspect/report")

	// Define the inspect flag (-i, --inspect) to enable inspection
	flag.BoolVar(&inspectFlg, "i", false, "Enable file inspection")

	// Define the report flag (-r, --report) to enable reporting
	flag.BoolVar(&reportFlg, "r", false, "Enable quick file reporting (structure only)")
}

func main() {
	// Parse the command-line flags
	flag.Parse()

	// Check if neither the inspect nor report flags were provided, or file is missing
	if !inspectFlg && !reportFlg {
		fmt.Println("Please provide either the -i (inspect) or -r (report) flag to inspect/report the file.")
		flag.Usage()
		return
	}

	// Check if the file path was provided
	if filePath == "" {
		fmt.Println("Please provide a file to inspect/report using the -f flag.")
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

	// If inspect flag is set, run the inspection
	if inspectFlg {
		err = insp.Inspect()
		if err != nil {
			fmt.Printf("Error inspecting file: %v\n", err)
			return
		}
		fmt.Println("File inspection completed successfully.")
	}

	// If report flag is set, run the report
	if reportFlg {
		_, err = insp.Report()
		if err != nil {
			fmt.Printf("Error reporting file: %v\n", err)
			return
		}
	}
}
