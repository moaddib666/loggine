package adapters

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"errors"
	"io"
	"time"
)

func Query(storage ports.DataPageManager, filterSet ports.FilterSet) (*domain.QueryResult, error) {
	ts := time.Now()
	reader := storage.GetReader()
	defer reader.Close()

	var result domain.QueryResult
	for {
		record, _, err := reader.ScanLogRecord(filterSet)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		result.Records = append(result.Records, record)
	}
	result.Report = domain.NewQueryReport(
		reader.ScannedRecordsCount(),
		len(result.Records),
		ts,
	)

	return &result, nil
}
