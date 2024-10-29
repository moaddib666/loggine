package presenters

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"fmt"
	"strings"
	"time"
)

var _ ports.QueryResultPresenter = new(QueryResultPresenter)

// QueryResultPresenter is responsible for generating a string report for a query result
type QueryResultPresenter struct {
	logRecordPresenter ports.LogRecordPresenter // Injected presenter for LogRecord
}

// NewQueryResultPresenter creates a new QueryResultPresenter
func NewQueryResultPresenter(logRecordPresenter ports.LogRecordPresenter) *QueryResultPresenter {
	return &QueryResultPresenter{
		logRecordPresenter: logRecordPresenter,
	}
}

//TODO: split part to QueryPresenter and re use it here

// Present generates a string report for the QueryResult
func (p *QueryResultPresenter) Present(result *domain.QueryResult) string {
	var builder strings.Builder

	// Write the Query Info
	builder.WriteString("=========== Query Info ===========\n")
	builder.WriteString(fmt.Sprintf("Operation   : %s\n", result.Query.Operation))
	builder.WriteString(fmt.Sprintf("Database    : %s\n", result.Query.Database))
	builder.WriteString(fmt.Sprintf("Table       : %s\n", result.Query.Table))

	// Write partition if present
	if result.Query.Partition != nil {
		builder.WriteString(fmt.Sprintf("Partition   : %s\n", *result.Query.Partition))
	}

	// Write time range if present
	if result.Query.QueryTimeRange != nil {
		builder.WriteString(fmt.Sprintf("Time Range  : %s - %s\n",
			result.Query.QueryTimeRange.From.Format(time.RFC3339),
			result.Query.QueryTimeRange.To.Format(time.RFC3339)))
	}

	// Write fields
	builder.WriteString(fmt.Sprintf("Fields      : %s\n", strings.Join(result.Query.Fields, ", ")))

	// Write conditions
	if len(result.Query.Conditions) > 0 {
		builder.WriteString("Conditions  :\n")
		for _, cond := range result.Query.Conditions {
			builder.WriteString(fmt.Sprintf("  - %s %s %v\n", cond.Field, cond.Operator, cond.Value))
		}
	}

	// Write limit if present
	if result.Query.Limit != nil {
		builder.WriteString(fmt.Sprintf("Limit       : %d\n", *result.Query.Limit))
	}

	// Write aggregation if present
	if result.Query.AggregatedBy != nil {
		builder.WriteString(fmt.Sprintf("Aggregated By: %s\n", *result.Query.AggregatedBy))
	}

	builder.WriteString(fmt.Sprintf("Format      : %s\n", result.Query.Format))
	builder.WriteString("===================================\n\n")

	// Write the QueryReport Info
	builder.WriteString("=========== Query Report ===========\n")
	builder.WriteString(fmt.Sprintf("Report ID   : %s\n", result.Report.Id))
	builder.WriteString(fmt.Sprintf("Total ScannedItems : %d\n", result.Report.ScannedItems))
	builder.WriteString(fmt.Sprintf("Hits        : %d\n", result.Report.Hits))
	builder.WriteString(fmt.Sprintf("Miss        : %d\n", result.Report.Miss))
	builder.WriteString(fmt.Sprintf("Result Records : %d\n", len(result.Records)))
	builder.WriteString(fmt.Sprintf("Elapsed Time: %s\n", result.Report.ElapsedTime))
	builder.WriteString("====================================\n\n")

	// Write the records
	builder.WriteString("=========== Query Records ===========\n")
	if len(result.Records) == 0 {
		builder.WriteString("No records found.\n")
	} else {
		for i, record := range result.Records {
			builder.WriteString(fmt.Sprintf("\n--- Record %d ---\n", i+1))
			builder.WriteString(p.logRecordPresenter.Present(record)) // Use the LogRecordPresenter to format each record
		}
	}
	builder.WriteString("=====================================\n")

	return builder.String()
}
