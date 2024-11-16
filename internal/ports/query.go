package ports

import (
	"LogDb/internal/domain"
	"LogDb/internal/domain/query_types"
	"time"
)

type QueryBuilderFactory interface {
	NewQueryBuilder() QueryBuilder
}

// QueryBuilder defines the methods required to build a query
type QueryBuilder interface {
	// SelectFields sets the fields to select in the query
	SelectFields(fields ...string) QueryBuilder

	// SetPartition sets the partition (optional)
	SetPartition(partition string) QueryBuilder

	// Where adds a condition to the where clause
	Where(field string, operator query_types.QueryOperator, value interface{}) QueryBuilder

	// Limit sets the maximum number of records to return (optional)
	Limit(limit int) QueryBuilder

	// AggregateBy sets the aggregation dimension (optional)
	AggregateBy(dimension query_types.Dimension) QueryBuilder

	// SetFormat sets the output format (json, csv, etc.)
	SetFormat(format query_types.Format) QueryBuilder

	// SetTimeRange sets the time range for the query
	SetTimeRange(startTime, endTime time.Time) QueryBuilder

	// Build returns the final Query object
	Build() (*domain.Query, error)
}

type QueryPreparer interface {
	PrepareQuery(query *domain.Query) (PreparedQuery, error)
}

type PreparedQuery interface {
	FromDateTime() uint64
	ToDateTime() uint64

	Begin()
	Skip()
	Next(record *domain.LogRecord) error
	End()

	SetError(err error)
	Error() error

	Result() (*domain.QueryResult, error)
}
