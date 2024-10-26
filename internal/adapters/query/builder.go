package query

import (
	"LogDb/internal/domain"
	"LogDb/internal/domain/query_types"
	"time"
)

// QueryBuilder helps construct a query
type QueryBuilder struct {
	query domain.Query
}

// NewQueryBuilder initializes a new QueryBuilder
func NewQueryBuilder(operation query_types.Operation, database, table string) *QueryBuilder {
	return &QueryBuilder{
		query: domain.Query{
			Operation: operation,
			Database:  database,
			Table:     table,
			Fields:    []string{"*"},    // default to all fields
			Format:    query_types.JSON, // default format
			QueryTimeRange: &domain.QueryTimeRange{
				From: time.Now().UTC().Add(-24 * time.Hour),
				To:   time.Now().UTC(),
			},
		},
	}
}

// SelectFields sets the fields to select in the query
func (qb *QueryBuilder) SelectFields(fields ...string) *QueryBuilder {
	qb.query.Fields = fields
	return qb
}

// SetPartition sets the partition (optional)
func (qb *QueryBuilder) SetPartition(partition string) *QueryBuilder {
	qb.query.Partition = &partition
	return qb
}

// Where adds a condition to the where clause
func (qb *QueryBuilder) Where(field string, operator query_types.QueryOperator, value interface{}) *QueryBuilder {
	qb.query.Conditions = append(qb.query.Conditions, query_types.Condition{
		Field:    field,
		Operator: operator,
		Value:    value,
	})
	return qb
}

// Limit sets the maximum number of records to return (optional)
func (qb *QueryBuilder) Limit(limit int) *QueryBuilder {
	qb.query.Limit = &limit
	return qb
}

// AggregateBy sets the aggregation dimension (optional)
func (qb *QueryBuilder) AggregateBy(dimension query_types.Dimension) *QueryBuilder {
	qb.query.AggregatedBy = &dimension
	return qb
}

// SetFormat sets the output format (json, csv, etc.)
func (qb *QueryBuilder) SetFormat(format query_types.Format) *QueryBuilder {
	qb.query.Format = format
	return qb
}

// SetTimeRange sets the time range for the query
func (qb *QueryBuilder) SetTimeRange(startTime, endTime time.Time) *QueryBuilder {
	qb.query.QueryTimeRange = &domain.QueryTimeRange{
		From: startTime,
		To:   endTime,
	}
	return qb
}

// Build returns the final Query object
func (qb *QueryBuilder) Build() domain.Query {
	return qb.query
}
