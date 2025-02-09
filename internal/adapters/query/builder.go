package query

import (
	"LogDb/internal/domain"
	"LogDb/internal/domain/query_types"
	"LogDb/internal/ports"
	"time"
)

// Builder helps construct a query
type Builder struct {
	query        domain.Query
	lCondBuilder ports.LabelConditionBuilder
	fBuilder     ports.FilterBuilder
}

// NewQueryBuilder initializes a new QueryBuilder
func NewQueryBuilder(operation query_types.Operation, database, table string) *Builder {
	return &Builder{
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
func (qb *Builder) SelectFields(fields ...string) ports.QueryBuilder {
	qb.query.Fields = fields
	return qb
}

// SetPartition sets the partition (optional)
func (qb *Builder) SetPartition(partition string) ports.QueryBuilder {
	qb.query.Partition = &partition
	return qb
}

// Where adds a condition to the where clause
func (qb *Builder) Where(field string, operator query_types.QueryOperator, value interface{}) ports.QueryBuilder {
	qb.query.Conditions = append(qb.query.Conditions, query_types.Condition{
		Field:    field,
		Operator: operator,
		Value:    value,
	})
	return qb
}

// Limit sets the maximum number of records to return (optional)
func (qb *Builder) Limit(limit int) ports.QueryBuilder {
	qb.query.Limit = &limit
	return qb
}

// AggregateBy sets the aggregation dimension (optional)
func (qb *Builder) AggregateBy(dimension query_types.Dimension) ports.QueryBuilder {
	qb.query.AggregatedBy = &dimension
	return qb
}

// SetFormat sets the output format (json, csv, etc.)
func (qb *Builder) SetFormat(format query_types.Format) ports.QueryBuilder {
	qb.query.Format = format
	return qb
}

// SetTimeRange sets the time range for the query
func (qb *Builder) SetTimeRange(startTime, endTime time.Time) ports.QueryBuilder {
	qb.query.QueryTimeRange = &domain.QueryTimeRange{
		From: startTime.UTC(),
		To:   endTime.UTC(),
	}
	return qb
}

// Build returns the final Query object
func (qb *Builder) Build() (*domain.Query, error) {
	return &qb.query, nil
}
