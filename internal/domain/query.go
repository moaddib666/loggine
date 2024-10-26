package domain

import (
	"LogDb/internal/domain/query_types"
	"fmt"
	"strings"
	"time"
)

type QueryTimeRange struct {
	From time.Time
	To   time.Time
}

// Query represents the full structure of a query_types
type Query struct {
	*QueryTimeRange
	Operation    query_types.Operation   // select or scan
	Fields       []string                // list of fields to retrieve
	Database     string                  // database name
	Table        string                  // table name
	Partition    *string                 // optional partition (shard)
	Conditions   []query_types.Condition // where conditions
	Limit        *int                    // optional limit for results
	AggregatedBy *query_types.Dimension  // optional aggregation by a dimension (minute, hour, etc.)
	Format       query_types.Format      // output format
}

// String representation of the Query for debugging
func (q Query) String() string {
	queryStr := fmt.Sprintf("%s %s from %s.%s", q.Operation, strings.Join(q.Fields, ", "), q.Database, q.Table)

	if q.Partition != nil {
		queryStr += fmt.Sprintf(" partition %s", *q.Partition)
	}

	if len(q.Conditions) > 0 {
		conditionsStr := []string{}
		for _, cond := range q.Conditions {
			conditionsStr = append(conditionsStr, fmt.Sprintf("%s %s %v", cond.Field, cond.Operator, cond.Value))
		}
		queryStr += fmt.Sprintf(" where %s", strings.Join(conditionsStr, " and "))
	}

	if q.Limit != nil {
		queryStr += fmt.Sprintf(" limit %d", *q.Limit)
	}

	if q.AggregatedBy != nil {
		queryStr += fmt.Sprintf(" aggregated by %s", *q.AggregatedBy)
	}

	queryStr += fmt.Sprintf(" format %s", q.Format)
	return queryStr
}
