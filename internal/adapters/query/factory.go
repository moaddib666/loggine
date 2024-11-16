package query

import (
	"LogDb/internal/domain/query_types"
	"LogDb/internal/ports"
)

var _ ports.QueryBuilderFactory = (*QueryBuilderFactory)(nil)

type QueryBuilderFactory struct{}

func (q *QueryBuilderFactory) NewQueryBuilder() ports.QueryBuilder {
	return NewQueryBuilder(query_types.Select, "default", "default")
}

// NewQueryBuilderFactory creates a new instance of QueryBuilderFactory
func NewQueryBuilderFactory() *QueryBuilderFactory {
	return &QueryBuilderFactory{}
}
