package ports

import "LogDb/internal/domain"

type QueryBuilder interface {
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
