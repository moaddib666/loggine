package ports

import "LogDb/internal/domain"

type LogRecordPresenter interface {
	Present(record *domain.LogRecord) string
}

type QueryResultPresenter interface {
	Present(result *domain.QueryResult) string
}
