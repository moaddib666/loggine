package domain

import "time"

type QueryResult struct {
	Query   *Query
	Report  *QueryReport
	Records []*LogRecord
}

// NewQueryResult creates a new QueryResult instance.
func NewQueryResult(q *Query) *QueryResult {
	return &QueryResult{
		Query: q,
		Report: &QueryReport{
			Count:       0,
			Hits:        0,
			ElapsedTime: 0,
		},
	}
}

// Miss increments the count of missed records in the query_types result.
func (qr *QueryResult) Miss() {
	qr.Report.Count++
}

// Hit increments the count of matched records in the query_types result.
func (qr *QueryResult) Hit(record *LogRecord) {
	qr.Report.Count++
	qr.Report.Hits++
	qr.Records = append(qr.Records, record)
}

// SpentTime sets the time spent on the query_types.
func (qr *QueryResult) SpentTime(elapsedTime time.Duration) {
	qr.Report.ElapsedTime = elapsedTime
}
