package query

import (
	"LogDb/internal/domain"
	"LogDb/internal/domain/query_types"
	"LogDb/internal/ports"
	"errors"
	"time"
)

type Prepared struct {
	from      uint64
	to        uint64
	r         *domain.QueryResult
	f         ports.FilterSet
	startTime time.Time
	e         error
}

// NewPreparedQuery creates a new PreparedQuery.
func NewPreparedQuery(q *domain.Query, f ports.FilterSet) *Prepared {
	return &Prepared{
		r:    domain.NewQueryResult(q),
		from: uint64(q.From.UTC().Unix()),
		to:   uint64(q.To.UTC().Unix()),
		f:    f,
	}
}

func (p *Prepared) FromDateTime() uint64 {
	return p.from
}

func (p *Prepared) ToDateTime() uint64 {
	return p.to
}

func (p *Prepared) Begin() {
	p.startTime = time.Now()
}

func (p *Prepared) Skip() {
	p.r.Miss()
}

func (p *Prepared) Next(record *domain.LogRecord) error {
	if p.f.IsMatch(record) {
		p.r.Hit(record)
	} else {
		p.r.Miss()
	}
	return nil
}

func (p *Prepared) SetError(err error) {
	if p.e != nil {
		// merge errors
		p.e = errors.Join(p.e, err)
		return
	}
	p.e = err
}

func (p *Prepared) Error() error {
	return p.e
}
func (p *Prepared) End() {
	p.r.SpentTime(time.Since(p.startTime))
}

func (p *Prepared) Result() (*domain.QueryResult, error) {
	return p.r, nil
}

// QueryPreparer
type Preparer struct {
	filterBuilderFactory  ports.FilterFactory
	labelConditionFactory ports.LabelConditionFactory
}

func (p *Preparer) PrepareQuery(q *domain.Query) (ports.PreparedQuery, error) {
	// Create Label Conditions
	//_ = p.labelConditionFactory.CreateConditionBuilder(query.Schema, query.Label)

	// Create Filters
	fb := p.filterBuilderFactory.CreateFilterBuilder()

	for _, cond := range q.Conditions {
		if cond.Field == "message" {
			if cond.Operator == query_types.Contains {
				fb.Contains([]byte(cond.Value.(string)))
			}
		}

	}

	filterSet, err := fb.Build()
	if err != nil {
		return nil, err
	}
	return NewPreparedQuery(q, filterSet), nil
}

// NewPreparer creates a new Preparer.
func NewPreparer(filterBuilder ports.FilterFactory, labelConditionBuilder ports.LabelConditionFactory) *Preparer {
	return &Preparer{
		filterBuilderFactory:  filterBuilder,
		labelConditionFactory: labelConditionBuilder,
	}
}
