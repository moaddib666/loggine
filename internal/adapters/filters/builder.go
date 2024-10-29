package filters

import "LogDb/internal/ports"

var _ ports.FilterBuilder = new(GenericFilterBuilder)

type GenericFilterBuilder struct {
	filter          ports.Filter
	timeStampFilter ports.TimeStampFilter
}

func (g *GenericFilterBuilder) WithTimeStampFilter(filter ports.TimeStampFilter) ports.FilterBuilder {
	g.timeStampFilter = filter
	return g
}

func (g *GenericFilterBuilder) And(filter ports.Filter) ports.FilterBuilder {
	if g.filter == nil {
		g.filter = filter
		return g
	}
	g.filter = NewAnd(g.filter, filter)
	return g
}

func (g *GenericFilterBuilder) Or(filter ports.Filter) ports.FilterBuilder {
	if g.filter == nil {
		g.filter = filter
		return g
	}
	g.filter = NewOr(g.filter, filter)
	return g
}

func (g *GenericFilterBuilder) Not(filter ports.Filter) ports.FilterBuilder {
	if g.filter == nil {
		g.filter = NewNot(filter)
		return g
	}
	g.filter = NewNot(filter)
	return g
}

func (g *GenericFilterBuilder) WithLabelCondition(idx int, schema uint64, condition ports.LabelCondition) ports.FilterBuilder {
	g.filter = NewAnd(g.filter, NewLabel(idx, schema, condition))
	return g
}

func (g *GenericFilterBuilder) Contains(bytes []byte) ports.FilterBuilder {
	return g.And(NewContains(bytes))
}

func (g *GenericFilterBuilder) NotContains(bytes []byte) ports.FilterBuilder {
	return g.Not(NewContains(bytes))
}

func (g *GenericFilterBuilder) OrContains(bytes []byte) ports.FilterBuilder {
	return g.Or(NewContains(bytes))
}

func (g *GenericFilterBuilder) Build() (ports.FilterSet, error) {
	if g.filter == nil {
		g.filter = NotSet
	}
	if g.timeStampFilter == nil {
		g.timeStampFilter = AllTimeRange
	}
	return &FilterSet{filter: g.filter, timeStampFilter: g.timeStampFilter}, nil
}
