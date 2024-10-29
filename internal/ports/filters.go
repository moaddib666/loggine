package ports

import (
	"LogDb/internal/domain"
)

type TimeStampFilter interface {
	IsBefore(timestamp uint64) bool
	IsAfter(timestamp uint64) bool
}

type Filter interface {
	IsMatch(record *domain.LogRecord) bool
}

type FilterSet interface {
	TimeStampFilter
	Filter
}

type LabelCondition interface {
	IsFit(l *domain.Label) bool
}

type LabelConditionBuilder interface {
	And(condition LabelCondition) LabelConditionBuilder
	Or(condition LabelCondition) LabelConditionBuilder
	Not(condition LabelCondition) LabelConditionBuilder

	Eq(l *domain.Label) LabelConditionBuilder
	Neq(l *domain.Label) LabelConditionBuilder
	Gt(l *domain.Label) LabelConditionBuilder
	Gte(l *domain.Label) LabelConditionBuilder
	Lt(l *domain.Label) LabelConditionBuilder
	Lte(l *domain.Label) LabelConditionBuilder

	Build() (LabelCondition, error)
}

type FilterBuilder interface {
	And(filter Filter) FilterBuilder
	Or(filter Filter) FilterBuilder
	Not(filter Filter) FilterBuilder

	WithLabelCondition(idx int, schama uint64, condition LabelCondition) FilterBuilder
	WithTimeStampFilter(filter TimeStampFilter) FilterBuilder

	Contains([]byte) FilterBuilder
	NotContains([]byte) FilterBuilder
	OrContains([]byte) FilterBuilder

	Build() (FilterSet, error)
}

type FilterFactory interface {
	CreateFilterBuilder() FilterBuilder
}

type LabelConditionFactory interface {
	CreateConditionBuilder(schema uint64, label *domain.Label) LabelConditionBuilder
}
