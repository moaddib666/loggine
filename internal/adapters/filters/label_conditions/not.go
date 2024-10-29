package label_conditions

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
)

var _ ports.LabelCondition = new(Not)

type Not struct {
	Condition ports.LabelCondition
}

func (n *Not) IsFit(l *domain.Label) bool {
	return !n.Condition.IsFit(l)
}

// NewNot creates a new Not label condition
func NewNot(condition ports.LabelCondition) *Not {
	return &Not{Condition: condition}
}
