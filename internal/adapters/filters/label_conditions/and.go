package label_conditions

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
)

var _ ports.LabelCondition = new(And)

type And struct {
	Conditions []ports.LabelCondition
}

func (a *And) IsFit(l *domain.Label) bool {
	for _, condition := range a.Conditions {
		if !condition.IsFit(l) {
			return false
		}
	}
	return true
}

// NewAnd creates a new And label condition
func NewAnd(conditions ...ports.LabelCondition) *And {
	return &And{Conditions: conditions}
}
