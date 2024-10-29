package label_conditions

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
)

var _ ports.LabelCondition = new(Or)

type Or struct {
	Conditions []ports.LabelCondition
}

func (o *Or) IsFit(l *domain.Label) bool {
	for _, condition := range o.Conditions {
		if condition.IsFit(l) {
			return true
		}
	}
	return false
}

// NewOr creates a new Or label condition
func NewOr(conditions ...ports.LabelCondition) *Or {
	return &Or{Conditions: conditions}
}
