package label_conditions

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"errors"
)

var _ ports.LabelConditionBuilder = new(SingleLabelConditionBuilder)

// SingleLabelConditionBuilder builds label conditions using a fluent interface.
// It allows combining various conditions (Eq, Gt, Lt, etc.) with logical operations like AND, OR, and NOT.
type SingleLabelConditionBuilder struct {
	schema    uint64
	label     *domain.Label
	condition ports.LabelCondition
}

// And adds a new condition that must be true along with the current condition.
// It combines the current condition with the new condition using logical AND.
func (s *SingleLabelConditionBuilder) And(condition ports.LabelCondition) ports.LabelConditionBuilder {
	s.condition = NewAnd(s.condition, condition)
	return s
}

// Or adds a new condition that can be true in addition to the current condition.
// It combines the current condition with the new condition using logical OR.
func (s *SingleLabelConditionBuilder) Or(condition ports.LabelCondition) ports.LabelConditionBuilder {
	s.condition = NewOr(s.condition, condition)
	return s
}

// Not negates the specified condition.
// It adds a condition that must be false for the combined condition to be true.
func (s *SingleLabelConditionBuilder) Not(condition ports.LabelCondition) ports.LabelConditionBuilder {
	s.condition = NewNot(condition)
	return s
}

// Eq sets a condition where the label must be equal to the specified label.
func (s *SingleLabelConditionBuilder) Eq(l *domain.Label) ports.LabelConditionBuilder {
	s.condition = NewEq(l)
	return s
}

// Neq sets a condition where the label must not be equal to the specified label.
// It is effectively a NOT condition applied to the Eq condition.
func (s *SingleLabelConditionBuilder) Neq(l *domain.Label) ports.LabelConditionBuilder {
	s.condition = NewNot(NewEq(l))
	return s
}

// Gt sets a condition where the label must be greater than the specified label.
func (s *SingleLabelConditionBuilder) Gt(l *domain.Label) ports.LabelConditionBuilder {
	s.condition = NewGt(l)
	return s
}

// Gte sets a condition where the label must be greater than or equal to the specified label.
// It combines Gt and Eq conditions using logical OR.
func (s *SingleLabelConditionBuilder) Gte(l *domain.Label) ports.LabelConditionBuilder {
	s.condition = NewOr(NewGt(l), NewEq(l))
	return s
}

// Lt sets a condition where the label must be less than the specified label.
func (s *SingleLabelConditionBuilder) Lt(l *domain.Label) ports.LabelConditionBuilder {
	s.condition = NewLt(l)
	return s
}

// Lte sets a condition where the label must be less than or equal to the specified label.
// It combines Lt and Eq conditions using logical OR.
func (s *SingleLabelConditionBuilder) Lte(l *domain.Label) ports.LabelConditionBuilder {
	s.condition = NewOr(NewLt(l), NewEq(l))
	return s
}

// Build finalizes the condition-building process and returns the constructed condition.
// If no conditions have been added, an error is returned.
func (s *SingleLabelConditionBuilder) Build() (ports.LabelCondition, error) {
	if s.condition == nil {
		return nil, errors.New("no conditions to build")
	}
	return s.condition, nil
}

// NewLabelConditionBuilder creates a new instance of SingleLabelConditionBuilder.
// It initializes the builder with the provided schema and label.
func NewLabelConditionBuilder(schema uint64, label *domain.Label) *SingleLabelConditionBuilder {
	return &SingleLabelConditionBuilder{
		schema: schema,
		label:  label,
	}
}
