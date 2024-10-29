package label_conditions

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
)

type factory struct{}

// CreateConditionBuilder creates a new label condition builder.
func (f *factory) CreateConditionBuilder(schema uint64, label *domain.Label) ports.LabelConditionBuilder {
	return NewLabelConditionBuilder(schema, label)
}

var Factory ports.LabelConditionFactory = new(factory)
