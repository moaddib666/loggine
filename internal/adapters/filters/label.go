package filters

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
)

type LabelFilter struct {
	labelIndex int
	schema     uint64
	condition  ports.LabelCondition
}

// IsMatch returns true if the LabelFilter's value is contained in the record's label.
func (c *LabelFilter) IsMatch(record *domain.LogRecord) bool {
	if c.schema != record.SchemaVersion {
		return false
	}
	if c.labelIndex >= len(record.Labels) {
		return false
	}
	return c.condition.IsFit(&record.Labels[c.labelIndex])
}

// NewLabel creates a new LabelFilter with the given value.
func NewLabel(labelIndex int, schema uint64, condition ports.LabelCondition) *LabelFilter {
	return &LabelFilter{labelIndex: labelIndex, schema: schema, condition: condition}
}
