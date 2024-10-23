package schema_deprecated

import (
	"LogDb/internal/ports"
)

// Schema represents a schema_deprecated with a unique ID and a set of fields.
type Schema struct {
	id          uint64
	fieldsCount int
	fields      []ports.FieldFactory
}

// ID returns the schema_deprecated's ID.
func (s *Schema) ID() uint64 {
	return s.id
}

// Fields returns the schema_deprecated's fields.
func (s *Schema) Fields() []ports.FieldFactory {
	return s.fields
}

// NewSchema creates a new schema_deprecated with the given ID and fields.
func NewSchema(id uint64, fields []ports.FieldFactory) ports.Schema {
	return &Schema{id: id, fieldsCount: len(fields), fields: fields}
}
