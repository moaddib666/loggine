package schema_deprecated

import (
	"LogDb/internal/ports"
)

type FieldFactory struct {
	name string
}

func (f *FieldFactory) FieldName() string {
	return f.name
}

func (f *FieldFactory) CreateField() ports.SchemaField {
	return NewStringField(f.name)
}
