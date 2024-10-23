package ports

import (
	"io"
)

type SchemaStore interface {
	GetSchema(version uint64) (Schema, error)
	CreateSchema(fields []SchemaField) (Schema, error)
}

type Schema interface {
	ID() uint64
	//Write(io.Writer) (int, error)
	//Read(io.Reader) (int, error)
	Fields() []FieldFactory
	//AsMap() map[string]string
}

type SchemaField interface {
	Name() string
	ReadValue(io.Reader) (int, error)
	WriteValue(io.Writer) (int, error)
	SlotSize() uint64
	Value() string
}

type FieldFactory interface {
	FieldName() string
	CreateField(interface{}) SchemaField
}
