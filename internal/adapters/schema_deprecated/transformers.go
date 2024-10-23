package schema_deprecated

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
)

type LabelsTransformer interface {
	ToFields(labels map[string]string) []ports.SchemaField
	FromFields(fields []ports.SchemaField, record domain.LogRecord) error
}

//**
// take  domain.LogRecord.labels => map[string]string
// use   schema_deprecated.Repository.GetSchema(domain.LogRecord.SchemaVersion) => schema_deprecated.Schema
// transform domain.LogRecord.labels => []ports.SchemaField
// Write []ports.SchemaField to io.Writer one by one

type LogRecordTransformer struct{}

func (t *LogRecordTransformer) ToFields(record domain.LogRecord, schema ports.Schema) []ports.SchemaField {
	var fields []ports.SchemaField
	for _, fieldFactory := range schema.Fields() {
		fieldValue, _ := record.Labels[fieldFactory.FieldName()]
		fields = append(fields, fieldFactory.CreateField(fieldValue))
	}
	return fields
}

func (t *LogRecordTransformer) FromFields(fields []ports.SchemaField, record domain.LogRecord) error {
	labels := make(map[string]string)
	for _, field := range fields {
		labels[field.Name()] = field.Value()
	}
	record.Labels = labels
	return nil
}
