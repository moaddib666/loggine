package web_api

import (
	"LogDb/internal/domain"
	"fmt"
)

type RecordTransformer struct {
}

func (rt *RecordTransformer) ToInternal(record *Record) *domain.LogRecord {
	var labels []domain.Label
	for _, value := range record.StringLabels {
		labels = append(labels, domain.Label{
			Type:  domain.StringLabelType,
			Size:  uint64(len(value)),
			Value: []byte(value),
		})
	}
	return &domain.LogRecord{
		Timestamp:     record.Timestamp,
		SchemaVersion: 0,
		Labels:        labels,
		Message:       []byte(record.Message),
	}
}

func (rt *RecordTransformer) ToExternal(record *domain.LogRecord) *Record {
	labels := make(map[string]string)
	for i, label := range record.Labels {
		// TODO resolve from schema
		labels[fmt.Sprintf("label_%d", i)] = string(label.Value)
	}
	return &Record{
		Timestamp:    record.Timestamp,
		Message:      string(record.Message),
		StringLabels: labels,
	}
}

// ToExternalBatch converts a slice of internal records to a slice of external records
func (rt *RecordTransformer) ToExternalBatch(records []*domain.LogRecord) []*Record {
	var externalRecords []*Record
	for _, record := range records {
		externalRecords = append(externalRecords, rt.ToExternal(record))
	}
	return externalRecords
}

// ToInternalBatch converts a slice of external records to a slice of internal records
func (rt *RecordTransformer) ToInternalBatch(records []*Record) []*domain.LogRecord {
	var internalRecords []*domain.LogRecord
	for _, record := range records {
		internalRecords = append(internalRecords, rt.ToInternal(record))
	}
	return internalRecords
}

var DefaultRecordTransformer = &RecordTransformer{}
