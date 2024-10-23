package transformers

import "LogDb/internal/domain"

func StringToLabel(value string) domain.Label {
	return domain.Label{Type: domain.StringLabelType, Value: []byte(value)}
}

func IntToLabel(value int) domain.Label {
	return domain.Label{Type: domain.IntLabelType, Value: []byte{byte(value)}}
}

func FloatToLabel(value float64) domain.Label {
	return domain.Label{Type: domain.FloatLabelType, Value: []byte{byte(value)}}
}

func LabelToString(label domain.Label) string {
	return string(label.Value)
}
