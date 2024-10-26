package transformers

import "LogDb/internal/domain"

func StringToLabel(value string) domain.Label {
	return domain.Label{Type: domain.StringLabelType, Value: []byte(value), Size: uint64(len(value))}
}

func IntToLabel(value int) domain.Label {
	return domain.Label{Type: domain.IntLabelType, Value: []byte{byte(value)}, Size: 1}
}

func FloatToLabel(value float64) domain.Label {
	return domain.Label{Type: domain.FloatLabelType, Value: []byte{byte(value)}, Size: 1}
}

func LabelToString(label domain.Label) string {
	return string(label.Value)
}

func LabelToInt(label domain.Label) int {
	return int(label.Value[0])
}

func LabelToFloat(label domain.Label) float64 {
	return float64(label.Value[0])
}
