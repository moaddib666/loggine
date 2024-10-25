package presenters

import (
	"LogDb/internal/domain"
	"encoding/binary"
	"fmt"
	"math"
)

type StringPresenter struct {
}

func (p *StringPresenter) Present(record *domain.LogRecord) string {
	ts := record.Timestamp.Format("2006-01-02 15:04:05")
	labels := ""
	for i, label := range record.Labels {
		switch label.Type {
		case domain.StringLabelType: // String
			labelValue := string(label.Value) // Convert bytes back to string
			labels += fmt.Sprintf("%dl: %s", i, labelValue)
		case domain.IntLabelType: // Integer
			labelValue := binary.LittleEndian.Uint64(label.Value) // Convert bytes back to int64
			labels += fmt.Sprintf("%dl: %d", i, labelValue)
		case domain.FloatLabelType: // Float
			labelValue := math.Float64frombits(binary.LittleEndian.Uint64(label.Value)) // Convert bytes back to float64
			labels += fmt.Sprintf("%dl: %f", i, labelValue)
		default:
			labels += fmt.Sprintf("%dl: Unknown label type %+v ", i, label.Value)
		}
	}
	message := string(record.Message)
	return fmt.Sprintf("%s - [%s] %s\n", ts, labels, message)
}

// NewStringPresenter creates a new StringPresenter
func NewStringPresenter() *StringPresenter {
	return &StringPresenter{}
}
