package label_conditions

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"bytes"
	"encoding/binary"
)

var _ ports.LabelCondition = new(Eq)

type Eq struct {
	expectedLabel *domain.Label
}

func (e *Eq) IsFit(l *domain.Label) bool {
	if l.Type != e.expectedLabel.Type {
		return false
	}

	switch l.Type {
	case domain.StringLabelType:
		return bytes.Equal(e.expectedLabel.Value, l.Value)
	case domain.IntLabelType:
		var int1, int2 int64
		binary.Read(bytes.NewReader(e.expectedLabel.Value), binary.LittleEndian, &int1)
		binary.Read(bytes.NewReader(l.Value), binary.LittleEndian, &int2)
		return int1 == int2
	case domain.FloatLabelType:
		var float1, float2 float64
		binary.Read(bytes.NewReader(e.expectedLabel.Value), binary.LittleEndian, &float1)
		binary.Read(bytes.NewReader(l.Value), binary.LittleEndian, &float2)
		return float1 == float2
	}

	return false
}

// NewEq creates a new Eq label condition
func NewEq(label *domain.Label) *Eq {
	return &Eq{expectedLabel: label}
}
