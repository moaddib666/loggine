package label_conditions

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"bytes"
	"encoding/binary"
)

var _ ports.LabelCondition = new(Gt)

type Gt struct {
	expectedLabel *domain.Label
}

func (g *Gt) IsFit(l *domain.Label) bool {
	if g.expectedLabel.Type != l.Type {
		return false
	}

	switch l.Type {
	case domain.IntLabelType:
		var int1, int2 int64
		binary.Read(bytes.NewReader(g.expectedLabel.Value), binary.LittleEndian, &int1)
		binary.Read(bytes.NewReader(l.Value), binary.LittleEndian, &int2)
		return int1 > int2
	case domain.FloatLabelType:
		var float1, float2 float64
		binary.Read(bytes.NewReader(g.expectedLabel.Value), binary.LittleEndian, &float1)
		binary.Read(bytes.NewReader(l.Value), binary.LittleEndian, &float2)
		return float1 > float2
	}

	return false
}

// NewGt creates a new Gt label condition
func NewGt(label *domain.Label) *Gt {
	return &Gt{expectedLabel: label}
}
