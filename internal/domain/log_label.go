package domain

// StringLabelType, IntLabelType, FloatLabelType are constants that represent the type of the label value
const StringLabelType uint8 = 0
const IntLabelType uint8 = 1
const FloatLabelType uint8 = 2

type Label struct {
	Type  uint8  // Label type: 0 = string, 1 = int, 2 = float
	Size  uint64 // Size of the label value in bytes
	Value []byte // The raw value of the label in a byte slice
}
