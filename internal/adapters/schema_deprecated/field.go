package schema_deprecated

import (
	"encoding/binary"
	"io"
)

const StringFieldSize = 256

// StringField represents a schema_deprecated field that maps string values to unique uint64 IDs.
type StringField struct {
	value      [StringFieldSize]byte // Fixed-size byte array for storing the value
	name       string                // Field name
	actualSize uint64                // Actual size of the string stored
}

// Name returns the field name.
func (s *StringField) Name() string {
	return s.name
}

// ReadValue reads a string value from the reader.
func (s *StringField) ReadValue(reader io.Reader) (int, error) {
	// Read the size of the value (actualSize)
	if err := binary.Read(reader, binary.LittleEndian, &s.actualSize); err != nil {
		return 0, err
	}

	// Ensure actualSize does not exceed the fixed buffer size
	if s.actualSize > StringFieldSize {
		return 0, io.ErrUnexpectedEOF
	}

	// Read the value into the buffer
	n, err := reader.Read(s.value[:s.actualSize])
	if err != nil {
		return n, err
	}

	// Return the number of bytes read
	return n + 8, nil // 8 bytes for the actualSize field
}

// WriteValue writes the string value to the writer.
func (s *StringField) WriteValue(writer io.Writer) (int, error) {
	// Write the actual size of the value
	if err := binary.Write(writer, binary.LittleEndian, s.actualSize); err != nil {
		return 0, err
	}

	// Write the actual string value up to the actual size
	n, err := writer.Write(s.value[:s.actualSize])
	if err != nil {
		return n, err
	}

	// Return the number of bytes written
	return n + 8, nil // 8 bytes for the actualSize field
}

// SlotSize returns the maximum size of VarChar elements in the schema_deprecated.
func (s *StringField) SlotSize() uint64 {
	return uint64(StringFieldSize)
}

// Value returns the current string value stored in the field.
func (s *StringField) Value() string {
	return string(s.value[:s.actualSize])
}

// SetValue sets the string value in the field (truncated if it exceeds StringFieldSize).
func (s *StringField) SetValue(val string) {
	// Convert string to bytes and store it, truncated to StringFieldSize
	byteVal := []byte(val)
	if len(byteVal) > StringFieldSize {
		byteVal = byteVal[:StringFieldSize]
	}
	copy(s.value[:], byteVal)

	// Update the actualSize
	s.actualSize = uint64(len(byteVal))
}

// NewStringField creates a new StringField with the given name.
func NewStringField(name string) *StringField {
	return &StringField{name: name}
}
