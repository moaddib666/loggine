package schema_deprecated

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestStringField_SetValue(t *testing.T) {
	field := &StringField{name: "test_field"}

	// Test setting a regular string value
	value := "Hello, World!"
	field.SetValue(value)

	if field.Value() != value {
		t.Errorf("Expected value %s, but got %s", value, field.Value())
	}

	if field.actualSize != uint64(len(value)) {
		t.Errorf("Expected actualSize %d, but got %d", len(value), field.actualSize)
	}

	// Test setting a value that exceeds StringFieldSize
	longValue := "A really long string that should be truncated because it exceeds the fixed buffer size"
	for len(longValue)-5 < StringFieldSize {
		longValue += longValue
	}
	field.SetValue(longValue)

	expectedValue := longValue[:StringFieldSize]
	if field.Value() != expectedValue {
		t.Errorf("Expected value %s, but got %s", expectedValue, field.Value())
	}

	if field.actualSize != uint64(StringFieldSize) {
		t.Errorf("Expected actualSize %d, but got %d", StringFieldSize, field.actualSize)
	}
}

func TestStringField_WriteValue(t *testing.T) {
	field := &StringField{name: "test_field"}
	value := "Test Value"
	field.SetValue(value)

	// Create a buffer to write to
	var buf bytes.Buffer
	bytesWritten, err := field.WriteValue(&buf)
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}

	expectedBytes := int(8 + field.actualSize) // 8 bytes for the actualSize and the size of the value
	if bytesWritten != expectedBytes {
		t.Errorf("Expected %d bytes written, but got %d", expectedBytes, bytesWritten)
	}

	// Verify the buffer contents
	var size uint64
	err = binary.Read(&buf, binary.LittleEndian, &size)
	if err != nil {
		t.Fatalf("Error reading actualSize: %v", err)
	}

	if size != field.actualSize {
		t.Errorf("Expected actualSize %d, but got %d", field.actualSize, size)
	}

	readValue := make([]byte, size)
	n, err := buf.Read(readValue)
	if err != nil {
		t.Fatalf("Error reading value: %v", err)
	}

	if n != int(size) {
		t.Errorf("Expected to read %d bytes, but got %d", size, n)
	}

	if string(readValue) != value {
		t.Errorf("Expected value %s, but got %s", value, string(readValue))
	}
}

func TestStringField_ReadValue(t *testing.T) {
	field := &StringField{name: "test_field"}
	value := "Test Value"
	field.SetValue(value)

	// Create a buffer with the serialized data
	var buf bytes.Buffer
	_, err := field.WriteValue(&buf)
	if err != nil {
		t.Fatalf("Expected no error writing value, but got %v", err)
	}

	// Create a new field to read the value into
	newField := &StringField{name: "test_field"}
	bytesRead, err := newField.ReadValue(&buf)
	if err != nil {
		t.Fatalf("Expected no error reading value, but got %v", err)
	}

	expectedBytes := int(8 + field.actualSize) // 8 bytes for the actualSize and the size of the value
	if bytesRead != expectedBytes {
		t.Errorf("Expected %d bytes read, but got %d", expectedBytes, bytesRead)
	}

	// Check if the value was read correctly
	if newField.Value() != value {
		t.Errorf("Expected value %s, but got %s", value, newField.Value())
	}

	if newField.actualSize != field.actualSize {
		t.Errorf("Expected actualSize %d, but got %d", field.actualSize, newField.actualSize)
	}
}

func TestStringField_SlotSize(t *testing.T) {
	field := &StringField{name: "test_field"}

	expectedSlotSize := uint64(StringFieldSize)
	if field.SlotSize() != expectedSlotSize {
		t.Errorf("Expected slot size %d, but got %d", expectedSlotSize, field.SlotSize())
	}
}
