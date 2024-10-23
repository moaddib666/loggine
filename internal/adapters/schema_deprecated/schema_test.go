package schema_deprecated_test

import (
	"LogDb/internal/adapters/schema_deprecated"
	"LogDb/internal/ports"
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

// MockSchemaField is a mock implementation of ports.SchemaField for testing.
type MockSchemaField struct {
	name  string
	value string
}

func (m *MockSchemaField) SlotSize() uint64 {
	return uint64(4 + len(m.value)) // 4 bytes for size + actual string length
}

func (m *MockSchemaField) Name() string {
	return m.name
}

func (m *MockSchemaField) Value() string {
	return m.value
}

func (m *MockSchemaField) ReadValue(reader io.Reader) (int, error) {
	// Mock reading the value (we simulate reading by ignoring the input)
	var size int32
	if err := binary.Read(reader, binary.LittleEndian, &size); err != nil {
		return 0, err
	}
	buffer := make([]byte, size)
	n, err := reader.Read(buffer)
	if err != nil {
		return n, err
	}
	m.value = string(buffer)
	return int(4 + size), nil // 4 bytes for size + actual string length
}

func (m *MockSchemaField) WriteValue(writer io.Writer) (int, error) {
	// Mock writing the value (we simulate writing by outputting the string length and string)
	size := int32(len(m.value))
	if err := binary.Write(writer, binary.LittleEndian, size); err != nil {
		return 0, err
	}
	n, err := writer.Write([]byte(m.value))
	if err != nil {
		return 0, err
	}
	return int(4 + n), nil // 4 bytes for size + actual string length
}

func TestSchema_WriteRead(t *testing.T) {
	// Setup the schema_deprecated with fields
	fields := []ports.SchemaField{
		&MockSchemaField{name: "FieldName", value: "John"},
		&MockSchemaField{name: "Age", value: "30"},
	}

	s := schema_deprecated.NewSchema(12345, fields)

	// Create a buffer to write the schema_deprecated to
	var buf bytes.Buffer

	// Write the schema_deprecated
	bytesWritten, err := s.Write(&buf)
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}
	expectedBytesWritten := 8 + 4 + len("John") + 4 + len("30") // 8 bytes for schema_deprecated ID, 4 for each size field
	if bytesWritten != expectedBytesWritten {
		t.Fatalf("Expected %d bytes written, but got %d", expectedBytesWritten, bytesWritten)
	}

	// Create a new schema_deprecated for reading
	newSchema := schema_deprecated.NewSchema(12345, []ports.SchemaField{
		&MockSchemaField{name: "FieldName"},
		&MockSchemaField{name: "Age"},
	})

	// Read the schema_deprecated from the buffer
	bytesRead, err := newSchema.Read(&buf)
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}
	if bytesRead != bytesWritten {
		t.Fatalf("Expected %d bytes read, but got %d", bytesWritten, bytesRead)
	}

	// Verify the field values are correct
	asMap := newSchema.AsMap()
	if asMap["FieldName"] != "John" || asMap["Age"] != "30" {
		t.Errorf("Expected field values to be 'John' and '30', but got %v", asMap)
	}
}

func TestSchema_ID(t *testing.T) {
	s := schema_deprecated.NewSchema(12345, nil)
	if s.ID() != 12345 {
		t.Errorf("Expected schema_deprecated ID to be 12345, but got %d", s.ID())
	}
}

func TestSchema_AsMap(t *testing.T) {
	fields := []ports.SchemaField{
		&MockSchemaField{name: "City", value: "New York"},
		&MockSchemaField{name: "Country", value: "USA"},
	}

	s := schema_deprecated.NewSchema(1, fields)

	// Verify AsMap output
	asMap := s.AsMap()
	if asMap["City"] != "New York" || asMap["Country"] != "USA" {
		t.Errorf("Expected field values 'New York' and 'USA', but got %v", asMap)
	}
}
