package schema_deprecated_test

import (
	"LogDb/internal/adapters/schema_deprecated"
	"LogDb/internal/ports"
	"testing"
)

func TestSchemaStore_CreateAndGetSchema(t *testing.T) {
	store := schema_deprecated.NewSchemaStore()

	// Create some mock fields
	fields := []ports.SchemaField{
		&MockSchemaField{name: "FieldName", value: "John"},
		&MockSchemaField{name: "Age", value: "30"},
	}

	// Create a new schema_deprecated
	newSchema, err := store.CreateSchema(fields)
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}

	// Verify schema_deprecated version
	if newSchema.ID() != 1 {
		t.Errorf("Expected schema_deprecated version 1, but got %d", newSchema.ID())
	}

	// Retrieve the schema_deprecated by version
	retrievedSchema, err := store.GetSchema(1)
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}

	// Verify the schema_deprecated fields
	asMap := retrievedSchema.AsMap()
	if asMap["FieldName"] != "John" || asMap["Age"] != "30" {
		t.Errorf("Expected field values 'John' and '30', but got %v", asMap)
	}
}

func TestSchemaStore_GetSchema_NotFound(t *testing.T) {
	store := schema_deprecated.NewSchemaStore()

	// Try to get a non-existing schema_deprecated version
	_, err := store.GetSchema(999)
	if err == nil {
		t.Fatalf("Expected error for non-existing schema_deprecated, but got none")
	}
}
