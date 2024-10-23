package schema_deprecated

import (
	"LogDb/internal/ports"
	"fmt"
	"sync"
)

// SchemaStoreImpl is an in-memory implementation of the SchemaStore interface.
type SchemaStoreImpl struct {
	schemas map[uint64]*Schema // Stores schemas by version
	mu      sync.RWMutex       // Ensures thread-safe access to the schemas map
	nextID  uint64             // Tracks the next schema_deprecated version to be assigned
}

// NewSchemaStore creates a new SchemaStoreImpl.
func NewSchemaStore() *SchemaStoreImpl {
	return &SchemaStoreImpl{
		schemas: make(map[uint64]*Schema),
		nextID:  1, // Start versioning from 1
	}
}

// GetSchema retrieves a schema_deprecated by its version.
func (store *SchemaStoreImpl) GetSchema(version uint64) (ports.Schema, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	schema, ok := store.schemas[version]
	if !ok {
		return nil, fmt.Errorf("schema_deprecated with version %d not found", version)
	}
	return schema, nil
}

// CreateSchema creates a new schema_deprecated with the given fields and assigns a unique version.
func (store *SchemaStoreImpl) CreateSchema(fields []ports.SchemaField) (ports.Schema, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	// Assign a new version ID
	version := store.nextID
	store.nextID++

	// Create a new schema_deprecated with the assigned version and the provided fields
	newSchema := &Schema{
		id:          version,
		fieldsCount: len(fields),
		fields:      fields,
	}

	// Store the new schema_deprecated in the map
	store.schemas[version] = newSchema

	return newSchema, nil
}

// Storage is the global storage for schema_deprecated
var Storage ports.SchemaStore = NewSchemaStore()
