package filters

import "LogDb/internal/ports"

type factory struct{}

// CreateFilterBuilder creates a new filter builder.
func (f *factory) CreateFilterBuilder() ports.FilterBuilder {
	return &GenericFilterBuilder{}
}

var Factory ports.FilterFactory = new(factory)
