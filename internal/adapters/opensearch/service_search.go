// service_search.go
package opensearch

type SearchService interface {
	Search(index string, request *SearchRequest) (*SearchResult, error)
}

type MockSearchService struct{}

func (s *MockSearchService) Search(index string, request *SearchRequest) (*SearchResult, error) {
	// Construct mock hits
	realHits := []*Hit{
		{
			Index: index,
			ID:    "1",
			Score: 1.0,
			Source: map[string]interface{}{
				"timestamp": "2024-10-31T21:04:51",
				"FlightNum": "Y868NAZ",
				"message":   "This is a test message",
			},
		},
		{
			Index: index,
			ID:    "2",
			Score: 1.0,
			Source: map[string]interface{}{
				"timestamp": "2024-10-31T21:04:51",
				"FlightNum": "X123ABC",
				"message":   "This is a test message",
				// Other fields as needed
			},
		},
	}

	hits := &Hits{
		Total:    &TotalValue{Value: 2, Relation: "eq"},
		MaxScore: 1.0,
		Hits:     realHits,
	}

	shards := &Shards{
		Total:      1,
		Successful: 1,
		Skipped:    0,
		Failed:     0,
	}

	response := NewSearchResult(1, false, shards, hits)
	return response, nil
}
