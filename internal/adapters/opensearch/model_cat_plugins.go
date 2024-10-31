package opensearch

// CatPlugin represents a plugin in the _cat/plugins response
type CatPlugin struct {
	Name      string `json:"name"`
	Component string `json:"component"`
	Version   string `json:"version"`
}
