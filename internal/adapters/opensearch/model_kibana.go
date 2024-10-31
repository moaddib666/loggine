package opensearch

// KibanaIndexResponse represents the response structure for the /.kibana endpoint
type KibanaIndexResponse struct {
	KibanaIndex KibanaIndex `json:".kibana_1"`
}

// KibanaIndex represents the structure of the .kibana index
type KibanaIndex struct {
	Aliases  map[string]interface{} `json:"aliases"`
	Mappings interface{}            `json:"mappings"`
	Settings interface{}            `json:"settings"`
}

// KibanaCountRequest represents the request body for the _count endpoint
type KibanaCountRequest struct {
	Query interface{} `json:"query"`
}

// KibanaCountResponse represents the response structure for the _count endpoint
type KibanaCountResponse struct {
	Count  int        `json:"count"`
	Shards ShardsInfo `json:"_shards"`
}
