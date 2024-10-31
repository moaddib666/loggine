package opensearch

// NodesResponse represents the response structure for the /_nodes endpoint
type NodesResponse struct {
	Nodes map[string]NodeInfo `json:"nodes"`
}

type NodeInfo struct {
	IP      string   `json:"ip"`
	Version string   `json:"version"`
	HTTP    HTTPInfo `json:"http"`
}

type HTTPInfo struct {
	PublishAddress string `json:"publish_address"`
}
