package opensearch

type SearchResponse struct {
	Took     int        `json:"took"`
	TimedOut bool       `json:"timed_out"`
	Shards   ShardsInfo `json:"_shards"`
	Hits     HitsInfo   `json:"hits"`
}

type ShardsInfo struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
}

type HitsInfo struct {
	Total    int           `json:"total"`
	MaxScore float64       `json:"max_score"`
	Hits     []HitInfoItem `json:"hits"`
}

type HitInfoItem struct {
	Index       string    `json:"_index"`
	ID          string    `json:"_id"`
	SeqNo       int       `json:"_seq_no"`
	PrimaryTerm int       `json:"_primary_term"`
	Score       float64   `json:"_score"`
	Source      HitSource `json:"_source"`
}

type HitSource struct {
	MigrationVersion map[string]string `json:"migrationVersion"`
	IndexPattern     IndexPatternTitle `json:"index-pattern"`
	References       []interface{}     `json:"references"`
	UpdatedAt        string            `json:"updated_at"`
	Type             string            `json:"type"`
}

type IndexPatternTitle struct {
	Title string `json:"title"`
}
