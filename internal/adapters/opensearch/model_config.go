package opensearch

type ConfigResponse struct {
	Index       string       `json:"_index"`
	ID          string       `json:"_id"`
	Version     int          `json:"_version"`
	SeqNo       int          `json:"_seq_no"`
	PrimaryTerm int          `json:"_primary_term"`
	Found       bool         `json:"found"`
	Source      ConfigSource `json:"_source"`
}

type ConfigSource struct {
	Config           ConfigDetail      `json:"config"`
	Type             string            `json:"type"`
	References       []interface{}     `json:"references"`
	MigrationVersion map[string]string `json:"migrationVersion"`
	UpdatedAt        string            `json:"updated_at"`
}

type ConfigDetail struct {
	BuildNum     int    `json:"buildNum"`
	DefaultIndex string `json:"defaultIndex"`
}
