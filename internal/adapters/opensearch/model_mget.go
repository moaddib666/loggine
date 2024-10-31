package opensearch

type MGetRequest struct {
	Docs []DocRequest `json:"docs"`
}

type DocRequest struct {
	ID    string `json:"_id"`
	Index string `json:"_index"`
}

type MGetResponse struct {
	Docs []DocResponse `json:"docs"`
}

type DocResponse struct {
	Index       string    `json:"_index"`
	ID          string    `json:"_id"`
	Version     int       `json:"_version"`
	SeqNo       int       `json:"_seq_no"`
	PrimaryTerm int       `json:"_primary_term"`
	Found       bool      `json:"found"`
	Source      DocSource `json:"_source"`
}

type DocSource struct {
	IndexPattern     IndexPattern      `json:"index-pattern"`
	Type             string            `json:"type"`
	References       []interface{}     `json:"references"`
	MigrationVersion map[string]string `json:"migrationVersion"`
	UpdatedAt        string            `json:"updated_at"`
}

type IndexPattern struct {
	Title          string `json:"title"`
	TimeFieldName  string `json:"timeFieldName"`
	Fields         string `json:"fields"`
	FieldFormatMap string `json:"fieldFormatMap"`
}
