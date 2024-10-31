package opensearch

import (
	"errors"
	"time"
)

// SearchRequest represents the overall structure of an OpenSearch request
type SearchRequest struct {
	Sort           []map[string]*SortOrder       `json:"sort"`
	Size           int                           `json:"size"`
	Version        bool                          `json:"version"`
	StoredFields   []string                      `json:"stored_fields"`
	ScriptFields   map[string]interface{}        `json:"script_fields"`
	DocvalueFields []*DocvalueField              `json:"docvalue_fields"`
	Source         *SourceSetting                `json:"_source"`
	Query          *Query                        `json:"query"`
	Highlight      *Highlight                    `json:"highlight"`
	Aggregations   map[string]*SearchAggregation `json:"aggs"`
}

func (r *SearchRequest) GetRange() *Range {
	var rg *Range
	for _, filter := range r.Query.Bool.Filter {
		if filter.Range != nil {
			rg = filter.Range
			break
		}
	}
	return rg
}

func (r *SearchRequest) GetSortOrder() (*SortOrder, error) {
	if len(r.DocvalueFields) != 1 {
		return nil, errors.New("only one docvalue field is supported")
	}
	docValueField := r.DocvalueFields[0].Field
	return r.Sort[0][docValueField], nil
}

// SearchAggregation represents the overall structure of an OpenSearch Aggregation
type SearchAggregation struct {
	DateHistogram *DateHistogram `json:"date_histogram"`
}

// DateHistogram represents the overall structure of an OpenSearch date_histogram Aggregation
type DateHistogram struct {
	Field            string `json:"field"`
	Interval         string `json:"fixed_interval"`
	CalendarInterval string `json:"calendar_interval"`
	TimeZone         string `json:"time_zone"`
	MinDocCount      int    `json:"min_doc_count"`
}

// SortOrder represents sorting options
type SortOrder struct {
	Order string `json:"order"`
}

// DocvalueField represents docvalue fields settings
type DocvalueField struct {
	Field  string `json:"field"`
	Format string `json:"format"`
}

// SourceSetting represents settings for _source field
type SourceSetting struct {
	Excludes []string `json:"excludes"`
}

// Query represents the query structure
type Query struct {
	Bool *BoolQuery `json:"bool,omitempty"`
	//MultiMatch *MultiMatch `json:"multi_match"`
}

type Filter struct {
	Bool        *BoolFilter `json:"bool,omitempty"`
	MultiMatch  *MultiMatch `json:"multi_match"`
	MatchAll    *MatchAll   `json:"match_all,omitempty"`
	MatchPhrase MatchPhrase `json:"match_phrase,omitempty"`
	Range       *Range      `json:"range,omitempty"`
}

type MatchPhrase map[string]string

type BoolFilter struct {
	Filter  []*Filter `json:"filter,omitempty"`
	MustNot *Filter   `json:"must_not,omitempty"`
}

type MatchAll struct {
}

type Range struct {
	DateTime *DateTimeRange `json:"datetime,omitempty"`
}

type DateTimeRange struct {
	GTE    time.Time `json:"gte"`
	LTE    time.Time `json:"lte"`
	Format string    `json:"format"`
}

// BoolQuery represents a boolean query
type BoolQuery struct {
	Must    []*Filter `json:"must,omitempty"`
	Filter  []*Filter `json:"filter,omitempty"`
	Should  []*Filter `json:"should,omitempty"`
	MustNot []*Filter `json:"must_not,omitempty"`
}

// MatchQuery represents a match query
type MatchQuery struct {
	MultiMatch *MultiMatch `json:"multi_match"`
}

// MultiMatch represents a multi-match query
type MultiMatch struct {
	Type    string `json:"type"`
	Query   string `json:"query"`
	Lenient bool   `json:"lenient"`
}

// Highlight represents highlight settings
type Highlight struct {
	PreTags      []string               `json:"pre_tags"`
	PostTags     []string               `json:"post_tags"`
	Fields       map[string]interface{} `json:"fields"`
	FragmentSize int                    `json:"fragment_size"`
}
