package opensearch

import (
	"fmt"
	"time"
)

// SearchResult is a struct that represents the result of a search query
type SearchResult struct {
	Took         int                           `json:"took"`
	TimedOut     bool                          `json:"timed_out"`
	Shards       *Shards                       `json:"_shards"`
	Hits         *Hits                         `json:"hits"`
	Aggregations map[string]*AggregationResult `json:"aggregations"`
}

// SearchResult creates a new SearchResponse struct
func NewSearchResult(took int, timedOut bool, shards *Shards, hits *Hits) *SearchResult {
	return &SearchResult{
		Took:     took,
		TimedOut: timedOut,
		Shards:   shards,
		Hits:     hits,
	}

}

// Shards is a struct that represents a shard
type Shards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
}

type Hits struct {
	Total    *TotalValue `json:"total"`
	MaxScore float64     `json:"max_score"`
	Hits     []*Hit      `json:"hits"`
}

// AddHit adds a hit to the Hits struct
func (h *Hits) AddHit(hit *Hit) {
	h.Hits = append(h.Hits, hit)
	if hit.Score > h.MaxScore {
		h.MaxScore = hit.Score
	}
	if h.Total == nil {
		h.Total = &TotalValue{}
	}
}

// NewHits creates a new Hits struct
func NewHits() *Hits {
	return &Hits{
		Hits: make([]*Hit, 0),
		Total: &TotalValue{
			Value: 0,
		},
	}
}

type TotalValue struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type Hit struct {
	Index  string                 `json:"_index,omitempty"`
	Type   string                 `json:"_type,omitempty"`
	ID     string                 `json:"_id,omitempty"`
	Score  float64                `json:"_score,omitempty"`
	Source map[string]interface{} `json:"_source,omitempty"`
	Fields interface{}            `json:"fields,omitempty"`
	Sort   HitSort                `json:"sort,omitempty"`
}

// NewHit creates a new Hit struct
func NewHit(index, id string, source map[string]interface{}, ts time.Time) *Hit {
	hit := &Hit{
		Index:  index,
		ID:     id,
		Source: source,
		Sort: []int{
			int(ts.Unix()),
		},
	}
	return hit
}

// IsBeforeHit returns true if hit is before the given hit
func (h *Hit) IsBeforeHit(hit *Hit) bool {
	if len(h.Sort) == 0 || len(hit.Sort) == 0 {
		return false
	}
	return h.Sort[0] < hit.Sort[0]
}

// IsAfterHit returns true if hit is after the given hit
func (h *Hit) IsAfterHit(hit *Hit) bool {
	if len(h.Sort) == 0 || len(hit.Sort) == 0 {
		return false
	}
	return h.Sort[0] > hit.Sort[0]
}

type HitSort []int

type AggregationResult struct {
	Buckets []*Bucket `json:"buckets"`
}

func NewAggregationResult() *AggregationResult {
	return &AggregationResult{
		Buckets: make([]*Bucket, 0),
	}
}

func (ar *AggregationResult) AddBucket(bucket *Bucket) {
	ar.Buckets = append(ar.Buckets, bucket)
}

func (ar *AggregationResult) DocsCount() int {
	count := 0
	for _, bucket := range ar.Buckets {
		count += bucket.DocCount
	}
	return count
}

type Bucket struct {
	KeyAsString string `json:"key_as_string"`
	Key         int64  `json:"key"`
	DocCount    int    `json:"doc_count"`
}

func NewBucket() *Bucket {
	return &Bucket{}
}

func (b *Bucket) HasDocs() bool {
	return b.DocCount > 0
}

func (b *Bucket) AddDoc() {
	b.DocCount++
}

func (b *Bucket) FromTime(t time.Time) {
	b.Key = t.UnixMilli()
	b.KeyAsString = t.Format(time.RFC3339)
}

func (b *Bucket) String() string {
	return fmt.Sprintf("Bucket: %s, %d", b.KeyAsString, b.DocCount)
}
