package opensearch

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// SearchKibana godoc
// @Summary Search Kibana Index
// @Description Search documents in .kibana index
// @Tags Search
// @Accept json
// @Produce json
// @Param data body interface{} true "Search Body"
// @Success 200 {object} SearchResponse
// @Router /.kibana/_search [post]
func SearchKibana(c *gin.Context) {
	response := SearchResponse{
		Took:     19,
		TimedOut: false,
		Shards: ShardsInfo{
			Total:      1,
			Successful: 1,
			Skipped:    0,
			Failed:     0,
		},
		Hits: HitsInfo{
			Total:    1,
			MaxScore: 1.0,
			Hits: []HitInfoItem{
				{
					Index: ".kibana_1",
					ID:    "index-pattern:d3d7af60-4c81-11e8-b3d7-01146121b73d",
					//SeqNo:       19,
					//PrimaryTerm: 1,1
					Score: 1.0,
					Source: HitSource{
						MigrationVersion: map[string]string{
							"index-pattern": "7.6.0",
						},
						IndexPattern: IndexPatternTitle{
							Title: "loggine",
						},
						References: []interface{}{},
						UpdatedAt:  "2024-10-31T21:04:40.651Z",
						Type:       "index-pattern",
					},
				},
			},
		},
	}

	c.JSON(http.StatusOK, response)
}
