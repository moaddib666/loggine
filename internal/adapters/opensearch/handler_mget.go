package opensearch

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// PostMGet godoc
// @Summary Multi Get Documents
// @Description Get multiple documents
// @Tags MGet
// @Accept json
// @Produce json
// @Param data body MGetRequest true "Request Body"
// @Success 200 {object} MGetResponse
// @Router /_mget [post]
func PostMGet(c *gin.Context) {
	var request MGetRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	response := MGetResponse{
		Docs: []DocResponse{
			{
				Index:       ".kibana_1",
				ID:          "index-pattern:d3d7af60-4c81-11e8-b3d7-01146121b73d",
				Version:     1,
				SeqNo:       19,
				PrimaryTerm: 1,
				Found:       true,
				Source: DocSource{
					IndexPattern: IndexPattern{
						Title:          "loggine",
						TimeFieldName:  "timestamp",
						Fields:         "[]", // Simplified for brevity
						FieldFormatMap: "{\"hour_of_day\":{\"id\":\"number\",\"params\":{\"pattern\":\"00\"}},\"AvgTicketPrice\":{\"id\":\"number\",\"params\":{\"pattern\":\"$0,0.[00]\"}}}",
					},
					Type:       "index-pattern",
					References: []interface{}{},
					MigrationVersion: map[string]string{
						"index-pattern": "7.6.0",
					},
					UpdatedAt: "2024-10-31T21:04:40.651Z",
				},
			},
		},
	}

	c.JSON(http.StatusOK, response)
}
