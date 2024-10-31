package opensearch

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// GetKibana godoc
// @Summary Get Kibana Index Information
// @Description Get information about the .kibana index
// @Tags Kibana
// @Produce json
// @Success 200 {object} KibanaIndexResponse
// @Router /.kibana [get]
func GetKibana(c *gin.Context) {
	// Mock response based on the logs
	response := KibanaIndexResponse{
		KibanaIndex: KibanaIndex{
			Aliases: map[string]interface{}{
				".kibana": map[string]interface{}{},
			},
			Mappings: map[string]interface{}{
				// Simplified mappings for brevity
				"dynamic": "strict",
				"_meta":   map[string]interface{}{
					// Populate with necessary data
				},
				"properties": map[string]interface{}{
					// Populate with necessary data
				},
			},
			Settings: map[string]interface{}{
				"index": map[string]interface{}{
					"number_of_shards":   "1",
					"number_of_replicas": "0",
					// Additional settings
				},
			},
		},
	}
	c.JSON(http.StatusOK, response)
}

// PostKibanaCount godoc
// @Summary Get the document count in the .kibana index
// @Description Get the count of documents in the .kibana index based on a query
// @Tags Kibana
// @Accept json
// @Produce json
// @Param body body KibanaCountRequest true "Query to count documents"
// @Success 200 {object} KibanaCountResponse
// @Router /.kibana/_count [post]
func PostKibanaCount(c *gin.Context) {
	var request KibanaCountRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// Mock response based on the logs
	response := KibanaCountResponse{
		Count: 0,
		Shards: ShardsInfo{
			Total:      1,
			Successful: 1,
			Skipped:    0,
			Failed:     0,
		},
	}
	c.JSON(http.StatusOK, response)
}
