package opensearch

import (
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"
)

// GetConfig godoc
// @Summary Get Config Document
// @Description Get configuration document from .kibana index
// @Tags Config
// @Produce json
// @Param id path string true "Config ID"
// @Success 200 {object} ConfigResponse
// @Router /.kibana/_doc/{id} [get]
func GetConfig(c *gin.Context) {
	idParam := c.Param("id")
	id, _ := url.QueryUnescape(idParam)

	response := ConfigResponse{
		Index:       ".kibana_1",
		ID:          id,
		Version:     2,
		SeqNo:       23,
		PrimaryTerm: 1,
		Found:       true,
		Source: ConfigSource{
			Config: ConfigDetail{
				BuildNum:     7969,
				DefaultIndex: "d3d7af60-4c81-11e8-b3d7-01146121b73d",
			},
			Type:       "config",
			References: []interface{}{},
			MigrationVersion: map[string]string{
				"config": "7.9.0",
			},
			UpdatedAt: "2024-10-31T21:04:42.194Z",
		},
	}
	c.JSON(http.StatusOK, response)
}
