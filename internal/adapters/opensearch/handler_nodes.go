package opensearch

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// GetNodes godoc
// @Summary Get Nodes Info
// @Description Get information about nodes
// @Tags Nodes
// @Produce json
// @Success 200 {object} NodesResponse
// @Router /_nodes [get]
func GetNodes(c *gin.Context) {
	response := NodesResponse{
		Nodes: map[string]NodeInfo{
			"ZQNc7huXTJmLdnG8RowLIw": {
				IP:      "172.18.0.2",
				Version: "2.17.1",
				HTTP: HTTPInfo{
					PublishAddress: "172.18.0.2:9200",
				},
			},
		},
	}
	c.JSON(http.StatusOK, response)
}
