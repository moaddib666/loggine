package opensearch

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// GetCatPlugins godoc
// @Summary Get Installed Plugins
// @Description Get information about installed plugins in JSON format
// @Tags Cat
// @Produce json
// @Success 200 {array} CatPlugin
// @Router /_cat/plugins [get]
func GetCatPlugins(c *gin.Context) {
	// Mock response based on the logs
	response := []CatPlugin{
		{
			Name:      "6f5ca42905e1",
			Component: "opensearch-alerting",
			Version:   "2.17.1.0",
		},
		// Include other plugins as per your logs
		// For brevity, only one is shown here
	}
	c.JSON(http.StatusOK, response)
}
