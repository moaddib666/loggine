package opensearch

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// GetDatasources godoc
// @Summary Get Datasources
// @Description Get datasources
// @Tags Query
// @Produce json
// @Success 200 {array} interface{}
// @Router /_plugins/_query/_datasources [get]
func GetDatasources(c *gin.Context) {
	c.JSON(http.StatusOK, []interface{}{})
}
