package opensearch

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// GetAccount godoc
// @Summary Get Account Info
// @Description Get account information
// @Tags Security
// @Produce json
// @Failure 400 {object} ErrorResponse
// @Router /_plugins/_security/api/account [get]
func GetAccount(c *gin.Context) {
	c.JSON(http.StatusBadRequest, ErrorResponse{
		Error: "no handler found for uri [/_plugins/_security/api/account] and method [GET]",
	})
}
