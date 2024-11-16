package web_api

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

// InsertRecords godoc
// @Summary Insert multiple log records
// @Description Insert multiple log records into storage
// @Tags logs
// @Accept json
// @Produce json
// @Param body body StoreBatchRequest true "Records to Insert"
// @Success 200 {object} StoreResult
// @Failure 400 {object} ErrorResponse
// @Router /api/v1/insert/records [post]
func (api *WebApi) InsertRecords(c *gin.Context) {
	var request StoreBatchRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// not implemented
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
	return
}
