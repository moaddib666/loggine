package web_api

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

// InsertRecord godoc
// @Summary Insert a single log record
// @Description Insert a single log record into storage
// @Tags logs
// @Accept json
// @Produce json
// @Param body body StoreRequest true "Record to Insert"
// @Success 200 {object} StoreResult
// @Failure 400 {object} ErrorResponse
// @Router /api/v1/insert/record [post]
func (api *WebApi) InsertRecord(c *gin.Context) {
	var request StoreRequest
	var result StoreResult
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	err := api.storage.StoreLogRecord(api.recordTransformer.ToInternal(request.Record))
	if err != nil {
		result.Success = false
		result.Error = err.Error()
		c.JSON(http.StatusInternalServerError, result)
	}
	result.Success = true
	result.RecordInserted = 1
	c.JSON(http.StatusOK, result)
}
