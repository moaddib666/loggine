package web_api

import (
	"LogDb/internal/domain/query_types"
	"github.com/gin-gonic/gin"
	"net/http"
)

// SearchRecords godoc
// @Summary Search for log records
// @Description Search log records based on criteria
// @Tags logs
// @Accept json
// @Produce json
// @Param body body SearchRequest true "Search Criteria"
// @Success 200 {object} SearchResult
// @Failure 400 {object} ErrorResponse
// @Router /api/v1/search/records [post]
func (api *WebApi) SearchRecords(c *gin.Context) {
	var request SearchRequest
	result := NewSearchResult()
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	qb := api.queryBuilder.NewQueryBuilder()
	if request.ShardingKey != "" {
		qb.Where("message", query_types.Contains, request.MessageMustContain)
	}
	qb.SetTimeRange(request.FromTime, request.ToTime)
	qb.Limit(request.Limit)
	query, err := qb.Build()

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	preparedQuery, err := api.queryProcessor.PrepareQuery(query)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	queryResult, err := api.storage.Query(preparedQuery)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	result.Records = api.recordTransformer.ToExternalBatch(queryResult.Records)
	result.Report.TotalRecords = queryResult.Report.Hits
	result.Report.ScannedRecords = queryResult.Report.ScannedItems
	result.Report.TimeTaken = queryResult.Report.ElapsedTime.Seconds()
	c.JSON(http.StatusOK, result)
	return
}
