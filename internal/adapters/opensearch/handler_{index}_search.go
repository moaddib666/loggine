// handler_search.go
package opensearch

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

// NewSearchByIndexHandler creates a handler for searching flights
func NewSearchByIndexHandler(service SearchService) gin.HandlerFunc {
	return func(c *gin.Context) {
		index := c.Param("index")

		// Parse Query Parameters
		if err := parseAndValidateQueryParams(c); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Parse Request Body
		searchRequest, err := parseSearchRequestBody(c)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Perform the search using the injected service
		response, err := service.Search(index, searchRequest)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Search failed"})
			return
		}

		c.JSON(http.StatusOK, response)
	}
}

// Helper Functions

func parseAndValidateQueryParams(c *gin.Context) error {
	// Parse and validate query parameters
	ignoreUnavailableStr := c.Query("ignore_unavailable")
	trackTotalHitsStr := c.Query("track_total_hits")
	timeoutStr := c.Query("timeout")

	// Parse ignore_unavailable
	if ignoreUnavailableStr != "" {
		if _, err := strconv.ParseBool(ignoreUnavailableStr); err != nil {
			return err
		}
	}

	// Parse track_total_hits
	if trackTotalHitsStr != "" {
		if _, err := strconv.ParseBool(trackTotalHitsStr); err != nil {
			return err
		}
	}

	// Parse timeout (e.g., "30000ms")
	if timeoutStr != "" {
		if _, err := strconv.Atoi(strings.TrimSuffix(timeoutStr, "ms")); err != nil {
			return err
		}
	}

	// Add more validation as needed

	return nil
}

func parseSearchRequestBody(c *gin.Context) (*SearchRequest, error) {
	var request SearchRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		return nil, err
	}
	return &request, nil
}
