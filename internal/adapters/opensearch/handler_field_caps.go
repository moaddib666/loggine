package opensearch

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// FieldCapsRequest represents the request body for field capabilities
type FieldCapsRequest struct {
	// Define fields if needed
}

// FieldCapsResponse represents the response structure for field capabilities
type FieldCapsResponse struct {
	Indices []string                   `json:"indices"`
	Fields  map[string]map[string]Caps `json:"fields"`
	// Additional fields as per the response
}

type Caps struct {
	Type         string `json:"type"`
	Searchable   bool   `json:"searchable"`
	Aggregatable bool   `json:"aggregatable"`
	// Additional capabilities
}

// PostFieldCaps godoc
// @Summary Field Capabilities
// @Description Retrieve field capabilities for indices
// @Tags Indices
// @Accept json
// @Produce json
// @Param index path string true "Index Name"
// @Success 200 {object} FieldCapsResponse
// @Router /{index}/_field_caps [post]
func PostFieldCaps(c *gin.Context) {
	index := c.Param("index")
	// For simplicity, return a mocked response
	response := FieldCapsResponse{
		Indices: []string{index},
		Fields: map[string]map[string]Caps{
			"field1": {
				"text": {
					Type:         "text",
					Searchable:   true,
					Aggregatable: false,
				},
			},
			"message": {
				"text": {
					Type:         "text",
					Searchable:   true,
					Aggregatable: false,
				},
			},
		},
	}
	c.JSON(http.StatusOK, response)
}
