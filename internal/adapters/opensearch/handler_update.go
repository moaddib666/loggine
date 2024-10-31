package opensearch

import (
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"
)

// UpdateRequest represents the update request body
type UpdateRequest struct {
	Doc    map[string]interface{} `json:"doc"`
	Script interface{}            `json:"script,omitempty"`
	Upsert interface{}            `json:"upsert,omitempty"`
	// Other fields as needed
}

// UpdateResponse represents the update response
type UpdateResponse struct {
	Result       string `json:"result"`
	_Shards      Shards `json:"_shards"`
	_Index       string `json:"_index"`
	_Id          string `json:"_id"`
	_Version     int    `json:"_version"`
	_SeqNo       int    `json:"_seq_no"`
	_PrimaryTerm int    `json:"_primary_term"`
	// Additional fields as per the response
}

// PostUpdate godoc
// @Summary Update a Document
// @Description Update a document in an index
// @Tags Update
// @Accept json
// @Produce json
// @Param index path string true "Index Name"
// @Param id path string true "Document ID"
// @Param body body UpdateRequest true "Update Body"
// @Success 200 {object} UpdateResponse
// @Router /{index}/_update/{id} [post]
func PostUpdate(c *gin.Context) {
	index := c.Param("index")
	idParam := c.Param("id")
	id, _ := url.QueryUnescape(idParam)

	var request UpdateRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// For simplicity, return a mocked response
	response := UpdateResponse{
		Result: "updated",
		_Shards: Shards{
			Total:      2,
			Successful: 1,
			Failed:     0,
		},
		_Index:       index,
		_Id:          id,
		_Version:     2,
		_SeqNo:       20,
		_PrimaryTerm: 1,
	}
	c.JSON(http.StatusOK, response)
}
