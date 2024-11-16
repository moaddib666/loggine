package web_api

import (
	_ "LogDb/internal/adapters/api/web_api/docs"
	"LogDb/internal/ports"
	"github.com/gin-gonic/gin"
)

// WebApi represents the API with storage dependency
type WebApi struct {
	storage           ports.DataStorage
	queryBuilder      ports.QueryBuilderFactory
	queryProcessor    ports.QueryPreparer
	recordTransformer *RecordTransformer
}

// NewWebApi creates a new instance of WebApi with injected storage dependency
func NewWebApi(storage ports.DataStorage, qb ports.QueryBuilderFactory, qp ports.QueryPreparer) *WebApi {
	return &WebApi{
		storage:           storage,
		queryBuilder:      qb,
		queryProcessor:    qp,
		recordTransformer: DefaultRecordTransformer,
	}
}

// RegisterRoutes initializes all the routes and their handlers
func (api *WebApi) RegisterRoutes(router *gin.Engine) {
	v1 := router.Group("/api/v1")
	{
		v1.POST("/search/records", api.SearchRecords)
		v1.POST("/insert/record", api.InsertRecord)
		v1.POST("/insert/records", api.InsertRecords)
	}
}
