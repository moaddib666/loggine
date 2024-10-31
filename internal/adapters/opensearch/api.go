package opensearch

import "github.com/gin-gonic/gin"

func NewOpenSearchApi() *gin.Engine {

	router := gin.Default()

	// System Routes
	router.GET("/_nodes", GetNodes)

	// Data Routes
	router.POST("/_mget", PostMGet)

	// Kibana Routes
	kibana := router.Group("/.kibana")
	{
		kibana.GET("", GetKibana)
		kibana.GET("/_doc/:id", GetConfig)
		kibana.POST("/_search", SearchKibana)
		kibana.POST("/_count", PostKibanaCount)
	}

	// Plugins Routes
	plugins := router.Group("/_plugins")
	{
		security := plugins.Group("/_security")
		{
			security.GET("/api/account", GetAccount)
		}
		query := plugins.Group("/_query")
		{
			query.GET("/_datasources", GetDatasources)
		}
	}

	// Indices Routes
	indices := router.Group("/:index")
	{
		indices.POST("/_search", NewSearchByIndexHandler(&MockSearchService{}))
		indices.POST("/_field_caps", PostFieldCaps)
		indices.POST("/_update/:id", PostUpdate)
	}

	// Cat Routes
	cat := router.Group("/_cat")
	{
		cat.GET("/plugins", GetCatPlugins)
	}

	return router
}
