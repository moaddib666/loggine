package main

import (
	"LogDb/internal/adapters/opensearch"
	"bytes"
	"fmt"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
)

var bindAddress string
var proxyAddress string

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
func init() {
	bindAddress = getEnv("PROXY_PORT", ":8080")
	proxyAddress = getEnv("TARGET_URL", "http://opensearch:9200")
}

func main() {
	// create a proxy server and start it make handler that wold log the request and response and then pass it to the proxy server if not in registered routes in gin
	api := opensearch.NewOpenSearchApi() // gin.Default()

	// Swagger endpoint (optional)
	// router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// NoRoute handler for unregistered routes
	api.NoRoute(ReverseProxyHandler)

	if err := api.Run(bindAddress); err != nil {
		log.Fatalf("Failed to start OpenSearch API: %v", err)
	}
}

// ReverseProxyHandler handles unregistered routes and proxies them to OpenSearch
func ReverseProxyHandler(c *gin.Context) {
	targetURL, err := url.Parse(proxyAddress)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Invalid proxy address"})
		return
	}

	proxy := httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = targetURL.Scheme
			req.URL.Host = targetURL.Host
			req.Host = targetURL.Host
			req.URL.Path = c.Request.URL.Path
			req.URL.RawQuery = c.Request.URL.RawQuery
		},
		ModifyResponse: func(resp *http.Response) error {
			return logResponse(resp)
		},
	}

	// Log the incoming request
	logRequest(c.Request)

	proxy.ServeHTTP(c.Writer, c.Request)
}

func logRequest(req *http.Request) {
	dump, err := httputil.DumpRequest(req, true)
	if err != nil {
		fmt.Printf("Request dump error: %v\n", err)
		return
	}
	fmt.Printf("REQUEST:\n%s\n", dump)
}

func logResponse(resp *http.Response) error {
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Response read error: %v\n", err)
		return err
	}
	resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	fmt.Printf("RESPONSE:\nStatus: %s\nHeaders: %v\nBody: %s\n", resp.Status, resp.Header, string(bodyBytes))
	return nil
}
