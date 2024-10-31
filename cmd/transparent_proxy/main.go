package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
)

func main() {
	targetURL, err := url.Parse(getEnv("TARGET_URL", "http://opensearch:9200"))
	if err != nil {
		log.Fatalf("Failed to parse target URL: %v", err)
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = targetURL.Scheme
			req.URL.Host = targetURL.Host
			req.Host = targetURL.Host
		},
		ModifyResponse: logResponse,
	}

	http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		logRequest(req)
		proxy.ServeHTTP(w, req)
	})

	port := getEnv("PROXY_PORT", "8080")
	log.Printf("Starting proxy on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func logRequest(req *http.Request) {
	dump, err := httputil.DumpRequest(req, true)
	if err != nil {
		log.Printf("Request dump error: %v", err)
		return
	}
	fmt.Printf("REQUEST:\n%s\n", dump)
}

func logResponse(resp *http.Response) error {
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Response read error: %v", err)
		return err
	}
	resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	fmt.Printf("RESPONSE:\nStatus: %s\nHeaders: %v\nBody: %s\n", resp.Status, resp.Header, string(bodyBytes))
	return nil
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
