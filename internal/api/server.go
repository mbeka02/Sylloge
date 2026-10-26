package api

import "net/http"

// NewServer assembles the HTTP routes. Startup applies the configured timeouts
// before serving; ingestion stays unavailable until the pipeline is connected.
func NewServer(addr string) *http.Server {
	return &http.Server{
		Handler: routes(),
		Addr:    addr,
	}
}
