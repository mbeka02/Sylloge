package api

import "net/http"

// NewServer assembles the HTTP routes. Startup applies the configured timeouts
// before serving; ingestion stays unavailable until the pipeline is connected.
func NewServer(addr string, options Options) (*http.Server, error) {
	handler, err := NewHandler(options)
	if err != nil {
		return nil, err
	}
	return &http.Server{
		Handler: handler,
		Addr:    addr,
	}, nil
}
