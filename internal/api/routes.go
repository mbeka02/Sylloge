package api

import (
	"io"
	"net/http"
)

func routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, "{\"status\":\"ok\"}\n")
	})
	mux.HandleFunc("POST /logs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, "{\"error\":\"ingestion unavailable\"}\n")
	})
	return mux
}
