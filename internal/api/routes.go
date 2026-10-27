package api

import (
	"log/slog"
	"net/http"
)

type handler struct {
	logger *slog.Logger
}

func routes() http.Handler {
	h := &handler{logger: slog.Default()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		h.respondWithJSON(w, http.StatusOK, struct {
			Status string `json:"status"`
		}{Status: "ok"})
	})
	mux.HandleFunc("POST /logs", func(w http.ResponseWriter, r *http.Request) {
		h.respondWithError(w, http.StatusServiceUnavailable, "ingestion unavailable")
	})
	return mux
}
