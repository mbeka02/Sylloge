package api

import (
	"encoding/json"
	"io"
	"net/http"
)

type errorResponse struct {
	Error string `json:"error"`
}

// Encode before committing headers so an encoding failure can still return 500.
// Once writing starts, a connection error is logged rather than writing a second
// response; the client may already have received part of the first response.
func (h *handler) respondWithJSON(w http.ResponseWriter, status int, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		h.logger.Error("encode HTTP response", "error", err)
		status = http.StatusInternalServerError
		data = []byte(`{"error":"internal server error"}`)
	}
	data = append(data, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	n, err := w.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		h.logger.Error("write HTTP response", "error", err)
	}
}

func (h *handler) respondWithError(w http.ResponseWriter, status int, message string) {
	h.respondWithJSON(w, status, errorResponse{Error: message})
}
