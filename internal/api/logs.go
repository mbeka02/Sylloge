package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mbeka02/sylloge/internal/event"
	"github.com/mbeka02/sylloge/internal/metrics"
)

const maxBodyBytes = 1_000_000

type validationError struct {
	Index int `json:"index"`
	event.FieldError
}

type acceptedResponse struct {
	Status  string `json:"status"`
	BatchID string `json:"batchId"`
}

func (h *handler) logs(w http.ResponseWriter, r *http.Request) {
	receivedAt := h.options.Now()
	sourceIP := ResolveClientIP(r, h.options.TrustedProxies)
	if allowed, retry := h.limiter.allow(sourceIP); !allowed {
		seconds := max(int64(1), int64((retry+time.Second-1)/time.Second))
		w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
		h.options.Metrics.Add(metrics.RateLimited, 1)
		h.respondWithError(w, http.StatusTooManyRequests, "rate limit exceeded")
		return
	}
	if h.options.Admitter == nil {
		h.respondWithError(w, http.StatusServiceUnavailable, "ingestion unavailable")
		return
	}
	select {
	case h.active <- struct{}{}:
		defer func() { <-h.active }()
	default:
		h.overloaded(w)
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		h.respondWithError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}
	if encoding := strings.TrimSpace(r.Header.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
		h.respondWithError(w, http.StatusUnsupportedMediaType, "unsupported content encoding")
		return
	}
	if r.ContentLength > maxBodyBytes {
		h.respondWithError(w, http.StatusRequestEntityTooLarge, "request body exceeds 1 MB")
		return
	}
	body := http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		var oversized *http.MaxBytesError
		if errors.As(err, &oversized) {
			h.respondWithError(w, http.StatusRequestEntityTooLarge, "request body exceeds 1 MB")
		} else {
			h.respondWithError(w, http.StatusBadRequest, "could not read request body")
		}
		return
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(data, &entries); err != nil {
		var syntax *json.SyntaxError
		if errors.As(err, &syntax) {
			h.respondWithError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON at byte %d", syntax.Offset))
		} else {
			h.respondWithError(w, http.StatusBadRequest, "body must be a JSON array of logs")
		}
		return
	}
	if len(entries) == 0 {
		h.respondWithError(w, http.StatusBadRequest, "body must be a nonempty JSON array of logs")
		return
	}
	batchID := event.NewID()
	metadata := event.Metadata{ReceivedAt: receivedAt, SourceIP: sourceIP, Environment: h.options.Environment}
	var valid []event.Raw
	var problems []validationError
	rejected := 0
	for index, entry := range entries {
		log, fields := event.Validate(entry)
		if len(fields) > 0 {
			rejected++
			for _, field := range fields {
				problems = append(problems, validationError{Index: index, FieldError: field})
			}
			continue
		}
		valid = append(valid, event.Raw{ID: event.NewID(), BatchID: batchID, Original: entry, Log: log, Metadata: metadata})
	}
	h.options.Metrics.Add(metrics.ValidationRejected, uint64(rejected))
	if len(valid) == 0 {
		h.respondWithJSON(w, http.StatusUnprocessableEntity, struct {
			Error    string            `json:"error"`
			Accepted int               `json:"accepted"`
			Rejected int               `json:"rejected"`
			Errors   []validationError `json:"errors"`
		}{Error: "validation failed", Rejected: rejected, Errors: problems})
		return
	}
	if err := h.options.Admitter.Admit(r.Context(), valid); err != nil {
		h.overloaded(w)
		return
	}
	accepted := acceptedResponse{Status: "accepted", BatchID: batchID}
	if rejected == 0 {
		h.respondWithJSON(w, http.StatusAccepted, accepted)
		return
	}
	h.respondWithJSON(w, http.StatusAccepted, struct {
		acceptedResponse
		Accepted int               `json:"accepted"`
		Rejected int               `json:"rejected"`
		Errors   []validationError `json:"errors"`
	}{acceptedResponse: accepted, Accepted: len(valid), Rejected: rejected, Errors: problems})
}

func (h *handler) overloaded(w http.ResponseWriter) {
	h.options.Metrics.Add(metrics.Overloaded, 1)
	h.respondWithError(w, http.StatusServiceUnavailable, "ingestion overloaded")
}
