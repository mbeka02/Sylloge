package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/netip"
	"time"

	"github.com/mbeka02/sylloge/internal/event"
	"github.com/mbeka02/sylloge/internal/metrics"
)

type Admitter interface {
	Admit(context.Context, []event.Raw) error
}

type Options struct {
	Admitter          Admitter
	Metrics           *metrics.Registry
	Logger            *slog.Logger
	Environment       string
	TrustedProxies    []netip.Prefix
	MaxActiveRequests int
	RateLimit         int
	MaxIPEntries      int
	IPIdleTimeout     time.Duration
	Now               func() time.Time
}

type handler struct {
	logger  *slog.Logger
	options Options
	active  chan struct{}
	limiter *ipLimiter
}

// NewHandler connects the HTTP seam to bounded admission. A nil Admitter keeps
// ingestion unavailable; production does not accept records without a real sink.
func NewHandler(options Options) (http.Handler, error) {
	if options.RateLimit == 0 {
		options.RateLimit = 1000
	}
	if options.RateLimit < 0 {
		return nil, errors.New("rate limit must be positive")
	}
	if options.MaxIPEntries == 0 {
		options.MaxIPEntries = 10000
	}
	if options.IPIdleTimeout == 0 {
		options.IPIdleTimeout = 60 * time.Second
	}
	if options.MaxIPEntries < 0 || options.IPIdleTimeout < time.Second {
		return nil, errors.New("limiter requires a positive IP bound and an idle timeout of at least one second")
	}
	if options.MaxActiveRequests == 0 {
		options.MaxActiveRequests = 128
	}
	if options.MaxActiveRequests < 0 {
		return nil, errors.New("maximum active requests must be positive")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.Metrics == nil {
		options.Metrics = new(metrics.Registry)
	}
	if options.Environment == "" {
		options.Environment = "production"
	}
	options.TrustedProxies = append([]netip.Prefix(nil), options.TrustedProxies...)
	for _, prefix := range options.TrustedProxies {
		if !prefix.IsValid() {
			return nil, errors.New("invalid trusted proxy prefix")
		}
	}
	h := &handler{
		logger: options.Logger, options: options, active: make(chan struct{}, options.MaxActiveRequests),
		limiter: &ipLimiter{buckets: make(map[string]bucket), capacity: options.RateLimit, maxIPs: options.MaxIPEntries, idleTimeout: options.IPIdleTimeout, now: options.Now},
	}
	return routes(h), nil
}

func routes(h *handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		h.respondWithJSON(w, http.StatusOK, struct {
			Status string `json:"status"`
		}{Status: "ok"})
	})
	mux.HandleFunc("POST /logs", h.logs)
	mux.HandleFunc("/logs", h.methodNotAllowed("POST"))
	mux.HandleFunc("/healthz", h.methodNotAllowed("GET, HEAD"))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { h.respondWithError(w, http.StatusNotFound, "not found") })
	return mux
}

func (h *handler) methodNotAllowed(allow string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Allow", allow)
		h.respondWithError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}
