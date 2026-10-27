package api_test

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mbeka02/sylloge/internal/api"
)

type failedConnection struct {
	headers http.Header
	status  int
}

func (w *failedConnection) Header() http.Header       { return w.headers }
func (w *failedConnection) WriteHeader(status int)    { w.status = status }
func (w *failedConnection) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestResponseConnectionFailuresAreLogged(t *testing.T) {
	var logs bytes.Buffer
	h, err := api.NewHandler(api.Options{Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	w := &failedConnection{headers: make(http.Header)}
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/logs", nil))
	if w.status != 503 || !strings.Contains(logs.String(), "write HTTP response") {
		t.Fatal("connection error was hidden")
	}
}
