package api_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mbeka02/sylloge/internal/api"
)

func TestStartupRoutes(t *testing.T) {
	server := api.NewServer(":3000")
	for _, tc := range []struct {
		method, path string
		status       int
		body         string
	}{
		{http.MethodGet, "/healthz", 200, "{\"status\":\"ok\"}\n"},
		{http.MethodPost, "/healthz", 405, ""},
		{http.MethodPost, "/logs", 503, "{\"error\":\"ingestion unavailable\"}\n"},
		{http.MethodGet, "/logs", 405, ""},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			server.Handler.ServeHTTP(recorder, httptest.NewRequest(tc.method, tc.path, nil))
			response := recorder.Result()
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != tc.status || (tc.body != "" && string(body) != tc.body) {
				t.Fatalf("got %d %q; want %d %q", response.StatusCode, body, tc.status, tc.body)
			}
			if tc.body != "" && response.Header.Get("Content-Type") != "application/json" {
				t.Fatal("expected JSON response")
			}
			if tc.status == http.StatusMethodNotAllowed && response.Header.Get("Allow") == "" {
				t.Fatal("method rejection must advertise allowed methods")
			}
		})
	}
}
