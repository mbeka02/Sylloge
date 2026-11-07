package api_test

import (
	"net/http/httptest"
	"testing"

	"github.com/mbeka02/sylloge/internal/api"
)

func TestReadinessReflectsPersistenceHealthAndPreservesLiveness(t *testing.T) {
	ready := false
	handler, err := api.NewHandler(api.Options{Ready: func() bool { return ready }})
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []bool{false, true, false} {
		ready = state
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/readyz", nil))
		expected := 503
		if state {
			expected = 200
		}
		if response.Code != expected {
			t.Fatalf("ready=%v: status=%d", state, response.Code)
		}
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/healthz", nil))
		if response.Code != 200 {
			t.Fatal("storage failure changed liveness")
		}
	}
}
