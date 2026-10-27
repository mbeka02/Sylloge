package api_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mbeka02/sylloge/internal/api"
)

func TestRateLimitCountsRequestsRatherThanLogRecords(t *testing.T) {
	h, p, counters := newHandlerWithOptions(t, 10, api.Options{RateLimit: 2})
	body := "[" + strings.TrimSuffix(strings.Repeat(validRecord+",", 5), ",") + "]"
	for range 2 {
		if w := requestLogs(h, body, "application/json"); w.Code != 202 {
			t.Fatalf("request budget was charged per record: %d %s", w.Code, w.Body)
		}
	}
	w := requestLogs(h, "["+validRecord+"]", "application/json")
	if w.Code != 429 || w.Header().Get("Retry-After") != "1" || strings.TrimSpace(w.Body.String()) != `{"error":"rate limit exceeded"}` {
		t.Fatalf("incorrect rate rejection: %d %s", w.Code, w.Body)
	}
	if p.Pending() != 10 || counters.Snapshot().RateLimited != 1 {
		t.Fatal("rate-limited request entered the pipeline or was not counted")
	}
}

func TestRateLimitRefillsAtOneSecondBoundary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, _, _ := newHandlerWithOptions(t, 10, api.Options{RateLimit: 1})
		post := func(want int) {
			t.Helper()
			if w := requestLogs(h, "["+validRecord+"]", "application/json"); w.Code != want {
				t.Fatalf("got %d; want %d", w.Code, want)
			}
		}
		post(202)
		post(429)
		time.Sleep(999 * time.Millisecond)
		post(429)
		time.Sleep(time.Millisecond)
		post(202)
		post(429)
	})
}

func TestRateLimitUsesIndependentBucketsPerIP(t *testing.T) {
	h, _, _ := newHandlerWithOptions(t, 10, api.Options{RateLimit: 1})
	for _, tc := range []struct {
		peer string
		want int
	}{
		{"192.0.2.1:1000", 202}, {"192.0.2.1:2000", 429}, {"192.0.2.2:1000", 202},
	} {
		r := httptest.NewRequest(http.MethodPost, "/logs", strings.NewReader("["+validRecord+"]"))
		r.Header.Set("Content-Type", "application/json")
		r.RemoteAddr = tc.peer
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("peer %s: got %d; want %d", tc.peer, w.Code, tc.want)
		}
	}
}

func TestRateLimitBoundsIPStateWithoutResettingActiveBudgets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, _, _ := newHandlerWithOptions(t, 10, api.Options{RateLimit: 1, MaxIPEntries: 1, IPIdleTimeout: time.Second})
		post := func(peer string, want int) {
			t.Helper()
			r := httptest.NewRequest(http.MethodPost, "/logs", strings.NewReader("["+validRecord+"]"))
			r.Header.Set("Content-Type", "application/json")
			r.RemoteAddr = peer
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != want {
				t.Fatalf("peer %s: got %d; want %d", peer, w.Code, want)
			}
		}
		post("192.0.2.1:1000", 202)
		post("192.0.2.2:1000", 429)
		post("192.0.2.1:1000", 429)
		time.Sleep(750 * time.Millisecond)
		post("192.0.2.1:1000", 429)
		time.Sleep(250 * time.Millisecond)
		post("192.0.2.2:1000", 429) // Rejected attempts keep the first IP active.
		post("192.0.2.1:1000", 202)
		time.Sleep(time.Second)
		post("192.0.2.2:1000", 202) // The first bucket is now idle and reclaimable.
	})
}

func TestConcurrentRequestsCannotOverspendTokens(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, p, counters := newHandlerWithOptions(t, 200, api.Options{RateLimit: 32, MaxActiveRequests: 128})
		var accepted, limited atomic.Int64
		var requests sync.WaitGroup
		for range 100 {
			requests.Go(func() {
				w := requestLogs(h, "["+validRecord+"]", "application/json")
				switch w.Code {
				case 202:
					accepted.Add(1)
				case 429:
					limited.Add(1)
				default:
					t.Errorf("unexpected concurrent status: %d", w.Code)
				}
			})
		}
		requests.Wait()
		if accepted.Load() != 32 || limited.Load() != 68 || p.Pending() != 32 || counters.Snapshot().RateLimited != 68 {
			t.Fatalf("token accounting: accepted=%d limited=%d pending=%d", accepted.Load(), limited.Load(), p.Pending())
		}
	})
}

func TestForwardedHeadersCannotBypassUntrustedPeerBucket(t *testing.T) {
	for _, trusted := range []bool{false, true} {
		t.Run(map[bool]string{false: "untrusted", true: "trusted"}[trusted], func(t *testing.T) {
			options := api.Options{RateLimit: 1}
			if trusted {
				options.TrustedProxies = []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
			}
			h, _, _ := newHandlerWithOptions(t, 10, options)
			for i, forwarded := range []string{"192.0.2.1", "192.0.2.2"} {
				r := httptest.NewRequest(http.MethodPost, "/logs", strings.NewReader("["+validRecord+"]"))
				r.Header.Set("Content-Type", "application/json")
				r.Header.Set("X-Forwarded-For", forwarded)
				r.RemoteAddr = "10.0.0.2:1000"
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				want := 202
				if i == 1 && !trusted {
					want = 429
				}
				if w.Code != want {
					t.Fatalf("forwarding trust mismatch: got %d; want %d", w.Code, want)
				}
			}
		})
	}
}

type unreadBody struct{ read bool }

func (b *unreadBody) Read([]byte) (int, error) { b.read = true; return 0, io.EOF }
func (b *unreadBody) Close() error             { return nil }

func TestRateLimitedRequestDoesNotReadBody(t *testing.T) {
	h, _, _ := newHandlerWithOptions(t, 10, api.Options{RateLimit: 1})
	if w := requestLogs(h, "["+validRecord+"]", "text/plain"); w.Code != 415 {
		t.Fatalf("expected media type rejection: %d", w.Code)
	}
	body := new(unreadBody)
	r := httptest.NewRequest(http.MethodPost, "/logs", body)
	r.Header.Set("Content-Type", "application/json")
	r.RemoteAddr = "10.0.0.2:1000"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 429 || body.read {
		t.Fatal("rate limiting must charge invalid requests and reject before reading the next body")
	}
}

func TestDefaultRateBudgetAlsoProtectsUnavailableIngestion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, err := api.NewHandler(api.Options{})
		if err != nil {
			t.Fatal(err)
		}
		for range 1000 {
			if w := requestLogs(h, "[]", "application/json"); w.Code != 503 {
				t.Fatalf("default budget was not 1000 requests: %d", w.Code)
			}
		}
		if w := requestLogs(h, "[]", "application/json"); w.Code != 429 {
			t.Fatalf("unavailable ingestion bypassed limiter: %d", w.Code)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		if w.Code != 200 {
			t.Fatal("health check was rate limited")
		}
	})
}

func TestDelayedRequestUsesCurrentBucketAfterIdleCleanup(t *testing.T) {
	var clock atomic.Int64
	var pause atomic.Bool
	paused, resume := make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(resume) })
	now := func() time.Time {
		value := clock.Load()
		if pause.CompareAndSwap(true, false) {
			close(paused)
			<-resume
		}
		return time.Unix(0, value)
	}
	h, _, _ := newHandlerWithOptions(t, 10, api.Options{RateLimit: 1, IPIdleTimeout: time.Second, Now: now})
	post := func(peer string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/logs", strings.NewReader("["+validRecord+"]"))
		r.Header.Set("Content-Type", "application/json")
		r.RemoteAddr = peer
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := post("192.0.2.1:1000"); w.Code != 202 {
		t.Fatal("first request rejected")
	}
	clock.Store(int64(100 * time.Millisecond))
	pause.Store(true)
	done := make(chan int, 1)
	go func() { done <- post("192.0.2.1:1000").Code }()
	select {
	case <-paused:
	case <-time.After(time.Second):
		t.Fatal("delayed request did not pause")
	}
	clock.Store(int64(2 * time.Second))
	if w := post("192.0.2.2:1000"); w.Code != 202 {
		t.Fatal("independent client rejected")
	}
	release.Do(func() { close(resume) })
	select {
	case code := <-done:
		if code != 202 {
			t.Fatalf("delayed request rejected: %d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("delayed request did not finish")
	}
	if w := post("192.0.2.1:1000"); w.Code != 429 {
		t.Fatalf("stale receive time granted a second current-window token: %d", w.Code)
	}
}
