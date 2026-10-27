package api_test

import (
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/mbeka02/sylloge/internal/api"
)

func TestClientIPTrustsOnlyConfiguredProxyHops(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("2001:db8:1::/48")}
	for _, tc := range []struct{ peer, forwarded, want string }{
		{"192.0.2.4:1234", "198.51.100.1", "192.0.2.4"},
		{"10.0.0.2:1234", "198.51.100.1, 10.0.0.1", "198.51.100.1"},
		{"10.0.0.2:1234", "forged, 192.0.2.1", "192.0.2.1"},
		{"10.0.0.2:1234", "192.0.2.1, invalid", "10.0.0.2"},
		{"[2001:db8:1::2]:1234", "2001:db8:2::1", "2001:db8:2::1"},
		{"[::ffff:10.0.0.2]:1234", "192.0.2.1", "192.0.2.1"},
		{"bad-address", "192.0.2.1", "unknown"},
		{"10.0.0.2:1234", "", "10.0.0.2"},
	} {
		t.Run(tc.peer+"/"+tc.forwarded, func(t *testing.T) {
			request := httptest.NewRequest("POST", "/logs", nil)
			request.RemoteAddr = tc.peer
			request.Header.Set("X-Forwarded-For", tc.forwarded)
			if got := api.ResolveClientIP(request, trusted); got != tc.want {
				t.Fatalf("got %q; want %q", got, tc.want)
			}
		})
	}
}
