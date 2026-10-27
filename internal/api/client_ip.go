package api

import (
	"net/http"
	"net/netip"
	"strings"
)

// ResolveClientIP walks X-Forwarded-For from the trusted end, stopping at the
// first untrusted address. Untrusted peers cannot supply audit/limiter identity.
func ResolveClientIP(r *http.Request, trusted []netip.Prefix) string {
	var peer netip.Addr
	if address, err := netip.ParseAddrPort(r.RemoteAddr); err == nil {
		peer = address.Addr().Unmap()
	} else if address, err := netip.ParseAddr(r.RemoteAddr); err == nil {
		peer = address.Unmap()
	} else {
		return "unknown"
	}
	isTrusted := func(address netip.Addr) bool {
		for _, prefix := range trusted {
			if prefix.Contains(address) {
				return true
			}
		}
		return false
	}
	if !isTrusted(peer) {
		return peer.String()
	}
	forwarded := strings.Join(r.Header.Values("X-Forwarded-For"), ",")
	if strings.TrimSpace(forwarded) == "" {
		return peer.String()
	}
	hops := strings.Split(forwarded, ",")
	current := peer
	for i := len(hops) - 1; i >= 0; i-- {
		if !isTrusted(current) {
			return current.String()
		}
		address, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
		if err != nil {
			return peer.String()
		}
		current = address.Unmap()
	}
	return current.String()
}
