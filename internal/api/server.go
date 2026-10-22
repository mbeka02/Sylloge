package api

import "net/http"

type Server struct {
	addr string
}

func NewServer(addr string) (*http.Server, error) {
	srv := &Server{
		addr,
	}
	return &http.Server{
		Handler: srv.RegisterRoutes(),
		Addr:    srv.addr,
	}, nil
}
