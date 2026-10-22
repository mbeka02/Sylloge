package api

import (
	"fmt"
	"net/http"
)

func (s *Server) RegisterRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "logs upon logs upon logs upon logs")
	})
	return mux
}
