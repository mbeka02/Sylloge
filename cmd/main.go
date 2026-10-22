package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/mbeka02/sylloge/internal/api"
)

func HandleGracefulShutdown(srv *http.Server, done chan bool) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	fmt.Println("shutting down gracefully, press Ctrl+C again to force")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		fmt.Errorf("server forced to shutdown:(%w)", err)
	}

	fmt.Println("server exiting")
	done <- true
}

func setupServer() (*http.Server, error) {
	srv, err := api.NewServer(":3000")
	if err != nil {
		return nil, fmt.Errorf("failed to setup server: %w", err)
	}

	return srv, nil
}

func main() {
	srv, err := setupServer()
	if err != nil {
		log.Fatalf("failed to setup server:(%w)", err)
	}

	done := make(chan bool, 1)
	go HandleGracefulShutdown(srv, done)
	log.Println("Starting server on port", srv.Addr)
	err = srv.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server error:(%w)", err)
	}

	<-done
	log.Println("graceful shutdown complete")
}
