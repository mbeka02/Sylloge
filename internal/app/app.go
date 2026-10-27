// Package app owns startup and shutdown of the application's concrete modules.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"

	"github.com/mbeka02/sylloge/internal/api"
	"github.com/mbeka02/sylloge/internal/config"
)

// Run serves until cancellation or a server failure. A failed graceful shutdown
// forces connections closed and returns an error rather than reporting success.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := config.LoadRouting(cfg.RoutingConfigFile); err != nil {
		return err
	}
	server, err := api.NewServer(net.JoinHostPort("", strconv.Itoa(cfg.HTTP.Port)), api.Options{
		Logger: logger, Environment: cfg.Environment, TrustedProxies: cfg.HTTP.TrustedProxies,
		MaxActiveRequests: cfg.HTTP.MaxActiveRequests, RateLimit: cfg.HTTP.RateLimit,
		MaxIPEntries: cfg.HTTP.MaxIPEntries, IPIdleTimeout: cfg.HTTP.IPIdleTimeout,
	})
	if err != nil {
		return fmt.Errorf("configure HTTP server: %w", err)
	}
	server.ReadHeaderTimeout = cfg.HTTP.ReadHeaderTimeout
	server.ReadTimeout = cfg.HTTP.ReadTimeout
	server.WriteTimeout = cfg.HTTP.WriteTimeout
	server.IdleTimeout = cfg.HTTP.IdleTimeout
	server.MaxHeaderBytes = cfg.HTTP.MaxHeaderBytes
	server.ErrorLog = slog.NewLogLogger(logger.Handler(), slog.LevelError)
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("listen for HTTP: %w", err)
	}
	logger.Info("listening", "address", listener.Addr().String(), "environment", cfg.Environment)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	select {
	case err := <-served:
		return serveError(err)
	case <-ctx.Done():
	}
	logger.Info("shutdown started")
	// The signal must not cancel the independent drain window.
	drain, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(drain); err != nil {
		closeErr := server.Close()
		return errors.Join(fmt.Errorf("shutdown HTTP server: %w", err), closeErr, serveError(<-served))
	}
	if err := serveError(<-served); err != nil {
		return err
	}
	logger.Info("shutdown complete")
	return nil
}

func serveError(err error) error {
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("serve HTTP: %w", err)
}
