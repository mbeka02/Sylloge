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
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if logger == nil {
		logger = slog.Default()
	}
	engine, err := newEngine(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, engine.closeStorage()) }()
	server, err := api.NewServer(net.JoinHostPort("", strconv.Itoa(cfg.HTTP.Port)), api.Options{
		Logger: logger, Environment: cfg.Environment, TrustedProxies: cfg.HTTP.TrustedProxies,
		MaxActiveRequests: cfg.HTTP.MaxActiveRequests, RateLimit: cfg.HTTP.RateLimit,
		MaxIPEntries: cfg.HTTP.MaxIPEntries, IPIdleTimeout: cfg.HTTP.IPIdleTimeout,
		Admitter: engine.pipeline, Metrics: engine.metrics, Ready: engine.healthy,
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
	engine.start()
	logger.Info("listening", "address", listener.Addr().String(), "environment", cfg.Environment)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	serveConsumed := false
	select {
	case serveErr := <-served:
		serveConsumed = true
		err = serveError(serveErr)
	case <-ctx.Done():
	}
	logger.Info("shutdown started")
	engine.ready.Store(false)
	// The signal must not cancel the independent drain window.
	drain, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if shutdownErr := server.Shutdown(drain); shutdownErr != nil {
		err = errors.Join(err, fmt.Errorf("shutdown HTTP server: %w", shutdownErr), server.Close())
	}
	if !serveConsumed {
		err = errors.Join(err, serveError(<-served))
	}
	err = errors.Join(err, engine.drain(drain))
	if err == nil {
		logger.Info("shutdown complete")
	}
	return err
}

func serveError(err error) error {
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("serve HTTP: %w", err)
}
