package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/config"
	"go.sia.tech/indexd/persist/postgres"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

func runRootCmd(ctx context.Context, cfg config.Config, log *zap.Logger) error {
	store, err := postgres.Connect(ctx, cfg.Database, log.Named("postgres"))
	if err != nil {
		return fmt.Errorf("failed to connect to postgres database: %w", err)
	}
	defer store.Close()

	httpListener, err := startLocalhostListener(cfg.HTTP.Address, log.Named("listener"))
	if err != nil {
		return fmt.Errorf("failed to listen on http address: %w", err)
	}
	defer httpListener.Close()

	apiOpts := []api.ServerOption{
		api.WithLogger(log.Named("api")),
	}

	web := http.Server{
		Handler: webRouter{
			api: jape.BasicAuth(cfg.HTTP.Password)(api.NewServer(apiOpts...)),
		},
		ReadTimeout: 30 * time.Second,
	}
	defer web.Close()

	go func() {
		log.Debug("starting http server", zap.String("address", cfg.HTTP.Address))
		if err := web.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", zap.Error(err))
		}
	}()

	log.Info("node started", zap.String("http", httpListener.Addr().String()))
	<-ctx.Done()
	log.Info("shutting down...")

	return nil
}

func startLocalhostListener(listenAddr string, log *zap.Logger) (l net.Listener, err error) {
	addr, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse API address: %w", err)
	}

	// if the address is not localhost, listen on the address as-is
	if addr != "localhost" {
		return net.Listen("tcp", listenAddr)
	}

	// localhost fails on some new installs of Windows 11, so try a few
	// different addresses
	tryAddresses := []string{
		net.JoinHostPort("localhost", port), // original address
		net.JoinHostPort("127.0.0.1", port), // IPv4 loopback
		net.JoinHostPort("::1", port),       // IPv6 loopback
	}

	for _, addr := range tryAddresses {
		l, err = net.Listen("tcp", addr)
		if err == nil {
			return
		}
		log.Debug("failed to listen on fallback address", zap.String("address", addr), zap.Error(err))
	}
	return
}
