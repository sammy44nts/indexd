package api

import (
	"net/http"

	"go.sia.tech/jape"
	"go.uber.org/zap"
)

type (
	// An api provides an HTTP API for the indexer
	api struct {
		log *zap.Logger
	}
)

// NewServer initializes the API
func NewServer(opts ...ServerOption) http.Handler {
	a := &api{log: zap.NewNop()}
	for _, opt := range opts {
		opt(a)
	}

	return jape.Mux(map[string]jape.Handler{
		"GET /state": a.handleGETState,
	})
}
