package api

import (
	"go.uber.org/zap"
)

// ServerOption is a functional option to configure an API server.
type ServerOption func(*api)

// WithLogger sets the logger for the API server.
func WithLogger(log *zap.Logger) ServerOption {
	return func(a *api) {
		a.log = log
	}
}
