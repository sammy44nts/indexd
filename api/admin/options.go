package admin

import "go.uber.org/zap"

type (
	// Option is a function that applies an option to the admin API.
	Option func(*admin)
)

// WithExplorer sets the explorer for the admin API.
func WithExplorer(e Explorer) Option {
	return func(api *admin) {
		api.explorer = e
	}
}

// WithLogger sets the logger for admin API.
func WithLogger(log *zap.Logger) Option {
	return func(api *admin) {
		api.log = log
	}
}

// WithDebug sets the debug mode for admin API. In debug mode, the server
// exposes additional debug endpoints that allow triggering certain actions.
func WithDebug() Option {
	return func(api *admin) {
		api.debug = true
	}
}
