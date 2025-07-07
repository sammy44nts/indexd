package admin

import (
	"fmt"
	"net/url"

	"go.sia.tech/indexd/api"
	"go.uber.org/zap"
)

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

// HostQueryParameterOption is an option to configure the query string for the
// Hosts endpoint.
type HostQueryParameterOption api.URLQueryParameterOption

// WithBlocked sets the 'blocked' parameter.
func WithBlocked(blocked bool) HostQueryParameterOption {
	return func(q url.Values) {
		q.Set("blocked", fmt.Sprint(blocked))
	}
}

// WithUsable sets the 'usable' parameter.
func WithUsable(usable bool) HostQueryParameterOption {
	return func(q url.Values) {
		q.Set("usable", fmt.Sprint(usable))
	}
}

// WithActiveContracts sets the 'activecontracts' parameter.
func WithActiveContracts(activeContracts bool) HostQueryParameterOption {
	return func(q url.Values) {
		q.Set("activecontracts", fmt.Sprint(activeContracts))
	}
}
