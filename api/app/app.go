package app

import (
	"net/http"

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.uber.org/zap"
)

type (
	// Option is a function that applies an option to the application API.
	Option func(*app)

	app struct {
		log *zap.Logger
	}
)

// WithLogger sets the logger for application API.
func WithLogger(log *zap.Logger) Option {
	return func(api *app) {
		api.log = log
	}
}

// NewAPI creates a new instance of the application API. This API is used by
// users, or rather their applications, to pin slabs to the indexer.
// Authentication happens through presigned URLs that are signed with a private
// key that corresponds to a previously registered public key.
func NewAPI(hostname string, store AccountStore, opts ...Option) http.Handler {
	a := &app{
		log: zap.NewNop(),
	}
	for _, opt := range opts {
		opt(a)
	}

	handlers := map[string]authedHandler{
		"GET /foo": func(jc jape.Context, pk types.PublicKey) {
			if ok, err := store.HasAccount(jc.Request.Context(), pk); err != nil {
				jc.ResponseWriter.WriteHeader(http.StatusInternalServerError)
				return
			} else if !ok {
				jc.Error(ErrUnknownAccount, http.StatusUnauthorized)
				return
			}
			jc.ResponseWriter.WriteHeader(http.StatusOK)
		},
	}

	signed := make(map[string]jape.Handler)
	for path, handler := range handlers {
		signed[path] = func(jc jape.Context) {
			pk, ok := checkSignedURLAuth(jc, hostname, store)
			if !ok {
				return
			}
			handler(jc, pk)
		}
	}
	return jape.Mux(signed)
}
