package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.uber.org/zap"
)

const dialTimeout = 10 * time.Second

// Dialer can be used to dial a host using SiaMux or QUIC.
type Dialer struct {
	cm                       ChainManager
	revisionSubmissionBuffer uint64
	signer                   rhp.FormContractSigner
	store                    RevisionStore
	log                      *zap.Logger
}

// DialerOption is a functional option type for configuring the
// Dialer.
type DialerOption func(*Dialer)

// WithRevisionSubmissionBuffer sets the revision submission buffer for the
// Dialer.
func WithRevisionSubmissionBuffer(buffer uint64) DialerOption {
	if buffer == 0 {
		panic("revisionSubmissionBuffer mustn't be 0") // developer error
	}
	return func(c *Dialer) {
		c.revisionSubmissionBuffer = buffer
	}
}

// NewDialer creates a new Dialer.
func NewDialer(cm ChainManager, signer rhp.FormContractSigner, store RevisionStore, log *zap.Logger, opts ...DialerOption) *Dialer {
	d := &Dialer{
		cm:                       cm,
		revisionSubmissionBuffer: defaultRevisionSubmissionBuffer,
		signer:                   signer,
		store:                    store,
		log:                      log,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// DialHost dials the host and returns a Client that can be used to interact
// with the host. It tries SiaMux first, then QUIC, and returns a host client
// that exposes the RPC methods defined in the RHP.
func (d *Dialer) DialHost(ctx context.Context, hk types.PublicKey, addrs []chain.NetAddress) (*HostClient, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	var lastErr error
	tryDial := func(proto chain.Protocol, dial func(addr string) (rhp.TransportClient, error)) rhp.TransportClient {
		for _, addr := range addrs {
			if addr.Protocol != proto {
				continue
			}
			tc, err := dial(addr.Address)
			if err != nil {
				d.log.Debug("failed to dial host",
					zap.String("protocol", string(proto)),
					zap.Stringer("hostKey", hk),
					zap.String("addr", addr.Address),
					zap.Error(err),
				)
				lastErr = err
				continue
			}
			return tc
		}
		return nil
	}

	var tc rhp.TransportClient
	tc = tryDial(siamux.Protocol, func(addr string) (rhp.TransportClient, error) {
		return siamux.Dial(ctx, addr, hk)
	})
	if tc == nil {
		tc = tryDial(quic.Protocol, func(addr string) (rhp.TransportClient, error) {
			return quic.Dial(ctx, addr, hk)
		})
	}
	if len(addrs) == 0 {
		return nil, errors.New("no addresses provided")
	} else if lastErr != nil {
		return nil, fmt.Errorf("all dials failed: %w", lastErr)
	} else if tc == nil {
		return nil, errors.New("no supported protocols in address list")
	}

	return newHostClient(hk, d.cm, tc, d.signer, d.store, d.revisionSubmissionBuffer, d.log.With(zap.Stringer("hostKey", hk))), nil
}
