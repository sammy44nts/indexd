package client

import (
	"context"
	"errors"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.uber.org/zap"
)

const dialTimeout = 10 * time.Second

// Dialer can be used to dial a host using the SiaMux protocol.
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
// with the host. It uses the SiaMux protocol to establish a connection and
// returns a host client that exposes the RPC methods defined in the RHP.
func (d *Dialer) DialHost(ctx context.Context, hk types.PublicKey, addrs []chain.NetAddress) (*HostClient, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	var tc rhp.TransportClient
	for _, addr := range addrs {
		if addr.Protocol == siamux.Protocol {
			var err error
			tc, err = siamux.Dial(ctx, addr.Address, hk)
			if err != nil {
				d.log.Debug("failed to dial host over siamux", zap.Stringer("pk", hk), zap.String("addr", addr.Address), zap.Error(err))
				break
			}
			break
		}
	}
	for _, addr := range addrs {
		if addr.Protocol == quic.Protocol {
			var err error
			tc, err = quic.Dial(ctx, addr.Address, hk)
			if err != nil {
				d.log.Debug("failed to dial host over QUIC", zap.Stringer("pk", hk), zap.String("addr", addr.Address), zap.Error(err))
				break
			}
			break
		}
	}
	if tc == nil {
		return nil, errors.New("host has no valid addresses")
	}

	return newHostClient(hk, d.cm, tc, d.signer, d.store, d.revisionSubmissionBuffer, d.log.With(zap.Stringer("hostKey", hk))), nil
}
