package testutils

import (
	"errors"
	"net"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap"
)

type (
	// A Host is an ephemeral host that can be used for testing.
	Host struct {
		address string
		pk      types.PrivateKey

		c  *testutil.EphemeralContractor
		ss *testutil.EphemeralSectorStore
		sr *testutil.EphemeralSettingsReporter
		s  *syncer.Syncer
		w  *wallet.SingleAddressWallet

		closeFn func() error
	}
)

// Addr returns the host's address.
func (h *Host) Addr() string {
	return h.address
}

// Close shuts down the host subsystems.
func (h *Host) Close() error {
	return errors.Join(
		h.s.Close(),
		h.w.Close(),
		h.c.Close(),
		h.closeFn(),
	)
}

// PublicKey returns the host's public key.
func (h *Host) PublicKey() types.PublicKey {
	return h.pk.PublicKey()
}

// NewHost creates a new host.
func NewHost(pk types.PrivateKey, n *consensus.Network, genesis types.Block, log *zap.Logger) (*Host, error) {
	db, tipstate, err := chain.NewDBStore(chain.NewMemDB(), n, genesis)
	if err != nil {
		return nil, err
	}
	cm := chain.NewManager(db, tipstate)

	syncerListener, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}
	s := syncer.New(syncerListener, cm, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: "localhost:1234",
	},
		syncer.WithSendBlocksTimeout(2*time.Second),
		syncer.WithRPCTimeout(2*time.Second),
	)
	go s.Run()

	ws := testutil.NewEphemeralWalletStore()
	w, err := wallet.NewSingleAddressWallet(types.GeneratePrivateKey(), cm, ws)
	if err != nil {
		return nil, err
	}

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	reorgCh := make(chan struct{}, 1)
	go func() {
		for range reorgCh {
			reverted, applied, err := cm.UpdatesSince(w.Tip(), 1000)
			if err != nil {
				panic(err)
			}

			if err := ws.UpdateChainState(func(tx wallet.UpdateTx) error {
				return w.UpdateChainState(tx, reverted, applied)
			}); err != nil {
				panic(err)
			}
		}
	}()
	unsubscribeFn := cm.OnReorg(func(index types.ChainIndex) {
		select {
		case reorgCh <- struct{}{}:
		default:
		}
	})

	rs := rhp4.NewServer(pk, cm, s, c, w, sr, ss, rhp4.WithPriceTableValidity(2*time.Minute))
	rhp4Listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}
	go rhp4.ServeSiaMux(rhp4Listener, rs, log)

	return &Host{
		c:       c,
		s:       s,
		ss:      ss,
		sr:      sr,
		w:       w,
		pk:      pk,
		address: rhp4Listener.Addr().String(),

		closeFn: func() error {
			unsubscribeFn()
			close(reorgCh)
			return errors.Join(
				rhp4Listener.Close(),
				syncerListener.Close(),
			)
		},
	}, nil
}
