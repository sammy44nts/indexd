package testutils

import (
	"net"
	"testing"
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
		l  net.Listener
		pk types.PrivateKey

		c  *testutil.EphemeralContractor
		ss *testutil.EphemeralSectorStore
		sr *testutil.EphemeralSettingsReporter
		s  *syncer.Syncer
		w  *wallet.SingleAddressWallet
	}
)

// Addr returns the host's address.
func (h *Host) Addr() string {
	return h.l.Addr().String()
}

// PublicKey returns the host's public key.
func (h *Host) PublicKey() types.PublicKey {
	return h.pk.PublicKey()
}

// NewHost creates a new host.
func NewHost(t testing.TB, pk types.PrivateKey, n *consensus.Network, genesis types.Block, log *zap.Logger) *Host {
	db, tipstate, err := chain.NewDBStore(chain.NewMemDB(), n, genesis)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(db, tipstate)

	syncerListener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { syncerListener.Close() })

	s := syncer.New(syncerListener, cm, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: "localhost:1234",
	},
		syncer.WithSendBlocksTimeout(2*time.Second),
		syncer.WithRPCTimeout(2*time.Second),
	)
	t.Cleanup(func() { s.Close() })
	go s.Run()

	ws := testutil.NewEphemeralWalletStore()
	w, err := wallet.NewSingleAddressWallet(types.GeneratePrivateKey(), cm, ws)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })

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
	t.Cleanup(func() { c.Close() })

	reorgCh := make(chan struct{}, 1)
	t.Cleanup(func() { close(reorgCh) })
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
	stop := cm.OnReorg(func(index types.ChainIndex) {
		select {
		case reorgCh <- struct{}{}:
		default:
		}
	})
	t.Cleanup(stop)

	rs := rhp4.NewServer(pk, cm, s, c, w, sr, ss, rhp4.WithPriceTableValidity(2*time.Minute))
	rhp4Listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rhp4Listener.Close() })
	go rhp4.ServeSiaMux(rhp4Listener, rs, log)

	return &Host{
		pk: pk,
		l:  rhp4Listener,

		c:  c,
		s:  s,
		ss: ss,
		sr: sr,
		w:  w,
	}
}
