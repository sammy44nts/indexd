package contracts

import (
	"context"
	"net"
	"slices"
	"strings"
	"testing"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type formContractCall struct {
	hk       types.PublicKey
	addr     string
	settings proto.HostSettings
	params   proto.RPCFormContractParams
}

type contractFormerMock struct {
	calls []formContractCall
}

func (cf *contractFormerMock) Calls() []formContractCall {
	calls := slices.Clone(cf.calls)
	slices.SortFunc(calls, func(a, b formContractCall) int {
		return strings.Compare(a.hk.String(), b.hk.String())
	})
	return calls
}

func (cf *contractFormerMock) FormContract(ctx context.Context, hk types.PublicKey, addr string, settings proto.HostSettings, params proto.RPCFormContractParams) (rhp.RPCFormContractResult, error) {
	cf.calls = append(cf.calls, formContractCall{
		hk:       hk,
		addr:     addr,
		settings: settings,
		params:   params,
	})
	return rhp.RPCFormContractResult{
		FormationSet: rhp.TransactionSet{
			Transactions: []types.V2Transaction{
				{
					MinerFee: types.Siacoins(1),
				},
			},
		},
	}, nil
}

type scannerMock struct {
	settings map[types.PublicKey]proto.HostSettings

	store *storeMock
}

// Scanner is a convenience method to create a scanner from a store mock. The
// scanner contains all the settings of the hosts from the mocked store and will
// be updating the store upon scanning.
func (s *storeMock) Scanner() *scannerMock {
	scannerMock := &scannerMock{
		store:    s,
		settings: map[types.PublicKey]proto.HostSettings{},
	}
	for _, host := range s.hosts {
		scannerMock.settings[host.PublicKey] = host.Settings
	}
	return scannerMock
}

// ScanHost returns the preconfigured settings for the host or no settings to
// simulate a failing scan. Upon success, the underlying store is updated.
func (s *scannerMock) ScanHost(ctx context.Context, hk types.PublicKey) (proto.HostSettings, error) {
	settings, ok := s.settings[hk]
	if !ok {
		return proto.HostSettings{}, nil
	}
	s.store.UpdateHostSettings(hk, settings)
	return settings, nil
}

func TestPerformContractFormation(t *testing.T) {
	cmMock := newChainManagerMock()
	blockHeight := cmMock.TipState().Index.Height
	syncerMock := &syncerMock{}

	goodUsability := hosts.Usability{
		Uptime:              true,
		MaxContractDuration: true,
		MaxCollateral:       true,
		ProtocolVersion:     true,
		PriceValidity:       true,
		AcceptingContracts:  true,

		ContractPrice:   true,
		Collateral:      true,
		StoragePrice:    true,
		IngressPrice:    true,
		EgressPrice:     true,
		FreeSectorPrice: true,
	}

	const (
		period = 100
		wanted = 5
	)
	hk1 := types.PublicKey{1}

	// prepare settings which will cause hosts to either be good for forming contracts or not
	badSettings := proto.HostSettings{AcceptingContracts: false}
	goodSettings := proto.HostSettings{
		AcceptingContracts: true,
		RemainingStorage:   minRemainingStorage,
	}

	// create the store with multiple hosts, all of which start with bad
	// settings since the scanner will update them
	store := &storeMock{
		hosts: map[types.PublicKey]hosts.Host{
			// good host
			hk1: {
				PublicKey: hk1,
				Networks: []net.IPNet{
					{IP: net.IP{127, 0, 0, 1}, Mask: net.CIDRMask(24, 32)},
				},
				Addresses: []chain.NetAddress{
					{
						Protocol: siamux.Protocol,
						Address:  "host1.com",
					},
				},
				Settings:  badSettings,
				Usability: goodUsability,
			},
		},
	}

	scanner := store.Scanner()
	scanner.settings[hk1] = goodSettings

	cf := &contractFormerMock{}
	renterKey := types.PublicKey{1, 2, 3, 4, 5}
	wallet := &walletMock{}
	contracts := newContractManager(renterKey, cmMock, cf, scanner, store, syncerMock, wallet)

	assertFormation := func(hk types.PublicKey, addr string, settings proto.HostSettings, call formContractCall) {
		t.Helper()
		if call.hk != hk {
			t.Fatalf("expected host key %v, got %v", hk, call.hk)
		} else if call.addr != addr {
			t.Fatalf("expected address %v, got %v", addr, call.addr)
		} else if call.settings != settings {
			t.Fatalf("expected settings %v+, got %v+", settings, call.settings)
		}
		// assert params
		allowance, collateral := initialContractFunding(settings.Prices, period)
		if !call.params.Allowance.Equals(call.params.Allowance) {
			t.Fatalf("expected allowance %v, got %v", allowance, call.params.Allowance)
		} else if !call.params.Collateral.Equals(collateral) {
			t.Fatalf("expected collateral %v, got %v", collateral, call.params.Collateral)
		} else if call.params.ProofHeight != blockHeight+period {
			t.Fatalf("expected proof height %v, got %v", blockHeight+period, call.params.ProofHeight)
		} else if call.params.RenterPublicKey != renterKey {
			t.Fatalf("expected renter key %v, got %v", renterKey, call.params.RenterPublicKey)
		} else if call.params.RenterAddress != wallet.Address() {
			t.Fatalf("expected renter address %v, got %v", wallet.Address(), call.params.RenterAddress)
		}
	}

	// perform formations
	if err := contracts.performContractFormation(context.Background(), period, wanted, zap.NewNop()); err != nil {
		t.Fatal(err)
	}

	// assert that we attempted to form contracts with the right hosts,
	// settings and params
	calls := cf.Calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %v", len(calls))
	}
	assertFormation(hk1, "host1.com", goodSettings, calls[0])
}
