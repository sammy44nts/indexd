package accounts

import (
	"context"
	"errors"
	"testing"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type dialerMock struct{}

func (d *dialerMock) Dial(ctx context.Context, hostKey types.PublicKey, addr string) (hosts.Client, error) {
	return &hostClientMock{}, nil
}

type cmMock struct{}

func (cmMock) TipState() consensus.State { return consensus.State{} }

type hostClientMock struct{}

func (*hostClientMock) Close() error { return nil }

func (*hostClientMock) AppendSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, sectors []types.Hash256) (rhp.RPCAppendSectorsResult, error) {
	return rhp.RPCAppendSectorsResult{}, nil
}
func (*hostClientMock) FormContract(ctx context.Context, settings proto.HostSettings, params proto.RPCFormContractParams) (rhp.RPCFormContractResult, error) {
	return rhp.RPCFormContractResult{}, nil
}
func (*hostClientMock) FreeSectors(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, indices []uint64) (rhp.RPCFreeSectorsResult, error) {
	return rhp.RPCFreeSectorsResult{}, nil
}
func (*hostClientMock) SectorRoots(ctx context.Context, hostPrices proto.HostPrices, contractID types.FileContractID, offset, length uint64) (rhp.RPCSectorRootsResult, error) {
	return rhp.RPCSectorRootsResult{}, nil
}
func (*hostClientMock) RefreshContract(ctx context.Context, settings proto.HostSettings, params proto.RPCRefreshContractParams) (rhp.RPCRefreshContractResult, error) {
	return rhp.RPCRefreshContractResult{}, nil
}
func (*hostClientMock) RenewContract(ctx context.Context, settings proto.HostSettings, contractID types.FileContractID, proofHeight uint64) (rhp.RPCRenewContractResult, error) {
	return rhp.RPCRenewContractResult{}, nil
}
func (*hostClientMock) ReplenishAccounts(ctx context.Context, contractID types.FileContractID, accounts []proto.Account, target types.Currency) (rhp.RPCReplenishAccountsResult, int, error) {
	// use contract ID to cover all possible branches
	switch contractID {
	case types.FileContractID{1}:
		return rhp.RPCReplenishAccountsResult{}, 0, hosts.ErrContractInsufficientFunds
	case types.FileContractID{2}:
		return rhp.RPCReplenishAccountsResult{}, 0, hosts.ErrContractNotRevisable
	case types.FileContractID{3}:
		return rhp.RPCReplenishAccountsResult{}, 0, errors.New("failed to replenish accounts")
	case types.FileContractID{4}:
		return rhp.RPCReplenishAccountsResult{Revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target.Sub(types.NewCurrency64(1))}}}, 1, nil
	case types.FileContractID{5}, types.FileContractID{6}:
		return rhp.RPCReplenishAccountsResult{Revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target}}}, 1, nil
	case types.FileContractID{7}:
		return rhp.RPCReplenishAccountsResult{Revision: types.V2FileContract{RenterOutput: types.SiacoinOutput{Value: target}}}, len(accounts) - 1, nil
	default:
		panic("unexpected contract ID in mock")
	}
}

// TestFunder is a unit test that checks the various edge cases in FundAccounts
func TestFunder(t *testing.T) {
	// prepare funder
	f := NewFunder(&cmMock{}, &dialerMock{}, nil)

	// prepare accounts
	accounts := []HostAccount{
		{AccountKey: proto.Account{1}},
		{AccountKey: proto.Account{2}},
		{AccountKey: proto.Account{3}},
	}

	// assert contract is marked as drained if it is out of funds
	funded, drained, err := f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{1}}, accounts, types.Siacoins(1), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 0 {
		t.Fatal("expected 0 accounts funded, got", funded)
	} else if drained != 1 {
		t.Fatal("expected 1 contracts drained, got", drained)
	}

	// assert contract is marked as drained if it is not revisable
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{2}}, accounts, types.Siacoins(1), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 0 {
		t.Fatal("expected 0 accounts funded, got", funded)
	} else if drained != 1 {
		t.Fatal("expected 1 contracts drained, got", drained)
	}

	// assert contract is not marked as drained if replenish RPC fails
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{3}}, accounts, types.Siacoins(1), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 0 {
		t.Fatal("expected 0 accounts funded, got", funded)
	} else if drained != 0 {
		t.Fatal("expected 0 contracts drained, got", drained)
	}

	// assert contract is marked as drained if replenish RPC succeeds but leaves the contract with insufficient funds afterwards
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{4}}, accounts, types.Siacoins(1), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 1 {
		t.Fatal("expected 1 account funded, got", funded)
	} else if drained != 1 {
		t.Fatal("expected 1 contracts drained, got", drained)
	}

	// assert contracts are iterated and funded is updated until we run out of contracts
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{5}, {6}}, accounts, types.Siacoins(1), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 2 {
		t.Fatal("expected 2 account funded, got", funded)
	} else if drained != 0 {
		t.Fatal("expected 0 contracts drained, got", drained)
	}

	// assert contracts are iterated and funded is updated until we run out of accounts
	funded, drained, err = f.FundAccounts(context.Background(), hosts.Host{}, []types.FileContractID{{7}, {1}, {5}, {4}}, accounts, types.Siacoins(1), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	} else if funded != 3 {
		t.Fatal("expected 3 account funded, got", funded)
	} else if drained != 1 {
		t.Fatal("expected 1 contracts drained, got", drained) // both 1 and 4 would be drained, were it not we ran out of accounts to replenish
	}
}
