package accounts

import (
	"context"
	"errors"
	"fmt"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type (
	// ChainManager defines an interface to get the current chain state.
	ChainManager interface {
		TipState() consensus.State
	}

	// Funder dials a host and replenish a set of ephemeral accounts.
	Funder struct {
		cm     ChainManager
		dialer hosts.Dialer
		signer rhp.ContractSigner
	}
)

type hostClient struct{}

func (c *hostClient) Dial(ctx context.Context, addr string, peerKey types.PublicKey) (rhp.TransportClient, error) {
	return siamux.Dial(ctx, addr, peerKey)
}

func (c *hostClient) RPCLatestRevision(ctx context.Context, t rhp.TransportClient, fcid types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
	return rhp.RPCLatestRevision(ctx, t, fcid)
}

func (c *hostClient) RPCReplenishAccounts(ctx context.Context, t rhp.TransportClient, params rhp.RPCReplenishAccountsParams, state consensus.State, signer rhp.ContractSigner) (rhp.RPCReplenishAccountsResult, error) {
	return rhp.RPCReplenishAccounts(ctx, t, params, state, signer)
}

// NewFunder creates a new Funder.
func NewFunder(cm ChainManager, dialer hosts.Dialer, signer rhp.ContractSigner) *Funder {
	return &Funder{
		cm:     cm,
		dialer: dialer,
		signer: signer,
	}
}

// FundAccounts tops up the provided accounts to the target balance using the
// specified contracts in order. The given accounts should not exceed the batch
// size used in the replenish RPC. This method returns two numbers, the first
// one indicates the number of accounts that were funded, the second indicates
// the number of contracts that were drained. Consecutive calls for the same
// host should take this into account and adjust the contract IDs that are being
// passed in.
func (f *Funder) FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, accounts []HostAccount, target types.Currency, log *zap.Logger) (funded int, drained int, _ error) {
	// sanity check the input
	if len(accounts) > proto.MaxAccountBatchSize {
		return 0, 0, errors.New("too many accounts")
	} else if len(contractIDs) == 0 {
		return 0, 0, errors.New("no contract provided")
	} else if len(accounts) == 0 {
		return 0, 0, nil
	}

	// dial the host
	client, err := f.dialer.Dial(ctx, host.PublicKey, host.SiamuxAddr())
	if err != nil {
		return 0, 0, fmt.Errorf("failed to dial host %s: %w", host.PublicKey, err)
	}

	// prepare account keys
	accountKeys := make([]proto.Account, len(accounts))
	for i, account := range accounts {
		accountKeys[i] = account.AccountKey
	}

	// iterate over contracts
	for _, contractID := range contractIDs {
		contractLog := log.With(zap.Stringer("contractID", contractID))

		// execute replenish RPC
		res, n, err := client.ReplenishAccounts(ctx, contractID, accountKeys, target)
		if errors.Is(err, hosts.ErrContractInsufficientFunds) {
			contractLog.Debug("contract has insufficient funds")
			drained++
			continue
		} else if errors.Is(err, hosts.ErrContractNotRevisable) {
			contractLog.Debug("contract is not revisable") // sanity check
			drained++
			continue
		} else if err != nil {
			contractLog.Debug("failed to replenish accounts", zap.Error(err))
			continue
		} else if res.Revision.RenterOutput.Value.Cmp(target) < 0 {
			contractLog.Debug("contract was drained by replenish RPC")
			drained++
		}

		// update funded ix
		funded += n
		accountKeys = accountKeys[n:]
		if len(accountKeys) == 0 {
			break
		}
	}

	return funded, drained, nil
}
