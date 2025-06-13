package accounts

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

const dialTimeout = 10 * time.Second

type (
	// ChainManager is the minimal interface of ChainManager functionality the
	// Funder requires.
	ChainManager interface {
		TipState() consensus.State
	}

	// HostClient defines the interface for the funder to interact with the
	// host.
	HostClient interface {
		Dial(context.Context, string, types.PublicKey) (rhp.TransportClient, error)
		RPCLatestRevision(context.Context, rhp.TransportClient, types.FileContractID) (proto.RPCLatestRevisionResponse, error)
		RPCReplenishAccounts(context.Context, rhp.TransportClient, types.FileContractID, []proto.Account, types.Currency) (rhp.RPCReplenishAccountsResult, int, error)
	}

	// Funder dials a host and replenish a set of ephemeral accounts.
	Funder struct {
		client HostClient
	}
)

var (
	errContractInsufficientFunds = errors.New("contract has insufficient funds")
	errContractNotRevisable      = errors.New("contract is not revisable")
	errContractOutOfFunds        = errors.New("contract is out of funds")
	errContractRenewed           = errors.New("contract got renewed")
)

type hostClient struct {
	cm     ChainManager
	signer rhp.ContractSigner
}

func (c *hostClient) Dial(ctx context.Context, addr string, peerKey types.PublicKey) (rhp.TransportClient, error) {
	return siamux.Dial(ctx, addr, peerKey)
}

func (c *hostClient) RPCLatestRevision(ctx context.Context, t rhp.TransportClient, fcid types.FileContractID) (proto.RPCLatestRevisionResponse, error) {
	return rhp.RPCLatestRevision(ctx, t, fcid)
}

// RPCReplenishAccounts replenishes the accounts in the contract to the target value.
func (c *hostClient) RPCReplenishAccounts(ctx context.Context, t rhp.TransportClient, contractID types.FileContractID, accounts []proto.Account, target types.Currency) (rhp.RPCReplenishAccountsResult, int, error) {
	revision, err := c.RPCLatestRevision(ctx, t, contractID)
	if err != nil {
		return rhp.RPCReplenishAccountsResult{}, 0, fmt.Errorf("failed to fetch latest revision: %w", err)
	} else if !revision.Revisable {
		return rhp.RPCReplenishAccountsResult{}, 0, errContractNotRevisable
	} else if revision.Contract.RenterOutput.Value.IsZero() {
		return rhp.RPCReplenishAccountsResult{}, 0, errContractOutOfFunds
	} else if revision.Contract.RenterOutput.Value.Cmp(target) < 0 {
		return rhp.RPCReplenishAccountsResult{}, 0, errContractInsufficientFunds
	}

	// prepare batch
	batchSize := int(min(revision.Contract.RenterOutput.Value.Div(target).Big().Uint64(), proto.MaxAccountBatchSize))
	funded := min(batchSize, len(accounts))
	batch := accounts[:funded]

	// prepare parameters
	params := rhp.RPCReplenishAccountsParams{
		Accounts: batch,
		Target:   target,
		Contract: rhp.ContractRevision{ID: contractID, Revision: revision.Contract},
	}

	res, err := rhp.RPCReplenishAccounts(ctx, t, params, c.cm.TipState(), c.signer)
	if err != nil {
		return rhp.RPCReplenishAccountsResult{}, 0, fmt.Errorf("failed to replenish accounts: %w", err)
	}

	return res, funded, nil
}

// NewFunder creates a new Funder.
func NewFunder(cm ChainManager, signer rhp.ContractSigner) *Funder {
	return &Funder{
		client: &hostClient{cm: cm, signer: signer},
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
	tc, err := f.client.Dial(ctx, host.SiamuxAddr(), host.PublicKey)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to dial host %s: %w", host.PublicKey, err)
	}
	defer tc.Close()

	// prepare account keys
	accountKeys := make([]proto.Account, len(accounts))
	for i, account := range accounts {
		accountKeys[i] = account.AccountKey
	}

	// iterate over contracts
	for _, contractID := range contractIDs {
		contractLog := log.With(zap.Stringer("contractID", contractID))

		// execute replenish RPC
		res, n, err := f.client.RPCReplenishAccounts(ctx, tc, contractID, accountKeys[funded:], target)
		if errors.Is(err, errContractInsufficientFunds) {
			contractLog.Debug("contract has insufficient funds")
			drained++
			continue
		} else if errors.Is(err, errContractNotRevisable) {
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
		if funded == len(accountKeys) {
			break
		}
	}

	return funded, drained, nil
}
