package contracts

import (
	"context"
	"errors"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/client/v2"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

type (
	// FunderHostClient defines the interface for the funder to interact with the
	// host.
	FunderHostClient interface {
		ReplenishAccounts(ctx context.Context, signer rhp.ContractSigner, chain client.ChainManager, params rhp.RPCReplenishAccountsParams) (rhp.RPCReplenishAccountsResult, error)
	}

	// Funder dials a host and replenish a set of ephemeral accounts.
	Funder struct {
		client FunderHostClient
		signer rhp.ContractSigner
		chain  ChainManager
		rev    *revisionManager

		log *zap.Logger
	}
)

// FunderOption is a functional option type for configuring the Funder.
type FunderOption func(*Funder)

// WithRevisionSubmissionBuffer sets the revision submission buffer for the
// Funder.
func WithRevisionSubmissionBuffer(buffer uint64) FunderOption {
	if buffer == 0 {
		panic("revisionSubmissionBuffer mustn't be 0") // developer error
	}
	return func(f *Funder) {
		f.rev.buffer = buffer
	}
}

// NewFunder creates a new Funder.
func NewFunder(client FunderHostClient, latestRevisionClient latestRevisionClient, signer rhp.ContractSigner, chain ChainManager, store RevisionStore, log *zap.Logger, opts ...FunderOption) *Funder {
	f := &Funder{
		client: client,
		signer: signer,
		chain:  chain,
		rev:    newRevisionManager(latestRevisionClient, chain, store, defaultRevisionSubmissionBuffer, log),
		log:    log,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// FundAccounts tops up the provided accounts to the target balance using the
// specified contracts in order. The given accounts should not exceed the batch
// size used in the replenish RPC. This method returns two numbers, the first
// one indicates the number of accounts that were funded, the second indicates
// the number of contracts that were drained. Consecutive calls for the same
// host should take this into account and adjust the contract IDs that are being
// passed in.
func (f *Funder) FundAccounts(ctx context.Context, host hosts.Host, contractIDs []types.FileContractID, accs []accounts.HostAccount, target types.Currency, log *zap.Logger) (funded int, drained int, _ error) {
	// sanity check the input
	if len(accs) > proto.MaxAccountBatchSize {
		return 0, 0, errors.New("too many accounts")
	} else if len(contractIDs) == 0 {
		return 0, 0, errors.New("no contract provided")
	} else if len(accs) == 0 {
		return 0, 0, nil
	}

	// prepare account keys
	accountKeys := make([]proto.Account, len(accs))
	for i, account := range accs {
		accountKeys[i] = account.AccountKey
	}

	// iterate over contracts
	for _, contractID := range contractIDs {
		contractLog := log.With(zap.Stringer("contractID", contractID))

		var res rhp.RPCReplenishAccountsResult
		var err error
		err = f.rev.withRevision(ctx, contractID, func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error) {
			if contract.Revision.RenterOutput.Value.Cmp(target) < 0 {
				return rhp.ContractRevision{}, proto.Usage{}, ErrContractInsufficientFunds
			}

			batchSize := int(max(1, min(contract.Revision.RenterOutput.Value.Div(target).Big().Uint64(), proto.MaxAccountBatchSize)))
			maxEnd := min(len(accountKeys), funded+batchSize)
			// execute replenish RPC
			res, err = f.client.ReplenishAccounts(ctx, f.signer, f.chain, rhp.RPCReplenishAccountsParams{
				Accounts: accountKeys[funded:maxEnd],
				Target:   target,
				Contract: contract,
			})
			if err != nil {
				return rhp.ContractRevision{}, proto.Usage{}, err
			}
			funded = maxEnd
			return rhp.ContractRevision{
				ID:       contractID,
				Revision: res.Revision,
			}, res.Usage, nil
		})
		if errors.Is(err, ErrContractInsufficientFunds) {
			contractLog.Debug("contract has insufficient funds", zap.Error(err))
			drained++
			continue
		} else if errors.Is(err, ErrContractNotRevisable) {
			contractLog.Debug("contract is not revisable", zap.Error(err)) // sanity check
			drained++
			continue
		} else if err != nil {
			contractLog.Debug("failed to replenish accounts", zap.Error(err))
			continue
		} else if res.Revision.RemainingAllowance().Cmp(target) < 0 {
			contractLog.Debug("contract was drained by replenish RPC",
				zap.Stringer("remainingAllowance", res.Revision.RemainingAllowance()),
				zap.Stringer("target", target))
			drained++
		}

		if funded == len(accountKeys) {
			break
		}
	}

	return funded, drained, nil
}
