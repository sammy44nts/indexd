package contracts

import (
	"context"
	"time"

	"go.sia.tech/core/consensus"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
)

var (
	MinRemainingStorage   uint64         = minRemainingStorage
	MaxContractSize       uint64         = maxContractSize
	MinContractGrowthRate uint64         = minContractGrowthRate
	MaxContractGrowthRate uint64         = maxContractGrowthRate
	MinAllowance          types.Currency = minAllowance
	MinHostCollateral     types.Currency = minHostCollateral
	PruneIntervalSuccess                 = pruneIntervalSuccess
	PruneIntervalFailure                 = pruneIntervalFailure
)

var (
	ShouldReplaceContract  = shouldReplaceContract
	NewTestContractManager = newContractManager

	ContractFunding           = contractFunding
	IsBeyondMaxRevisionHeight = isBeyondMaxRevisionHeight
)

const DefaultRevisionSubmissionBuffer = defaultRevisionSubmissionBuffer

type CandidateContract = candidateContract

type TestLatestRevisionClient interface {
	LatestRevision(ctx context.Context, hostKey types.PublicKey, contractID types.FileContractID) (proto.RPCLatestRevisionResponse, error)
}

type TestRevisionManager struct {
	rm *revisionManager
}

func NewTestRevisionManager(client TestLatestRevisionClient, chain ChainManager, store RevisionStore, buffer uint64, log *zap.Logger) *TestRevisionManager {
	return &TestRevisionManager{
		rm: newRevisionManager(client, chain, store, buffer, log),
	}
}

func (trm *TestRevisionManager) WithRevision(ctx context.Context, contractID types.FileContractID, reviseFn func(contract rhp.ContractRevision) (rhp.ContractRevision, proto.Usage, error)) error {
	return trm.rm.withRevision(ctx, contractID, reviseFn)
}

func NewCandidateContract(goodForAppend, goodForFunding, goodForRefresh error) CandidateContract {
	return CandidateContract{
		goodForAppend:  goodForAppend,
		goodForFunding: goodForFunding,
		goodForRefresh: goodForRefresh,
	}
}

func (cc CandidateContract) GoodForAppend() error { return cc.goodForAppend }

func (cc CandidateContract) GoodForFunding() error { return cc.goodForFunding }

func (cc CandidateContract) GoodForRefresh() error { return cc.goodForRefresh }

type TestUpdateTx struct {
	updateTx *updateTx
}

func NewTestUpdateTx(tx UpdateTx) *TestUpdateTx {
	return &TestUpdateTx{
		updateTx: &updateTx{
			UpdateTx:       tx,
			knownContracts: make(map[types.FileContractID]bool),
		},
	}
}

func UpdateContractElementProofs(tx *TestUpdateTx, updater wallet.ProofUpdater) error {
	return updateContractElementProofs(tx.updateTx, updater)
}

func (cm *ContractManager) ApplyV2ContractDiffs(tx *TestUpdateTx, diffs []consensus.V2FileContractElementDiff) error {
	return cm.applyV2ContractDiffs(tx.updateTx, diffs)
}

func (cm *ContractManager) RevertV2ContractDiffs(tx *TestUpdateTx, diffs []consensus.V2FileContractElementDiff) error {
	return cm.revertV2ContractDiffs(tx.updateTx, diffs)
}

func (cm *ContractManager) PerformContractFormation(ctx context.Context, ms MaintenanceSettings, blockHeight uint64, log *zap.Logger) error {
	return cm.performContractFormation(ctx, ms, blockHeight, log)
}

func (cm *ContractManager) PerformContractRenewals(ctx context.Context, period, renewWindow uint64, log *zap.Logger) error {
	return cm.performContractRenewals(ctx, period, renewWindow, log)
}

func (cm *ContractManager) PerformAccountFunding(ctx context.Context, force bool, log *zap.Logger) error {
	return cm.performAccountFunding(ctx, force, log)
}

func (cm *ContractManager) PerformBroadcastContractRevisions(ctx context.Context, log *zap.Logger) error {
	return cm.performBroadcastContractRevisions(ctx, log)
}

func (cm *ContractManager) PerformSectorPinningOnHost(ctx context.Context, host hosts.Host, log *zap.Logger) error {
	return cm.performSectorPinningOnHost(ctx, host, log)
}

func (cm *ContractManager) PerformContractPruningOnHost(ctx context.Context, host hosts.Host, log *zap.Logger) error {
	return cm.performContractPruningOnHost(ctx, host, log)
}

func (cm *ContractManager) BlockBadHosts(ctx context.Context) error {
	return cm.blockBadHosts(ctx)
}

func (cm *ContractManager) SetRevisionBroadcastInterval(d time.Duration) {
	cm.revisionBroadcastInterval = d
}
