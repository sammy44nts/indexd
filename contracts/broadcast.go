package contracts

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

func (cm *ContractManager) performBroadcastContractRevisions(ctx context.Context, log *zap.Logger) error {
	broadcastLog := log.Named("broadcast")

	const batchSize = 50
	for offset := 0; ; offset += batchSize {
		contracts, err := cm.store.Contracts(ctx, offset, batchSize, WithGood(true), WithRevisable(true))
		if err != nil {
			return fmt.Errorf("failed to fetch contracts for broadcasting: %w", err)
		}

		var wg sync.WaitGroup
		for _, contract := range contracts {
			if !contract.NeedsBroadcast(cm.revisionBroadcastInterval) {
				continue
			}

			wg.Add(1)
			go func(contract Contract, log *zap.Logger) {
				defer wg.Done()

				ctx, cancel := context.WithTimeout(ctx, time.Minute)
				defer cancel()

				err := cm.broadcastContractRevision(ctx, contract, log)
				if err != nil {
					broadcastLog.Error("failed to broadcast contract revision", zap.Error(err))
					return
				}

				err = cm.store.MarkSuccessfulBroadcast(ctx, contract.ID)
				if err != nil {
					broadcastLog.Error("failed to mark contract as broadcasted", zap.Error(err))
				}
			}(contract, broadcastLog.With(zap.Stringer("contractID", contract.ID)))
		}
		wg.Wait()

		if len(contracts) < batchSize {
			break
		}
	}

	return nil
}

func (cm *ContractManager) broadcastContractRevision(ctx context.Context, contract Contract, log *zap.Logger) error {
	// fetch the host
	host, err := cm.store.Host(ctx, contract.HostKey)
	if err != nil {
		return fmt.Errorf("failed to fetch host: %w", err)
	}

	// fetch contract element
	fce, err := cm.store.ContractElement(ctx, contract.ID)
	if err != nil {
		return fmt.Errorf("failed to fetch contract element: %w", err)
	}

	// fetch the latest revision
	resp, err := cm.contractor.LatestRevision(ctx, contract.HostKey, host.SiamuxAddr(), contract.ID)
	if err != nil {
		log.Warn("failed to fetch latest revision", zap.Error(err))
		return nil
	}

	// create the transaction
	const stdTxnSize = 1000
	fee := cm.cm.RecommendedFee().Mul64(stdTxnSize)
	txn := types.V2Transaction{
		MinerFee: fee,
		FileContractRevisions: []types.V2FileContractRevision{
			{
				Parent:   fce,
				Revision: resp.Contract,
			},
		},
	}

	// fund the transaction (only the fee) and sign it
	basis, toSign, err := cm.w.FundV2Transaction(&txn, fee, true)
	if err != nil {
		log.Debug("failed to fund transaction", zap.Error(err))
		return nil
	}
	cm.w.SignV2Inputs(&txn, toSign)

	// verify the transaction and add it to the transaction pool
	txnSet := []types.V2Transaction{txn}
	_, err = cm.cm.AddV2PoolTransactions(basis, txnSet)
	if err != nil {
		cm.w.ReleaseInputs(nil, txnSet)
		log.Debug("failed to add transaction set to the pool", zap.Error(err))
		return nil
	}

	// broadcast the transaction
	cm.s.BroadcastV2TransactionSet(basis, txnSet)
	return nil
}
