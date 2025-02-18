package postgres

import (
	"context"

	"go.sia.tech/coreutils/wallet"
)

type updateTx struct {
	ctx context.Context
	tx  *txn
}

// ApplyChainUpdate applies a chain update to the store.
func (s *Store) ApplyChainUpdate(ctx context.Context, fn func(tx wallet.UpdateTx) error) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		return fn(&updateTx{ctx: ctx, tx: tx})
	})
}
