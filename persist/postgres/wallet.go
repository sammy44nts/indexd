package postgres

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

var _ wallet.SingleAddressStore = (*Store)(nil)

// Tip returns the last scanned index.
func (s *Store) Tip() (ci types.ChainIndex, err error) {
	err = s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		return tx.QueryRow(ctx, `SELECT last_scanned_index FROM global_settings`).Scan((*sqlChainIndex)(&ci))
	})
	return
}

// UnspentSiacoinElements returns a list of all unspent siacoin outputs
// including immature outputs.
func (s *Store) UnspentSiacoinElements() (sces []types.SiacoinElement, err error) {
	err = s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `SELECT output_id, value, address, merkle_proof, leaf_index, maturity_height FROM wallet_siacoin_elements`)
		if err != nil {
			return fmt.Errorf("failed to query unspent siacoin elements: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var se types.SiacoinElement
			if err := rows.Scan((*sqlHash256)(&se.ID), (*sqlCurrency)(&se.SiacoinOutput.Value), (*sqlHash256)(&se.SiacoinOutput.Address), (*sqlMerkleProof)(&se.StateElement.MerkleProof), &se.StateElement.LeafIndex, &se.MaturityHeight); err != nil {
				return fmt.Errorf("failed to scan unspent siacoin element: %w", err)
			}
			sces = append(sces, se)
		}
		return rows.Err()
	})
	return
}

// WalletEvents returns a paginated list of transactions ordered by maturity
// height, descending. If no more transactions are available, (nil, nil) should
// be returned.
func (s *Store) WalletEvents(offset, limit int) (events []wallet.Event, err error) {
	ci, err := s.Tip()
	if err != nil {
		return nil, fmt.Errorf("failed to get last scanned index: %w", err)
	}

	err = s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `SELECT chain_index, maturity_height, event_id, event_type, event_data FROM wallet_events ORDER BY maturity_height DESC, id DESC LIMIT $1 OFFSET $2`, limit, offset)
		if err != nil {
			return fmt.Errorf("failed to query wallet events: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var event wallet.Event
			err := rows.Scan((*sqlChainIndex)(&event.Index), &event.MaturityHeight, (*sqlHash256)(&event.ID), &event.Type, sqlDecodeEvent(&event.Data))
			if err != nil {
				return fmt.Errorf("failed to scan wallet event: %w", err)
			}
			if ci.Height > event.Index.Height {
				event.Confirmations = ci.Height - event.Index.Height
			}
			events = append(events, event)
		}
		return rows.Err()
	})
	return
}

// WalletEventCount returns the total number of events relevant to the wallet.
func (s *Store) WalletEventCount() (count uint64, err error) {
	err = s.transaction(context.Background(), func(ctx context.Context, tx *txn) error {
		err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM wallet_events`).Scan(&count)
		return err
	})
	return
}
