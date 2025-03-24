package postgres

import (
	"context"

	"go.sia.tech/indexd/pins"
)

// PinnedSettings returns the pinned settings.
func (s *Store) PinnedSettings(ctx context.Context) (ps pins.PinnedSettings, err error) {
	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		query := `SELECT pins_currency, pins_min_collateral, pins_max_storage_price, pins_max_ingress_price, pins_max_egress_price FROM global_settings`
		return tx.QueryRow(ctx, query).Scan(
			&ps.Currency,
			&ps.MinCollateral,
			&ps.MaxStoragePrice,
			&ps.MaxIngressPrice,
			&ps.MaxEgressPrice,
		)
	})
	return
}

// UpdatePinnedSettings updates the pinned settings.
func (s *Store) UpdatePinnedSettings(ctx context.Context, ps pins.PinnedSettings) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		query := `UPDATE global_settings SET pins_currency = $1, pins_min_collateral = $2, pins_max_storage_price = $3, pins_max_ingress_price = $4, pins_max_egress_price = $5`
		_, err := tx.Exec(ctx, query, ps.Currency, ps.MinCollateral, ps.MaxStoragePrice, ps.MaxIngressPrice, ps.MaxEgressPrice)
		return err
	})
}
