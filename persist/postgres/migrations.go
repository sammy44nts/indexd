package postgres

import (
	"context"

	"go.uber.org/zap"
)

var migrations = []func(context.Context, *txn, *zap.Logger) error{
	// adds the app_connect_keys table
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `CREATE TABLE app_connect_keys (
    app_key TEXT PRIMARY KEY,
    use_description TEXT NOT NULL,
    remaining_uses INTEGER NOT NULL,
    total_uses INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_used TIMESTAMP WITH TIME ZONE
);`)
		return err
	},
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `CREATE INDEX sectors_uploaded_at_unpinned_idx ON sectors(uploaded_at) WHERE host_id IS NOT NULL AND contract_sectors_map_id IS NULL;`)
		return err
	},
	// adds the service_account column to accounts
	func(ctx context.Context, tx *txn, _ *zap.Logger) error {
		_, err := tx.Exec(ctx, `ALTER TABLE accounts ADD COLUMN service_account BOOLEAN NOT NULL DEFAULT FALSE;`)
		if err != nil {
			return err
		}
		// NOTE: the following is not perfect since a service account might not
		// yet have any rows in the service_accounts table, but it's the best we
		// can do
		_, err = tx.Exec(ctx, `UPDATE accounts SET service_account = TRUE WHERE EXISTS (SELECT 1 FROM service_accounts sa WHERE sa.account_id = accounts.id)`)
		return err
	},
}
