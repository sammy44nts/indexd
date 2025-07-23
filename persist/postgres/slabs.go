package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
)

// Slab returns the slab with the given ID from the database.
func (s *Store) Slab(ctx context.Context, digest slabs.SlabID) (slab slabs.Slab, err error) {
	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var slabID int64
		err := tx.QueryRow(ctx, `
			SELECT s.id, s.encryption_key, s.min_shards, s.pinned_at
			FROM slabs s
			INNER JOIN account_slabs ac ON s.id = ac.slab_id
			INNER JOIN accounts a ON a.id = ac.account_id
			WHERE digest = $1`, sqlHash256(digest)).Scan(&slabID, (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards, &slab.PinnedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrSlabNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get slab %q: %w", digest, err)
		}

		sectorsBatch := &pgx.Batch{}
		sectorsBatch.Queue(`
			SELECT s.sector_root, h.public_key, csm.contract_id
			FROM sectors s
			INNER JOIN slab_sectors ss ON s.id = ss.sector_id
			LEFT JOIN hosts h ON h.id = s.host_id
			LEFT JOIN contract_sectors_map csm ON s.contract_sectors_map_id = csm.id
			WHERE ss.slab_id = $1
			ORDER BY ss.slab_index ASC`, slabID).Query(func(rows pgx.Rows) error {
			defer rows.Close()
			for rows.Next() {
				var sector slabs.Sector
				var hostKey sql.Null[sqlPublicKey]
				var contractID sql.Null[sqlHash256]

				if err := rows.Scan((*sqlHash256)(&sector.Root), &hostKey, &contractID); err != nil {
					return fmt.Errorf("failed to scan sector: %w", err)
				}
				if hostKey.Valid {
					sector.HostKey = (*types.PublicKey)(&hostKey.V)
				}
				if contractID.Valid {
					sector.ContractID = (*types.FileContractID)(&contractID.V)
				}
				slab.Sectors = append(slab.Sectors, sector)
			}
			return rows.Err()
		})
		if err := tx.Tx.SendBatch(ctx, sectorsBatch).Close(); err != nil {
			return fmt.Errorf("failed to get slab sectors: %w", err)
		}
		slab.ID = digest
		return nil
	})
	return
}

// PinnedSlab retrieves a pinned slab from the database by its ID.
func (s *Store) PinnedSlab(ctx context.Context, slabID slabs.SlabID) (slab slabs.PinnedSlab, err error) {
	slab.ID = slabID
	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var dbID int64
		err = tx.QueryRow(ctx, `SELECT s.id, s.encryption_key, s.min_shards FROM slabs s WHERE digest = $1`, sqlHash256(slabID)).Scan(
			&dbID, (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrSlabNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get slab %q: %w", slabID, err)
		}

		rows, err := tx.Query(ctx, `SELECT s.sector_root, h.public_key
FROM slab_sectors ss
INNER JOIN sectors s ON (s.id = ss.sector_id)
LEFT JOIN hosts h ON (h.id = s.host_id)
WHERE ss.slab_id = $1 AND s.host_id IS NOT NULL
ORDER BY ss.slab_index ASC`, dbID)
		if err != nil {
			return fmt.Errorf("failed to get slab sectors: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var sector slabs.PinnedSector

			if err := rows.Scan((*sqlHash256)(&sector.Root), (*sqlPublicKey)(&sector.HostKey)); err != nil {
				return fmt.Errorf("failed to scan sector: %w", err)
			}
			slab.Sectors = append(slab.Sectors, sector)
		}

		if len(slab.Sectors) < int(slab.MinShards) {
			return fmt.Errorf("recovery requires at least %d sectors, slab has %d sectors: %w", slab.MinShards, len(slab.Sectors), slabs.ErrUnrecoverable)
		}
		return rows.Err()
	})
	return
}
