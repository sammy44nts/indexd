package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/slabs"
)

const (
	minRepairBackoff = time.Hour
	maxRepairBackoff = 24 * time.Hour
	// maxBadParityShards is the maximum proportion of parity shards that can be
	// on bad hosts when pinning a slab.
	maxBadParityShards = 0.2
)

// MarkFailingSectorsLost marks sectors as lost if they have failed the
// integrity checks >= maxChecks times.
func (s *Store) MarkFailingSectorsLost(ctx context.Context, hostKey types.PublicKey, maxChecks uint) error {
	const batchSize = 1000
	for {
		updated, err := s.markFailingSectorsLostBatch(ctx, hostKey, maxChecks, batchSize)
		if err != nil {
			return err
		} else if updated < batchSize {
			break
		}
	}
	return nil
}

// MarkSectorsLost marks the sectors as lost by setting both the contract ID and
// host ID to NULL. This is meant to be used in 2 cases:
// - The host reports that the sector is lost (e.g. when pinning it, during the integrity check or when fetching it for migration)
// - The host has failed the integrity check for that sector enough times
func (s *Store) MarkSectorsLost(ctx context.Context, hostKey types.PublicKey, roots []types.Hash256) error {
	if len(roots) == 0 {
		return nil
	}

	sqlRoots := make([]sqlHash256, len(roots))
	for i, root := range roots {
		sqlRoots[i] = sqlHash256(root)
	}

	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var hostID int64
		err := tx.QueryRow(ctx, "SELECT id FROM hosts WHERE public_key = $1", sqlPublicKey(hostKey)).Scan(&hostID)
		if err != nil {
			return fmt.Errorf("failed to get host ID: %w", err)
		}

		rows, err := tx.Query(ctx, `
			SELECT id, contract_sectors_map_id
			FROM sectors
			WHERE host_id = $1 AND sector_root = ANY($2)`, hostID, sqlRoots)
		if err != nil {
			return fmt.Errorf("failed to query sectors: %w", err)
		}
		defer rows.Close()

		sectorIDs, pinned, unpinned, err := scanSectorIDs(rows)
		if err != nil {
			return fmt.Errorf("failed to scan sectors: %w", err)
		} else if len(sectorIDs) == 0 {
			return nil
		}

		totalLost := pinned + unpinned
		if _, err := tx.Exec(ctx, `UPDATE sectors SET host_id = NULL, contract_sectors_map_id = NULL WHERE id = ANY($1)`, sectorIDs); err != nil {
			return fmt.Errorf("failed to mark sectors as lost: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE hosts SET lost_sectors = lost_sectors + $1 WHERE id = $2`, totalLost, hostID); err != nil {
			return fmt.Errorf("failed to increment host's lost sectors: %w", err)
		}
		if err := updateSectorStats(ctx, tx, -pinned, -unpinned, totalLost); err != nil {
			return fmt.Errorf("failed to update sector stats: %w", err)
		}
		return nil
	})
	return err
}

// MarkSectorsUnpinnable sets the host ID for sectors that haven't been pinned
// by the threshold time to NULL.
func (s *Store) MarkSectorsUnpinnable(ctx context.Context, threshold time.Time) error {
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		res, err := tx.Exec(ctx, `
            UPDATE sectors
            SET host_id = NULL
            WHERE host_id IS NOT NULL
	            AND contract_sectors_map_id IS NULL
	            AND uploaded_at <= $1`, threshold)
		if err != nil {
			return fmt.Errorf("failed to prune unpinnable sectors: %w", err)
		} else if unpinnable := res.RowsAffected(); unpinnable > 0 {
			if err := incrementNumUnpinnableSectors(ctx, tx, unpinnable); err != nil {
				return fmt.Errorf("failed to increment unpinnable sectors: %w", err)
			} else if err := incrementNumUnpinnedSectors(ctx, tx, -unpinnable); err != nil {
				return fmt.Errorf("failed to decrement unpinned sectors: %w", err)
			}
		}
		return nil
	})
	return err
}

// MigrateSector updates a sector that was just migrated in the database to be
// linked to the new host identified by 'hostKey'. This will reset the contract
// ID since a freshly migrated sector isn't pinned yet. To pin a sector
// 'PinSectors' is used. If the host is not found, e.g. due to being deleted in
// the meantime, this operation is a no-op.
func (s *Store) MigrateSector(ctx context.Context, root types.Hash256, hostKey types.PublicKey) (migrated bool, err error) {
	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var hostID sql.NullInt64
		var contractMapID sql.NullInt64
		err := tx.QueryRow(ctx, `
			SELECT host_id, contract_sectors_map_id
			FROM sectors
			WHERE sector_root = $1`, sqlHash256(root)).Scan(&hostID, &contractMapID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil // not migrated
		}

		resp, err := tx.Exec(ctx, `
			UPDATE sectors
			SET host_id = hosts.id, contract_sectors_map_id = NULL, consecutive_failed_checks = 0, num_migrated = num_migrated + 1, uploaded_at=NOW()
			FROM hosts
			WHERE sector_root = $1 AND hosts.public_key = $2
		`, sqlHash256(root), sqlPublicKey(hostKey))
		if err != nil {
			return err
		} else if resp.RowsAffected() == 0 {
			return nil
		}

		migrated = true
		if err := incrementNumMigratedSectors(ctx, tx); err != nil {
			return fmt.Errorf("failed to increment number of migrated sectors: %w", err)
		} else if contractMapID.Valid {
			// sector was pinned before, update stats
			if err := incrementNumPinnedSectors(ctx, tx, -1); err != nil {
				return fmt.Errorf("failed to decrement pinned sectors: %w", err)
			} else if err := incrementNumUnpinnedSectors(ctx, tx, 1); err != nil {
				return fmt.Errorf("failed to increment unpinned sectors: %w", err)
			}
		} else if !hostID.Valid {
			// sector was unpinnable before, update stats
			if err := incrementNumUnpinnableSectors(ctx, tx, -1); err != nil {
				return fmt.Errorf("failed to decrement unpinnable sectors: %w", err)
			} else if err := incrementNumUnpinnedSectors(ctx, tx, 1); err != nil {
				return fmt.Errorf("failed to increment unpinned sectors: %w", err)
			}
		}

		return nil
	})
	return
}

// PinSectors pins a batch of sector roots to a given contract. This also
// updates the host the sector is associated with to the host that we have the
// contract with. That way, we can avoid a race where the host changes in the
// meantime and the contract then no longer matches the host.
func (s *Store) PinSectors(ctx context.Context, contractID types.FileContractID, roots []types.Hash256) error {
	if len(roots) == 0 {
		return nil
	}

	sqlRoots := make([]sqlHash256, len(roots))
	for i, root := range roots {
		sqlRoots[i] = sqlHash256(root)
	}

	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var hostID, contractMapID int64
		err := tx.QueryRow(ctx, `
			SELECT c.host_id, csm.id
			FROM contract_sectors_map csm
			INNER JOIN contracts c ON c.contract_id = csm.contract_id
			WHERE csm.contract_id = $1
		`, sqlHash256(contractID)).Scan(&hostID, &contractMapID)
		if errors.Is(err, sql.ErrNoRows) {
			return contracts.ErrNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get contract's host: %w", err)
		}

		rows, err := tx.Query(ctx, `
			SELECT id, contract_sectors_map_id
			FROM sectors
			WHERE sector_root = ANY($1) AND (contract_sectors_map_id IS NULL OR contract_sectors_map_id != $2)`, sqlRoots, contractMapID)
		if err != nil {
			return fmt.Errorf("failed to query sectors: %w", err)
		}
		defer rows.Close()

		sectorIDs, _, unpinned, err := scanSectorIDs(rows)
		if err != nil {
			return fmt.Errorf("failed to scan sectors: %w", err)
		} else if len(sectorIDs) == 0 {
			return nil
		}

		if _, err := tx.Exec(ctx, `UPDATE sectors SET host_id = $1, contract_sectors_map_id = $2 WHERE id = ANY($3)`, hostID, contractMapID, sectorIDs); err != nil {
			return fmt.Errorf("failed to pin sectors: %w", err)
		} else if unpinned > 0 {
			if err := incrementNumPinnedSectors(ctx, tx, unpinned); err != nil {
				return fmt.Errorf("failed to update number of pinned sectors: %w", err)
			} else if err := incrementNumUnpinnedSectors(ctx, tx, -unpinned); err != nil {
				return fmt.Errorf("failed to update number of unpinned sectors: %w", err)
			}
		}

		return nil
	})
}

// RecordIntegrityCheck records the result of integrity checks for the given
// sectors stored on the given host.
func (s *Store) RecordIntegrityCheck(ctx context.Context, success bool, nextCheck time.Time, hostKey types.PublicKey, roots []types.Hash256) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		sqlRoots := make([]sqlHash256, len(roots))
		for i, root := range roots {
			sqlRoots[i] = sqlHash256(root)
		}
		var err error
		if success {
			_, err = tx.Exec(ctx, `
				UPDATE sectors
				SET next_integrity_check = $1, consecutive_failed_checks = 0
				WHERE host_id = (SELECT id FROM hosts WHERE public_key = $2) AND
					sector_root = ANY($3)
			`, nextCheck, sqlPublicKey(hostKey), sqlRoots)
		} else {
			_, err = tx.Exec(ctx, `
				UPDATE sectors
				SET next_integrity_check = $1, consecutive_failed_checks = consecutive_failed_checks + 1
				WHERE host_id = (SELECT id FROM hosts WHERE public_key = $2) AND
					sector_root = ANY($3)
			`, nextCheck, sqlPublicKey(hostKey), sqlRoots)
		}
		return err
	})
}

// SectorsForIntegrityCheck returns up to `limit` sectors that are due for an
// integrity check.
func (s *Store) SectorsForIntegrityCheck(ctx context.Context, hostKey types.PublicKey, limit int) ([]types.Hash256, error) {
	var sectors []types.Hash256
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
			WITH hid AS (
				SELECT id FROM hosts WHERE public_key = $1
			)
			SELECT sector_root
			FROM sectors
			WHERE
				host_id = (SELECT id FROM hid)
				AND next_integrity_check <= NOW()
			ORDER BY next_integrity_check ASC
			LIMIT $2
		`, sqlPublicKey(hostKey), limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var root types.Hash256
			if err := rows.Scan((*sqlHash256)(&root)); err != nil {
				return err
			}
			sectors = append(sectors, root)
		}
		return rows.Err()
	})
	return sectors, err
}

// UnhealthySlabs returns the IDs of slabs which have at least one sector that
// needs to be migrated and have not been abandoned.
//
// The condition for such a sector is that it's either not stored on a host or
// it's not pinned to a good contract.
//
// NOTE: Subsequent calls to this function do not return the same slabs because
// a minimum of 1 hour must pass between consecutive migration attempts. The
// caller is expected to update the slab with the repair result after the
// migration was attempted.
//
// NOTE: For the sake of scalability, we don't prioritize slabs based on their
// health but simply return the slabs that have been waiting the longest for a
// repair first.
func (s *Store) UnhealthySlabs(ctx context.Context, limit int) (unhealthy []slabs.SlabID, err error) {
	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		const query = `SELECT s.id, s.digest
			FROM slabs s
			WHERE s.next_repair_attempt < NOW()
				AND EXISTS (
					SELECT 1
					FROM slab_sectors ss
					JOIN sectors sec ON sec.id = ss.sector_id
					LEFT JOIN contract_sectors_map csm ON csm.id = sec.contract_sectors_map_id
					LEFT JOIN contracts c ON c.contract_id = csm.contract_id
					WHERE ss.slab_id = s.id
						AND (
						sec.host_id IS NULL
						OR (sec.contract_sectors_map_id IS NOT NULL
							AND (c.good = FALSE OR c.state NOT IN (0, 1)))
						)
				)
			ORDER BY s.next_repair_attempt ASC
			LIMIT $1;`
		rows, err := tx.Query(ctx, query, limit)
		if err != nil {
			return fmt.Errorf("failed to query unhealthy slabs: %w", err)
		}
		defer rows.Close()

		var slabIDs []int64
		for rows.Next() {
			var id int64
			var slabID slabs.SlabID
			if err := rows.Scan(&id, (*sqlHash256)(&slabID)); err != nil {
				return fmt.Errorf("failed to scan unhealthy slab: %w", err)
			}
			unhealthy = append(unhealthy, slabID)
			slabIDs = append(slabIDs, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to get unhealthy slabs: %w", err)
		} else if len(slabIDs) == 0 {
			return nil // no unhealthy slabs
		}

		// update next repair attempt time
		_, err = tx.Exec(ctx, `UPDATE slabs SET next_repair_attempt = $1 WHERE id = ANY($2)`, time.Now().Add(minRepairBackoff), slabIDs)
		if err != nil {
			return fmt.Errorf("failed to update next repair attempt: %w", err)
		}

		return nil
	})
	return
}

// UnpinnedSectors returns up to 'limit' sectors which have been uploaded to a host but
// not pinned to a contract yet.
func (s *Store) UnpinnedSectors(ctx context.Context, hostKey types.PublicKey, limit int) ([]types.Hash256, error) {
	roots := make([]types.Hash256, 0, limit)
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		rows, err := tx.Query(ctx, `
			WITH hid AS (
				SELECT id FROM hosts WHERE public_key = $1
			)
			SELECT sector_root
			FROM sectors
				WHERE host_id = (SELECT id FROM hid)
				AND contract_sectors_map_id IS NULL
			ORDER BY uploaded_at ASC
			LIMIT $2
		`, sqlPublicKey(hostKey), limit)
		if err != nil {
			return fmt.Errorf("failed to query sectors: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var root types.Hash256
			err = rows.Scan((*sqlHash256)(&root))
			if err != nil {
				return fmt.Errorf("failed to scan unpinned sector: %w", err)
			}
			roots = append(roots, root)
		}
		return rows.Err()
	})
	return roots, err
}

// markFailingSectorsLostBatch marks a batch of failing sectors as lost. We have
// to batch it because we first need to select all sectors to update in order to
// correctly updated the pinned sectors statistics.
func (s *Store) markFailingSectorsLostBatch(ctx context.Context, hostKey types.PublicKey, maxChecks, batchSize uint) (int64, error) {
	var totalLost int64
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var hostID int64
		err := tx.QueryRow(ctx, "SELECT id FROM hosts WHERE public_key = $1", sqlPublicKey(hostKey)).Scan(&hostID)
		if err != nil {
			return fmt.Errorf("failed to get host ID: %w", err)
		}

		rows, err := tx.Query(ctx, `
			SELECT id, contract_sectors_map_id
			FROM sectors
			WHERE host_id = $1 AND consecutive_failed_checks >= $2
			LIMIT $3
		`, hostID, maxChecks, batchSize)
		if err != nil {
			return fmt.Errorf("failed to mark failing sectors as lost: %w", err)
		}
		defer rows.Close()

		sectorIDs, pinned, unpinned, err := scanSectorIDs(rows)
		if err != nil {
			return fmt.Errorf("failed to scan sectors: %w", err)
		} else if len(sectorIDs) == 0 {
			return nil
		}

		totalLost = pinned + unpinned
		if _, err := tx.Exec(ctx, `UPDATE sectors SET host_id = NULL, contract_sectors_map_id = NULL WHERE id = ANY($1)`, sectorIDs); err != nil {
			return fmt.Errorf("failed to mark failing sectors as lost: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE hosts SET lost_sectors = lost_sectors + $1 WHERE id = $2`, totalLost, hostID); err != nil {
			return fmt.Errorf("failed to mark failing sectors as lost: %w", err)
		}
		if err := updateSectorStats(ctx, tx, -pinned, -unpinned, totalLost); err != nil {
			return fmt.Errorf("failed to update sector stats: %w", err)
		}
		return nil
	}); err != nil {
		return 0, err
	}

	return totalLost, nil
}

func updateSectorStats(ctx context.Context, tx *txn, pinnedDelta, unpinnedDelta, unpinnableDelta int64) error {
	if err := incrementNumPinnedSectors(ctx, tx, pinnedDelta); err != nil {
		return fmt.Errorf("failed to update pinned sectors: %w", err)
	}
	if err := incrementNumUnpinnedSectors(ctx, tx, unpinnedDelta); err != nil {
		return fmt.Errorf("failed to update unpinned sectors: %w", err)
	}
	if err := incrementNumUnpinnableSectors(ctx, tx, unpinnableDelta); err != nil {
		return fmt.Errorf("failed to update unpinnable sectors: %w", err)
	}
	return nil
}

// scanSectorIDs scans sector IDs from the given rows. It also counts how many
// of the sectors are currently pinned and unpinned.
func scanSectorIDs(rows *rows) (sectorIDs []int64, pinned, unpinned int64, err error) {
	for rows.Next() {
		var sectorID int64
		var contractMapID sql.NullInt64
		if err := rows.Scan(&sectorID, &contractMapID); err != nil {
			return nil, 0, 0, fmt.Errorf("failed to scan sector row: %w", err)
		} else if contractMapID.Valid {
			pinned++
		} else {
			unpinned++
		}
		sectorIDs = append(sectorIDs, sectorID)
	}
	err = rows.Err()
	return
}
