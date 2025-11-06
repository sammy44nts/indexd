package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/slabs"
)

// MarkSlabRepaired marks the slab as repaired or increments the failed repair
// count. If the repair was successful, the consecutive_failed_repairs counter
// is reset to zero. If the repair failed, the counter is incremented and the
// next repair attempt time is set using exponential backoff.
func (s *Store) MarkSlabRepaired(ctx context.Context, slabID slabs.SlabID, success bool) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		if success {
			if res, err := tx.Exec(ctx, `UPDATE slabs SET consecutive_failed_repairs = 0 WHERE digest = $1`, sqlHash256(slabID)); err != nil {
				return fmt.Errorf("failed to mark slab as repaired: %w", err)
			} else if res.RowsAffected() == 0 {
				return slabs.ErrSlabNotFound
			}
			return nil
		}

		var currentFailures int
		err := tx.QueryRow(ctx, `
			SELECT consecutive_failed_repairs
			FROM slabs
			WHERE digest = $1
			FOR UPDATE
		`, sqlHash256(slabID)).Scan(&currentFailures)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrSlabNotFound
		} else if err != nil {
			return fmt.Errorf("failed to fetch repair state: %w", err)
		}

		nextRepairBackoff := min(minRepairBackoff*time.Duration(1<<(currentFailures)), maxRepairBackoff)
		_, err = tx.Exec(ctx, `
			UPDATE slabs 
			SET consecutive_failed_repairs = $2, next_repair_attempt = $3 
			WHERE digest = $1`, sqlHash256(slabID), currentFailures+1, time.Now().Add(nextRepairBackoff))
		if err != nil {
			return fmt.Errorf("failed to update repair state: %w", err)
		}

		return nil
	})
}

// PinSlabs adds slabs to the database for pinning. The slabs are associated
// with the provided account.
func (s *Store) PinSlabs(ctx context.Context, account proto.Account, nextIntegrityCheck time.Time, toPin ...slabs.SlabPinParams) ([]slabs.SlabID, error) {
	var digests []slabs.SlabID
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var accountID int64
		var pinnedData, maxPinnedData uint64
		err := tx.QueryRow(ctx, "UPDATE accounts SET last_used = NOW() WHERE public_key = $1 RETURNING id, pinned_data, max_pinned_data", sqlPublicKey(account)).Scan(&accountID, &pinnedData, &maxPinnedData)
		if errors.Is(err, sql.ErrNoRows) {
			return accounts.ErrNotFound
		} else if err != nil {
			return err
		}

		hostRows, err := tx.Query(ctx, `SELECT h.public_key FROM contracts c INNER JOIN hosts h ON c.host_id = h.id WHERE state IN (0,1) AND renewed_to IS NULL AND good`)
		if err != nil {
			return fmt.Errorf("failed to get good hosts: %w", err)
		}
		defer hostRows.Close()

		goodHosts := make(map[types.PublicKey]struct{})
		for hostRows.Next() {
			var hk types.PublicKey
			if err := hostRows.Scan((*sqlPublicKey)(&hk)); err != nil {
				return fmt.Errorf("failed to scan host key: %w", err)
			}
			goodHosts[hk] = struct{}{}
		}
		if err := hostRows.Err(); err != nil {
			return fmt.Errorf("failed to get good host rows: %w", err)
		}

		for _, slab := range toPin {
			if slab.MinShards <= 0 || uint(len(slab.Sectors)) < slab.MinShards {
				return slabs.ErrMinShards
			}

			digest, err := slab.Digest()
			if err != nil {
				return fmt.Errorf("failed to calculate slab digest: %w", err)
			}
			digests = append(digests, digest)

			// insert slab
			var slabID int64
			var existingSlab bool
			err = tx.QueryRow(ctx, `
			INSERT INTO slabs (digest, encryption_key, min_shards)
			VALUES ($1, $2, $3)
			ON CONFLICT (digest) DO UPDATE SET pinned_at = NOW()
			RETURNING id
			`, sqlHash256(digest), sqlHash256(slab.EncryptionKey), slab.MinShards).Scan(&slabID)
			if err != nil {
				return err
			}

			// insert slab into join table
			res, err := tx.Exec(ctx, `
			INSERT INTO account_slabs (account_id, slab_id) VALUES ($1, $2)
			ON CONFLICT (account_id, slab_id) DO NOTHING
		`, accountID, slabID)
			if err != nil {
				return fmt.Errorf("failed to insert slab into account_slabs: %w", err)
			}

			// check if pinning the slab would exceed the account's storage limit
			// and if not, update the account's pinned data NOTE: we perform this
			// check here since we need to know if the slab is a new slab or whether
			// it was just repinned.
			if res.RowsAffected() > 0 {
				newPinnedData := pinnedData + slab.Size()
				if newPinnedData > maxPinnedData {
					return accounts.ErrStorageLimitExceeded
				}

				_, err := tx.Exec(ctx, `UPDATE accounts SET last_used=NOW(), pinned_data = $1 WHERE id = $2`, newPinnedData, accountID)
				if err != nil {
					return fmt.Errorf("failed to update account's pinned data: %w", err)
				}
			}

			// if the slab already existed, we don't need to insert the sectors
			if existingSlab {
				continue
			}

			// update slab stats
			if err := incrementNumSlabs(ctx, tx, 1); err != nil {
				return fmt.Errorf("failed to increment number of slabs: %w", err)
			}

			// insert slab's sectors in a single batch
			batch := &pgx.Batch{}
			for _, sector := range slab.Sectors {
				batch.Queue(`
				INSERT INTO sectors (sector_root, host_id, next_integrity_check)
				SELECT $1, h.id, $3
				FROM hosts h
				WHERE h.public_key = $2
				ON CONFLICT (sector_root) DO UPDATE SET uploaded_at=NOW()
				RETURNING id, (xmax = 0) AS inserted`,
					sqlHash256(sector.Root),
					sqlPublicKey(sector.HostKey),
					nextIntegrityCheck)
			}

			var badHosts int
			var unpinned int64
			br := tx.SendBatch(ctx, batch)
			sectorIDs := make([]int64, len(slab.Sectors))
			for i, sector := range slab.Sectors {
				if _, ok := goodHosts[sector.HostKey]; !ok {
					badHosts++
				}

				var inserted bool
				if err := br.QueryRow().Scan(&sectorIDs[i], &inserted); err != nil {
					br.Close()
					if errors.Is(err, sql.ErrNoRows) {
						return fmt.Errorf("unknown host %q for sector", sector.HostKey)
					}
					return fmt.Errorf("failed to insert sector %q: %w", sector.Root, err)
				} else if inserted {
					unpinned++
				}
			}
			br.Close()

			// if more than 20% of parity shards are on bad hosts, don't allow slab to be pinned
			parityShards := len(slab.Sectors) - int(slab.MinShards)
			if float64(badHosts) > maxBadParityShards*float64(parityShards) {
				return slabs.ErrBadHosts
			}

			// update number of unpinned sectors
			if unpinned > 0 {
				if err := incrementNumUnpinnedSectors(ctx, tx, unpinned); err != nil {
					return fmt.Errorf("failed to increment number of unpinned sectors: %w", err)
				}

				// insert slab sectors into join table
				batch = &pgx.Batch{}
				for i, sectorID := range sectorIDs {
					batch.Queue(`
				INSERT INTO slab_sectors (slab_id, slab_index, sector_id)
				VALUES ($1, $2, $3)
				ON CONFLICT (slab_id, slab_index) DO NOTHING
			`, slabID, i, sectorID)
				}
				if err = tx.Tx.SendBatch(ctx, batch).Close(); err != nil {
					return fmt.Errorf("failed to insert slab sectors: %w", err)
				}
			}
		}
		return nil
	})
	return digests, err
}

// PinnedSlab retrieves a pinned slab from the database by its ID.
func (s *Store) PinnedSlab(ctx context.Context, account proto.Account, slabID slabs.SlabID) (slab slabs.PinnedSlab, err error) {
	slab.ID = slabID
	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		if _, err := tx.Exec(ctx, `UPDATE accounts SET last_used = NOW() WHERE public_key = $1`, sqlPublicKey(account)); err != nil {
			return fmt.Errorf("failed to update last used: %w", err)
		}

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

// PruneSlabs prunes all pinned slabs of a user not currently connected to an
// object.
func (s *Store) PruneSlabs(ctx context.Context, account proto.Account) error {
	var id int64
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var err error
		id, err = accountID(ctx, tx, account)
		if err != nil {
			return fmt.Errorf("failed to get account ID: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	getSlabs := func(tx *txn, limit int64) ([]slabs.SlabID, error) {
		rows, err := tx.Query(ctx, `SELECT s.digest
FROM slabs s
JOIN account_slabs a ON s.id = a.slab_id
WHERE a.account_id = $1
	AND NOT EXISTS (
		SELECT 1
		FROM objects o
		JOIN object_slabs os ON o.id = os.object_id
		WHERE o.account_id = a.account_id
		AND os.slab_digest = s.digest
	)
LIMIT $2
`, id, limit)
		if err != nil {
			return nil, fmt.Errorf("failed to get unused slabs: %w", err)
		}
		defer rows.Close()

		var slabIDs []slabs.SlabID
		for rows.Next() {
			var slabID slabs.SlabID
			if err := rows.Scan((*sqlHash256)(&slabID)); err != nil {
				return nil, fmt.Errorf("failed to scan slab ID: %w", err)
			}
			slabIDs = append(slabIDs, slabID)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("failed to get slab IDs: %w", err)
		} else if len(slabIDs) == 0 {
			return nil, nil
		}
		return slabIDs, nil
	}

	var exhausted bool
	const batchSize = 100
	for !exhausted {
		err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
			slabIDs, err := getSlabs(tx, batchSize)
			if err != nil {
				return fmt.Errorf("failed to get slabs to unpin: %w", err)
			} else if len(slabIDs) < batchSize {
				exhausted = true
			}
			if err := s.unpinSlabs(ctx, tx, id, slabIDs); err != nil {
				return fmt.Errorf("failed to unpin slabs: %w", err)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// Slab retrieves a slab from the database by its ID.
func (s *Store) Slab(ctx context.Context, slabID slabs.SlabID) (slab slabs.Slab, err error) {
	err = s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		var dbID int64
		err = tx.QueryRow(ctx, `SELECT s.id, s.encryption_key, s.min_shards, s.pinned_at FROM slabs s WHERE digest = $1`, sqlHash256(slabID)).Scan(
			&dbID, (*sqlHash256)(&slab.EncryptionKey), &slab.MinShards, &slab.PinnedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return slabs.ErrSlabNotFound
		} else if err != nil {
			return fmt.Errorf("failed to get slab %q: %w", slabID, err)
		}
		slab.ID = slabID

		rows, err := tx.Query(ctx, `
			SELECT s.sector_root, h.public_key, csm.contract_id
			FROM sectors s
			INNER JOIN slab_sectors ss ON s.id = ss.sector_id
			LEFT JOIN hosts h ON h.id = s.host_id
			LEFT JOIN contract_sectors_map csm ON s.contract_sectors_map_id = csm.id
			WHERE ss.slab_id = $1
			ORDER BY ss.slab_index ASC`, dbID)
		if err != nil {
			return fmt.Errorf("failed to get slab sectors: %w", err)
		}
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
	return
}

// Slabs returns the slabs with the given IDs from the database.
func (s *Store) Slabs(ctx context.Context, account proto.Account, slabIDs []slabs.SlabID) ([]slabs.Slab, error) {
	if len(slabIDs) == 0 {
		return nil, nil
	}

	results := make([]slabs.Slab, len(slabIDs))
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		dbIDMap := make(map[int64]int)
		var dbIDs []int64
		slabBatch := &pgx.Batch{}
		for i, slabID := range slabIDs {
			slabBatch.Queue(`SELECT s.id, s.encryption_key, s.min_shards, s.pinned_at
				FROM slabs s
				INNER JOIN account_slabs ac ON s.id = ac.slab_id
				INNER JOIN accounts a ON a.id = ac.account_id
				WHERE digest = $1 AND a.public_key = $2`, sqlHash256(slabID), sqlPublicKey(account)).QueryRow(func(row pgx.Row) error {
				results[i].ID = slabID
				var dbID int64
				if err := row.Scan(&dbID, (*sqlHash256)(&results[i].EncryptionKey), &results[i].MinShards, &results[i].PinnedAt); err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						err = slabs.ErrSlabNotFound
					}
					return fmt.Errorf("failed to get slab %q: %w", slabID, err)
				}
				dbIDs = append(dbIDs, dbID)
				dbIDMap[dbID] = i
				return nil
			})
		}
		if err := tx.Tx.SendBatch(ctx, slabBatch).Close(); err != nil {
			return fmt.Errorf("failed to get slabs: %w", err)
		}

		sectorsBatch := &pgx.Batch{}
		for _, slabID := range dbIDs {
			sectorsBatch.Queue(`SELECT s.sector_root, h.public_key, csm.contract_id
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
					results[dbIDMap[slabID]].Sectors = append(results[dbIDMap[slabID]].Sectors, sector)
				}
				return rows.Err()
			})
		}
		if err := tx.Tx.SendBatch(ctx, sectorsBatch).Close(); err != nil {
			return fmt.Errorf("failed to get slab sectors: %w", err)
		}

		return nil
	})
	return results, err
}

// SlabIDs returns the IDs of slabs associated with the given account. The IDs
// are returned in descending order of the `pinned_at` timestamp, which is the
// time when the slab was pinned to the indexer.
func (s *Store) SlabIDs(ctx context.Context, account proto.Account, offset, limit int) ([]slabs.SlabID, error) {
	if err := validateOffsetLimit(offset, limit); err != nil {
		return nil, err
	} else if limit == 0 {
		return nil, nil
	}

	var ids []slabs.SlabID
	if err := s.transaction(ctx, func(ctx context.Context, tx *txn) (err error) {
		rows, err := tx.Query(ctx, `SELECT digest
			FROM slabs s
			INNER JOIN account_slabs ac ON s.id = ac.slab_id
			INNER JOIN accounts a ON a.id = ac.account_id
			WHERE a.public_key = $1
			ORDER BY s.pinned_at DESC
			LIMIT $2 OFFSET $3`, sqlPublicKey(account), limit, offset)
		if err != nil {
			return fmt.Errorf("failed to query slab digests: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var id slabs.SlabID
			if err := rows.Scan((*sqlHash256)(&id)); err != nil {
				return fmt.Errorf("failed to scan slab digest: %w", err)
			}
			ids = append(ids, id)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}

	return ids, nil
}

// UnpinSlab removes the association between the account and the given slab. If
// this slab was only owned by the given account, it will also be deleted.  The
// sectors of the slab will also be removed in that case.
func (s *Store) UnpinSlab(ctx context.Context, account proto.Account, slabID slabs.SlabID) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		id, err := accountID(ctx, tx, account)
		if err != nil {
			return fmt.Errorf("failed to get account ID: %w", err)
		}

		var exists bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM account_slabs WHERE account_id = $1 and slab_id = (SELECT id FROM slabs WHERE digest = $2))`, id, sqlHash256(slabID)).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to check if slab exists: %w", err)
		} else if !exists {
			return slabs.ErrSlabNotFound
		}

		if err := s.unpinSlabs(ctx, tx, id, []slabs.SlabID{slabID}); err != nil {
			return fmt.Errorf("failed to unpin slab: %w", err)
		}
		return nil
	})
}

func (s *Store) unpinSlabs(ctx context.Context, tx *txn, accountID int64, slabIDs []slabs.SlabID) error {
	var args []sqlHash256
	for _, slabID := range slabIDs {
		args = append(args, sqlHash256(slabID))
	}

	// delete the association between the account and the slab
	rows, err := tx.Query(ctx, `DELETE FROM account_slabs a
USING slabs s
WHERE a.account_id = $1
  AND a.slab_id = s.id
  AND s.digest = ANY($2)
RETURNING a.slab_id;`, accountID, args)
	if err != nil {
		return fmt.Errorf("failed to unpin slab: %w", err)
	}
	defer rows.Close()

	var sIDs []int64
	for rows.Next() {
		var sID int64
		if err := rows.Scan(&sID); err != nil {
			return fmt.Errorf("failed to scan slab ID: %w", err)
		}

		sIDs = append(sIDs, sID)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to get slab IDs: %w", err)
	}
	if len(sIDs) == 0 {
		return nil
	}

	// update the account's pinned data
	_, err = tx.Exec(ctx, `
			UPDATE accounts
			SET pinned_data = pinned_data - (
				SELECT COUNT(*) * $1
				FROM slab_sectors
				WHERE slab_id = ANY($2)
			)
			WHERE id = $3
		`, proto.SectorSize, sIDs, accountID)
	if err != nil {
		return fmt.Errorf("failed to update account's pinned data: %w", err)
	}

	// ignore the slabs that are pinned by another account
	rows, err = tx.Query(ctx, `SELECT slab_id FROM account_slabs WHERE slab_id = ANY($1)`, sIDs)
	if err != nil {
		return fmt.Errorf("failed to check if slab was pinned: %w", err)
	}
	defer rows.Close()

	seen := make(map[int64]struct{})
	for rows.Next() {
		var sID int64
		if err := rows.Scan(&sID); err != nil {
			return fmt.Errorf("failed to check pinned slab: %w", err)
		}
		seen[sID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to get pinned slabs: %w", err)
	}

	// get all of the slabs that are not pinned by another account
	var toDelete []int64
	for _, sID := range sIDs {
		if _, ok := seen[sID]; ok {
			continue
		}
		toDelete = append(toDelete, sID)
	}

	// prune the slab and its sectors
	batch := &pgx.Batch{}
	batch.Queue(`
				WITH candidate_sectors AS (
					SELECT ss.sector_id
					FROM slab_sectors ss
					WHERE ss.slab_id = ANY($1) AND NOT EXISTS (
						SELECT 1
						FROM slab_sectors ss2
						WHERE ss2.sector_id = ss.sector_id AND ss2.slab_id <> ANY($1)
					)
				)
				DELETE FROM sectors WHERE id IN (SELECT sector_id FROM candidate_sectors);`, toDelete)
	batch.Queue(`DELETE FROM slabs WHERE id = ANY($1)`, toDelete)
	if err := tx.Tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("failed to prune slab: %w", err)
	}

	// update slab stats
	if err := incrementNumSlabs(ctx, tx, -int64(len(toDelete))); err != nil {
		return fmt.Errorf("failed to decrement number of slabs: %w", err)
	}

	return nil
}
