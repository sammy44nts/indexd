package postgres

import (
	"context"

	"go.sia.tech/indexd/api/admin"
)

func (s *Store) SectorStats(ctx context.Context) (admin.SectorsStatsResponse, error) {
	var stats admin.SectorsStatsResponse
	err := s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		row := tx.QueryRow(ctx, "SELECT updated_at, num_sectors, num_unpinned_sectors, num_sectors_bad_contracts, num_sectors_no_host FROM sectors_stats")
		return row.Scan(&stats.UpdatedAt, &stats.NumSectors, &stats.NumUnpinnedSectors, &stats.NumSectorsBadContracts, &stats.NumSectorsNoHosts)
	})
	return stats, err
}

func (s *Store) RefreshSectorStats(ctx context.Context) error {
	return s.transaction(ctx, func(ctx context.Context, tx *txn) error {
		_, err := tx.Exec(ctx, "REFRESH MATERIALIZED VIEW sectors_stats")
		return err
	})
}
