package postgres

import (
	"context"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api/admin"
	"go.uber.org/zap/zaptest"
)

func TestSectorStats(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t))

	// prepare a host with a contract
	hk := store.addTestHost(t)
	fcid := store.addTestContract(t, hk)

	assertStats := func(nSectors, nUnpinned, nBad, nNoHost uint64) {
		t.Helper()
		stats, err := store.SectorStats(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if stats.UpdatedAt.IsZero() {
			t.Fatalf("expected stats to be updated, got %v", stats.UpdatedAt)
		}
		stats.UpdatedAt = time.Time{} // ignore the timestamp
		if !reflect.DeepEqual(stats, admin.SectorsStatsResponse{
			NumSectors:             nSectors,
			NumUnpinnedSectors:     nUnpinned,
			NumSectorsBadContracts: nBad,
			NumSectorsNoHosts:      nNoHost,
		}) {
			t.Fatalf("expected stats (%d, %d, %d, %d), got %v", nSectors, nUnpinned, nBad, nNoHost, stats)
		}
	}

	// initially, there are no sectors
	assertStats(0, 0, 0, 0)

	// pin a slab with 3 sectors
	store.AddAccount(context.Background(), types.PublicKey{})
	slabID := store.pinTestSlab(t, rhp.Account{}, 1, []types.PublicKey{hk, hk, hk})

	// assert 3 unpinned sectors after refreshing the stats
	assertStats(0, 0, 0, 0)
	if err := store.RefreshSectorStats(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertStats(3, 3, 0, 0)

	// pin 2 sectors
	slab, err := store.Slab(context.Background(), slabID)
	if err != nil {
		t.Fatal(err)
	}
	s1, s2 := slab.Sectors[0].Root, slab.Sectors[1].Root
	if err := store.PinSectors(context.Background(), fcid, []types.Hash256{s1, s2}); err != nil {
		t.Fatal(err)
	}

	// assert only 1 sector remains unpinned
	assertStats(3, 3, 0, 0)
	if err := store.RefreshSectorStats(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertStats(3, 1, 0, 0)

	// mark one of the sectors lost and the contract bad
	store.MarkSectorsLost(context.Background(), hk, []types.Hash256{s1})
	store.pool.Exec(context.Background(), `UPDATE contracts SET good = FALSE WHERE id = 1`)

	// we now got 1 sector that is still unpinned, 1 sector pinned to a bad
	// contract and 1 sector that is on no host at all
	assertStats(3, 1, 0, 0)
	if err := store.RefreshSectorStats(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertStats(3, 1, 1, 1)
}
