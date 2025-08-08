package postgres

import (
	"context"
	"reflect"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api/admin"
	"go.uber.org/zap"
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
	slabID := store.pinTestSlab(t, proto.Account{}, 1, []types.PublicKey{hk, hk, hk})

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

func BenchmarkSectorStats(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	account := proto.Account{1}
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
		b.Fatal("failed to add account:", err)
	}

	// 100 hosts with contracts
	nHosts := 100
	var hks []types.PublicKey
	for i := byte(0); i < byte(nHosts); i++ {
		hk := store.addTestHost(b, types.PublicKey{i})
		hks = append(hks, hk)
		fcid := store.addTestContract(b, hk)

		// 10% of the contracts are bad
		if i%10 == 0 {
			res, err := store.pool.Exec(context.Background(), "UPDATE contracts SET good = FALSE WHERE contract_id = $1", sqlHash256(fcid))
			if err != nil {
				b.Fatal(err)
			} else if res.RowsAffected() != 1 {
				b.Fatal("expected to update 1 row")
			}
		}
	}

	// create 'dbBaseSize' bytes worth of sectors
	var dbBaseSize = 1 << 40 // 1TiB

	// each slab has a size of 4MiB per host since we add 1 shard per host
	var slabSize = nHosts * 1 << 20 // 400MiB

	// prepare base db
	for range dbBaseSize / slabSize {
		store.pinTestSlab(b, account, 1, hks)
	}

	// pin every sector to a contract
	_, err := store.pool.Exec(context.Background(), "UPDATE sectors SET contract_sectors_map_id = ((sectors.id-1) % (SELECT COUNT(*) FROM contract_sectors_map) + 1)")
	if err != nil {
		b.Fatal(err)
	}

	// 10% of the sectors are lost
	_, err = store.pool.Exec(context.Background(), "UPDATE sectors SET host_id = NULL, contract_sectors_map_id = NULL WHERE sectors.id % 10 = 0")
	if err != nil {
		b.Fatal(err)
	}

	// 10% of the sectors are unpinned
	_, err = store.pool.Exec(context.Background(), "UPDATE sectors SET contract_sectors_map_id = NULL WHERE (sectors.id + 1) % 10 = 0")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for b.Loop() {
		if err := store.RefreshSectorStats(context.Background()); err != nil {
			b.Fatal(err)
		}
		stats, err := store.SectorStats(context.Background())
		if err != nil {
			b.Fatal(err)
		} else if stats.UpdatedAt.IsZero() {
			b.Fatal("expected stats to be updated, got zero timestamp")
		}
		stats.UpdatedAt = time.Time{}
		if !reflect.DeepEqual(stats, admin.SectorsStatsResponse{
			NumSectors:             1048500,
			NumUnpinnedSectors:     104850,
			NumSectorsBadContracts: 104850,
			NumSectorsNoHosts:      104850,
		}) {
			b.Fatal("unexpected stats")
		}
	}
}
