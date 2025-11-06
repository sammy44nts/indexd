package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestMarkSectorsLost(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account
	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))

	// add two hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)

	// add a contract for each host
	fcid1 := store.addTestContract(t, hk1)
	_ = store.addTestContract(t, hk2)

	// pin a slab that adds 2 sectors to each host
	root1 := frand.Entropy256()
	root2 := frand.Entropy256()
	root3 := frand.Entropy256()
	root4 := frand.Entropy256()
	_, err := store.PinSlabs(context.Background(), account, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{
				Root:    root1,
				HostKey: hk1,
			},
			{
				Root:    root2,
				HostKey: hk1,
			},
			{
				Root:    root3,
				HostKey: hk2,
			},
			{
				Root:    root4,
				HostKey: hk2,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	assertSectorStats := func(expectedPinned, expectedUnpinned, expectedUnpinnable int64) {
		t.Helper()
		var pinned, unpinned, unpinnable int64
		err := store.pool.QueryRow(context.Background(), `
			SELECT num_pinned_sectors, num_unpinned_sectors, num_unpinnable_sectors
			FROM stats
			WHERE id = 0`,
		).Scan(&pinned, &unpinned, &unpinnable)
		if err != nil {
			t.Fatal(err)
		}
		if pinned != expectedPinned || unpinned != expectedUnpinned || unpinnable != expectedUnpinnable {
			t.Fatalf("unexpected sector stats: pinned=%d (want %d) unpinned=%d (want %d) unpinnable=%d (want %d)", pinned, expectedPinned, unpinned, expectedUnpinned, unpinnable, expectedUnpinnable)
		}
	}

	assertSectorStats(0, 4, 0)

	if err := store.PinSectors(context.Background(), fcid1, []types.Hash256{root1, root2}); err != nil {
		t.Fatal(err)
	}
	assertSectorStats(2, 2, 0)

	assertSectorLost := func(root types.Hash256, lost bool) {
		t.Helper()
		var isLost bool
		err := store.pool.QueryRow(context.Background(), `SELECT host_id IS NULL FROM sectors WHERE sector_root = $1`, sqlHash256(root)).Scan(&isLost)
		if err != nil {
			t.Fatal(err)
		} else if isLost != lost {
			t.Fatalf("expected sector %x to be lost: %v, got %v", root, lost, isLost)
		}
	}

	assertLostSectors := func(hostKey types.PublicKey, numLost int) {
		t.Helper()
		var count int
		err := store.pool.QueryRow(context.Background(), `SELECT lost_sectors FROM hosts WHERE public_key = $1`, sqlHash256(hostKey)).Scan(&count)
		if err != nil {
			t.Fatal(err)
		} else if count != numLost {
			t.Fatalf("expected %d lost sectors for host %x, got %d", numLost, hostKey, count)
		}
	}

	markSectorLost := func(hk types.PublicKey, roots []types.Hash256) {
		t.Helper()
		if err := store.MarkSectorsLost(context.Background(), hk, roots); err != nil {
			t.Fatal(err)
		}
	}

	// check that no sectors are lost
	assertLostSectors(hk1, 0)
	assertLostSectors(hk2, 0)
	assertSectorLost(root1, false)
	assertSectorLost(root2, false)
	assertSectorLost(root3, false)
	assertSectorLost(root4, false)

	// mark sectors 1 to 3 lost
	markSectorLost(hk1, []types.Hash256{root1, root2})
	assertSectorStats(0, 2, 2)
	markSectorLost(hk2, []types.Hash256{root3})
	assertSectorStats(0, 1, 3)

	assertLostSectors(hk1, 2)
	assertLostSectors(hk2, 1)
	assertSectorLost(root1, true)
	assertSectorLost(root2, true)
	assertSectorLost(root3, true)
	assertSectorLost(root4, false)

	// mark last sector lost as well
	markSectorLost(hk2, []types.Hash256{root4})
	assertSectorStats(0, 0, 4)

	assertLostSectors(hk1, 2)
	assertLostSectors(hk2, 2)
	assertSectorLost(root1, true)
	assertSectorLost(root2, true)
	assertSectorLost(root3, true)
	assertSectorLost(root4, true)
}

func TestMarkSectorsUnpinnable(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	assertUnpinnableSectors := func(expected uint64) {
		t.Helper()
		var got uint64
		err := store.pool.QueryRow(context.Background(), "SELECT num_unpinnable_sectors FROM stats WHERE id = 0").Scan(&got)
		if err != nil {
			t.Fatal(err)
		} else if got != expected {
			t.Fatalf("expected %d unpinnable sectors, got %d", expected, got)
		}
	}

	// add account
	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))

	// add host with a contract
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// pin a slab to add a few sectors to the database
	root1 := frand.Entropy256()
	root2 := frand.Entropy256()
	root3 := frand.Entropy256()
	_, err := store.PinSlabs(context.Background(), account, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{
				Root:    root1,
				HostKey: hk,
			},
			{
				Root:    root2,
				HostKey: hk,
			},
			{
				Root:    root3,
				HostKey: hk,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// assert initial count
	assertUnpinnableSectors(0)

	// make sure some time passes since the default time that is set when the
	// slab is pinned
	time.Sleep(100 * time.Millisecond)

	// after pinning, no slab should be unhealthy since their sectors aren't
	// pinned to contracts yet.
	unhealthyIDs, err := store.UnhealthySlabs(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	} else if len(unhealthyIDs) != 0 {
		t.Fatalf("expected 0 unhealthy slabs, got %d", len(unhealthyIDs))
	}

	// set the uploaded timestamp to past the threshold pruning threshold date
	// of 3 days
	_, err = store.pool.Exec(context.Background(), "UPDATE sectors SET uploaded_at = NOW() - Interval '4 days' WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}

	// we should still have no unhealthy slabs because the host_id has not been
	// set to null yet
	unhealthyIDs, err = store.UnhealthySlabs(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	} else if len(unhealthyIDs) != 0 {
		t.Fatalf("expected 0 unhealthy slabs, got %d", len(unhealthyIDs))
	}

	assertUnpinnableSectors(0)

	if err := store.MarkSectorsUnpinnable(context.Background(), time.Now().Add(-3*24*time.Hour)); err != nil {
		t.Fatal(err)
	}

	// sector should have had host_id nulled out due to MarkSectorsUnpinnable
	// and should now be unhealthy
	unhealthyIDs, err = store.UnhealthySlabs(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	} else if len(unhealthyIDs) != 1 {
		t.Fatalf("expected 1 unhealthy slabs, got %d", len(unhealthyIDs))
	}

	assertUnpinnableSectors(1)
}

func TestMigrateSector(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account
	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))

	// add two hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)

	// add contract for first host
	fcid1 := store.addTestContract(t, hk1)

	// pin a slab to add 2 sectors which are both stored on the first host
	pinTime := time.Now().Round(time.Microsecond)
	root1 := types.Hash256{1}
	root2 := types.Hash256{2}
	_, err := store.PinSlabs(context.Background(), account, pinTime, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{
				Root:    root1,
				HostKey: hk1,
			},
			{
				Root:    root2,
				HostKey: hk1,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// pin sectors to contract
	if err := store.PinSectors(context.Background(), fcid1, []types.Hash256{root1, root2}); err != nil {
		t.Fatal(err)
	}

	// mark all sectors as having failed once
	if _, err := store.pool.Exec(context.Background(), `
		UPDATE sectors
		SET consecutive_failed_checks = 1
	`); err != nil {
		t.Fatal(err)
	}

	sectorUploadedAt := func(root types.Hash256) (uploadedAt time.Time) {
		t.Helper()

		err := store.pool.QueryRow(context.Background(), `
            SELECT uploaded_at
            FROM sectors
            WHERE sector_root = $1
        `, sqlHash256(root)).Scan(&uploadedAt)
		if err != nil {
			t.Fatal(err)
		}
		return
	}

	// helper to assert sector state
	assertSector := func(root types.Hash256, expectedHostKey types.PublicKey, expectedContractID types.FileContractID, expectedFailures, expectedMigrated int) {
		t.Helper()

		var hostKey types.PublicKey
		var contractID types.FileContractID
		var migrated, failures int
		err := store.pool.QueryRow(context.Background(), `
			SELECT hosts.public_key, contract_sectors_map.contract_id, consecutive_failed_checks, num_migrated
			FROM sectors
			INNER JOIN hosts ON sectors.host_id = hosts.id
			LEFT JOIN contract_sectors_map ON sectors.contract_sectors_map_id = contract_sectors_map.id
			WHERE sector_root = $1
		`, sqlHash256(root)).Scan(asNullable((*sqlPublicKey)(&hostKey)), asNullable((*sqlHash256)(&contractID)), &failures, &migrated)
		if err != nil {
			t.Fatal(err)
		} else if hostKey != expectedHostKey {
			t.Fatalf("expected host key %v, got %v", expectedHostKey, hostKey)
		} else if contractID != expectedContractID {
			t.Fatalf("expected contract ID %v, got %v", expectedContractID, contractID)
		} else if failures != expectedFailures {
			t.Fatalf("expected %d consecutive failures, got %d", expectedFailures, failures)
		} else if migrated != expectedMigrated {
			t.Fatalf("expected %d migrations, got %d", expectedMigrated, migrated)
		}
	}

	assertMigratedSectors := func(expected int64) {
		t.Helper()

		var got int64
		err = store.pool.QueryRow(context.Background(), `SELECT num_migrated_sectors FROM stats`).Scan(&got)
		if err != nil {
			t.Fatal(err)
		} else if got != expected {
			t.Fatalf("expected %d migrated sectors, got %d", expected, got)
		}
	}

	migrate := func(root types.Hash256, hostKey types.PublicKey, expectedMigrated bool) {
		t.Helper()

		beforeUploadedAt := sectorUploadedAt(root)
		if migrated, err := store.MigrateSector(context.Background(), root, hostKey); err != nil {
			t.Fatal(err)
		} else if migrated != expectedMigrated {
			t.Fatalf("expected migrated %v, got %v", expectedMigrated, migrated)
		}
		afterUploadedAt := sectorUploadedAt(root)

		if expectedMigrated && afterUploadedAt.Compare(beforeUploadedAt) != 1 {
			t.Fatal("expected after uploaded at timestamp to be greater than before timestamp")
		} else if !expectedMigrated && !afterUploadedAt.Equal(beforeUploadedAt) {
			t.Fatal("expected after uploaded at timestamp equal before timestamp because no migration happened")
		}
	}

	// assert initial state
	assertSector(root1, hk1, fcid1, 1, 0)
	assertSector(root2, hk1, fcid1, 1, 0)
	assertMigratedSectors(0)

	// migrate sector 1 to host 2
	migrate(root1, hk2, true)
	assertSector(root1, hk2, types.FileContractID{}, 0, 1)
	assertSector(root2, hk1, fcid1, 1, 0)
	assertMigratedSectors(1)

	// migrate sector 2 to unknown host, this should be a no-op
	migrate(root2, types.PublicKey{10}, false)
	assertSector(root1, hk2, types.FileContractID{}, 0, 1)
	assertSector(root2, hk1, fcid1, 1, 0)
	assertMigratedSectors(1)

	// migrate sector 2 to host 2
	migrate(root2, hk2, true)
	assertSector(root1, hk2, types.FileContractID{}, 0, 1)
	assertSector(root2, hk2, types.FileContractID{}, 0, 1)
	assertMigratedSectors(2)
}

func TestPinSectors(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// create host and account
	hk := store.addTestHost(t)
	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))

	// create 2 contracts
	contractID1 := store.addTestContract(t, hk, types.FileContractID{1})
	contractID2 := store.addTestContract(t, hk, types.FileContractID{2})

	// create 4 sectors
	_, err := store.PinSlabs(context.Background(), account, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{
				HostKey: hk,
				Root:    types.Hash256{1},
			},
			{
				HostKey: hk,
				Root:    types.Hash256{2},
			},
			{
				HostKey: hk,
				Root:    types.Hash256{3},
			},
			{
				HostKey: hk,
				Root:    types.Hash256{4},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// set the sectors' host IDs to NULL to make sure PinSectors also sets
	// those
	res, err := store.pool.Exec(context.Background(), "UPDATE sectors SET host_id = NULL")
	if err != nil {
		t.Fatal(err)
	} else if res.RowsAffected() != 4 {
		t.Fatalf("expected 4 rows affected, got %d", res.RowsAffected())
	}

	// helper to assert sector is pinned
	assertPinned := func(sid int64, contractID *int64) {
		t.Helper()
		var selectedContractID, selectedHostID sql.NullInt64
		err := store.pool.QueryRow(context.Background(), "SELECT contract_sectors_map_id, host_id FROM sectors WHERE id = $1", sid).
			Scan(&selectedContractID, &selectedHostID)
		if err != nil {
			t.Fatal(err)
		} else if contractID != nil && !selectedContractID.Valid {
			t.Fatalf("expected contract ID %v, got nil", contractID)
		} else if contractID == nil && selectedContractID.Valid {
			t.Fatalf("expected nil contract ID, got %v", selectedContractID)
		} else if contractID != nil && selectedContractID.Int64 != *contractID {
			t.Fatalf("expected contract ID %v, got %v", *contractID, selectedContractID.Int64)
		} else if contractID != nil && (!selectedHostID.Valid || selectedHostID.Int64 != 1) {
			t.Fatal("expected host ID to be set to 1 if sector is pinned", selectedHostID.Int64)
		}
	}

	// none are pinned
	assertPinned(1, nil)
	assertPinned(2, nil)
	assertPinned(3, nil)
	assertPinned(4, nil)

	// pin sectors 1 and 3 to contract 1
	err = store.PinSectors(context.Background(), contractID1, []types.Hash256{{1}, {3}})
	if err != nil {
		t.Fatal(err)
	}
	one := int64(1)
	assertPinned(1, &one)
	assertPinned(2, nil)
	assertPinned(3, &one)
	assertPinned(4, nil)

	// pin sectors 2 and 4 to contract 2
	err = store.PinSectors(context.Background(), contractID2, []types.Hash256{{2}, {4}})
	if err != nil {
		t.Fatal(err)
	}
	two := int64(2)
	assertPinned(1, &one)
	assertPinned(2, &two)
	assertPinned(3, &one)
	assertPinned(4, &two)

	// pin to contract that doesn't exist
	err = store.PinSectors(context.Background(), types.FileContractID{9}, []types.Hash256{{2}})
	if !errors.Is(err, contracts.ErrNotFound) {
		t.Fatal("expected ErrNotFound, got", err)
	}
}

func TestRecordIntegrityCheck(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account
	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))

	// add host
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// pin a slab to add 2 sectors
	pinTime := time.Now().Round(time.Microsecond)
	root1 := types.Hash256{1}
	root2 := types.Hash256{2}
	_, err := store.PinSlabs(context.Background(), account, pinTime, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{
				Root:    root1,
				HostKey: hk,
			},
			{
				Root:    root2,
				HostKey: hk,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// helper to assert sector state
	assertSectors := func(root types.Hash256, expectedNextCheck time.Time, expectedConsecutiveFailures int) {
		t.Helper()
		var nextCheck time.Time
		var consecutiveFailures int
		err := store.pool.QueryRow(context.Background(), "SELECT next_integrity_check, consecutive_failed_checks FROM sectors WHERE sector_root = $1", sqlHash256(root)).Scan(&nextCheck, &consecutiveFailures)
		if err != nil {
			t.Fatal(err)
		} else if expectedNextCheck != nextCheck {
			t.Fatalf("expected next check %v, got %v", expectedNextCheck, nextCheck)
		} else if consecutiveFailures != expectedConsecutiveFailures {
			t.Fatalf("expected %d consecutive failures, got %d", expectedConsecutiveFailures, consecutiveFailures)
		}
	}

	assertFailingSectors := func(expectedRoots []types.Hash256, minChecks int) {
		t.Helper()
		rows, err := store.pool.Query(context.Background(), `
			SELECT sector_root
			FROM sectors
   			WHERE
                host_id = (SELECT id FROM hosts WHERE public_key = $1)
                AND consecutive_failed_checks >= $2
		`, sqlPublicKey(hk), minChecks)
		if err != nil {
			t.Fatal(err)
		}
		var roots []types.Hash256
		for rows.Next() {
			var root types.Hash256
			if err := rows.Scan((*sqlHash256)(&root)); err != nil {
				t.Fatal(err)
			}
			roots = append(roots, root)
		}
		if err != nil {
			t.Fatal(err)
		} else if len(roots) != len(expectedRoots) {
			t.Fatalf("expected %d failing sectors, got %d", len(expectedRoots), len(roots))
		} else if len(roots) > 0 && !reflect.DeepEqual(roots, expectedRoots) {
			t.Fatalf("expected failing sectors %v, got %v", expectedRoots, roots)
		}
	}

	assertLostSectors := func(expected int) {
		t.Helper()
		var lostSectors int
		if err := store.pool.QueryRow(context.Background(), "SELECT lost_sectors FROM hosts WHERE public_key = $1", sqlPublicKey(hk)).
			Scan(&lostSectors); err != nil {
			t.Fatal(err)
		} else if lostSectors != expected {
			t.Fatalf("expected %d lost sectors, got %d", expected, lostSectors)
		}
	}

	assertSectorStats := func(expectedPinned, expectedUnpinned, expectedUnpinnable int64) {
		t.Helper()
		var pinned, unpinned, unpinnable int64
		err := store.pool.QueryRow(context.Background(), `
			SELECT num_pinned_sectors, num_unpinned_sectors, num_unpinnable_sectors
			FROM stats
			WHERE id = 0`,
		).Scan(&pinned, &unpinned, &unpinnable)
		if err != nil {
			t.Fatal(err)
		}
		if pinned != expectedPinned || unpinned != expectedUnpinned || unpinnable != expectedUnpinnable {
			t.Fatalf("unexpected sector stats: pinned=%d (want %d) unpinned=%d (want %d) unpinnable=%d (want %d)", pinned, expectedPinned, unpinned, expectedUnpinned, unpinnable, expectedUnpinnable)
		}
	}

	record := func(success bool, nextCheck time.Time, roots []types.Hash256) {
		t.Helper()
		err := store.RecordIntegrityCheck(context.Background(), success, nextCheck, hk, roots)
		if err != nil {
			t.Fatal(err)
		}
	}

	// check initial state - 0 failures
	assertSectors(root1, pinTime, 0)
	assertSectors(root2, pinTime, 0)
	assertFailingSectors([]types.Hash256{}, 1)

	// record success for both
	now := time.Now().Round(time.Microsecond)
	record(true, now, []types.Hash256{root1, root2})
	assertSectors(root1, now, 0)
	assertSectors(root2, now, 0)
	assertFailingSectors([]types.Hash256{}, 1)

	// record failure for both
	now = now.Add(time.Minute)
	record(false, now, []types.Hash256{root1, root2})
	assertSectors(root1, now, 1)
	assertSectors(root2, now, 1)
	assertFailingSectors([]types.Hash256{root1, root2}, 1)

	// one more failure for root1 and success for root2
	now = now.Add(time.Minute)
	record(false, now, []types.Hash256{root1})
	record(true, now, []types.Hash256{root2})
	assertSectors(root1, now, 2)
	assertSectors(root2, now, 0)
	assertFailingSectors([]types.Hash256{root1}, 1)

	// host should not have any lost sectors
	assertLostSectors(0)

	// mark sectors lost with a threshold of 2 which is too high to mark
	// root1 as lost
	if err := store.MarkFailingSectorsLost(context.Background(), hk, 3); err != nil {
		t.Fatal(err)
	}
	assertSectorStats(0, 2, 0)
	assertFailingSectors([]types.Hash256{root1}, 2)

	// one more time with threshold of 1
	if err := store.MarkFailingSectorsLost(context.Background(), hk, 2); err != nil {
		t.Fatal(err)
	}
	assertSectorStats(0, 1, 1)
	assertFailingSectors([]types.Hash256{}, 2)

	// host should have lost sector
	assertLostSectors(1)
}

func TestSectorsForIntegrityCheck(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add account
	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))

	// add host
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// pin a slab to add a few sectors to the database
	root1 := frand.Entropy256()
	root2 := frand.Entropy256()
	root3 := frand.Entropy256()
	root4 := frand.Entropy256()
	_, err := store.PinSlabs(context.Background(), account, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: [32]byte{},
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{
				Root:    root1,
				HostKey: hk,
			},
			{
				Root:    root2,
				HostKey: hk,
			},
			{
				Root:    root3,
				HostKey: hk,
			},
			{
				Root:    root4,
				HostKey: hk,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// update next integrity check time for roots and assert the ordering works
	updateNextCheck := func(root types.Hash256, nextCheck time.Time) {
		t.Helper()
		_, err := store.pool.Exec(context.Background(), `UPDATE sectors SET next_integrity_check = $1 WHERE sector_root = $2`, nextCheck, sqlHash256(root))
		if err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().Round(time.Microsecond)
	updateNextCheck(root2, now.Add(-2*time.Hour))      // requires check
	updateNextCheck(root4, now.Add(-1*time.Hour))      // requires check
	updateNextCheck(root3, now.Add(-time.Millisecond)) // requires check
	updateNextCheck(root1, now.Add(1*time.Hour))       // doesn't require check

	// make sure limit is applied
	assertSectors := func(limit int) {
		t.Helper()
		expected := []types.Hash256{root2, root4, root3}
		sectors, err := store.SectorsForIntegrityCheck(context.Background(), hk, limit)
		if err != nil {
			t.Fatal(err)
		} else if len(sectors) != min(limit, len(expected)) {
			t.Fatalf("expected 3 sectors, got %d", len(sectors))
		} else if !reflect.DeepEqual(sectors, expected[:min(limit, len(expected))]) {
			t.Fatal("sectors don't match expected roots")
		}
	}
	assertSectors(10)
	assertSectors(3)
	assertSectors(2)
	assertSectors(1)

	// no sectors for unknown host
	sectors, err := store.SectorsForIntegrityCheck(context.Background(), types.PublicKey{2}, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(sectors) != 0 {
		t.Fatalf("expected 0 sectors, got %d", len(sectors))
	}
}

func TestUnhealthySlabs(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// assertUnhealthySlabs asserts the number of unhealthy slabs
	assertUnhealthySlabs := func(expected, limit int) []slabs.SlabID {
		t.Helper()

		unhealthy, err := store.UnhealthySlabs(context.Background(), limit)
		if err != nil {
			t.Fatal(err)
		} else if len(unhealthy) != expected {
			t.Fatalf("expected %d unhealthy slabs, got %d", expected, len(unhealthy))
		}
		return unhealthy
	}

	// resetNextRepairAttemptTime sets the next_repair_attempt to an hour
	// ago for all slabs to allow them to be returned again should they still be
	// unhealthy
	resetNextRepairAttemptTime := func() {
		t.Helper()

		_, err := store.pool.Exec(context.Background(), "UPDATE slabs SET next_repair_attempt = NOW() - INTERVAL '1 hour'")
		if err != nil {
			t.Fatal(err)
		}
	}

	// add an account
	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))

	// add a host and a contract
	hk := store.addTestHost(t)
	contractID := store.addTestContract(t, hk)

	// add two slabs
	slabID1 := store.pinTestSlab(t, account, 1, []types.PublicKey{hk, hk})
	slabID2 := store.pinTestSlab(t, account, 1, []types.PublicKey{hk, hk})
	resetNextRepairAttemptTime()

	// pin all sectors to the contract
	_, err := store.pool.Exec(context.Background(), "UPDATE sectors SET contract_sectors_map_id = 1")
	if err != nil {
		t.Fatal(err)
	}

	// assert we have no unhealthy slabs
	assertUnhealthySlabs(0, 10)

	// renew the contract
	renewal := newTestRevision(hk)
	renewal.ExpirationHeight = 0 // expired, will be pruned the next time PruneContractSectorsMap is called
	err = store.AddRenewedContract(context.Background(), contractID, types.FileContractID{1}, renewal, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{})
	if err != nil {
		t.Fatal(err)
	}

	// assert we still have no unhealthy slabs
	assertUnhealthySlabs(0, 10)

	// update the contract to be bad
	_, err = store.pool.Exec(context.Background(), "UPDATE contracts SET good = FALSE")
	if err != nil {
		t.Fatal(err)
	}

	// assert both slabs are unhealthy
	assertUnhealthySlabs(2, 10)

	// assert consecutive calls to unhealthy slabs return no new slabs
	assertUnhealthySlabs(0, 10)

	// reset the next repair attempt time and assert we have unhealthy slabs
	resetNextRepairAttemptTime()
	assertUnhealthySlabs(2, 10)

	// reset the next repair attempt time and assert the limit is applied
	resetNextRepairAttemptTime()
	assertUnhealthySlabs(1, 1)

	// we can call it again since we have one left, should be SlabID2
	unhealthy := assertUnhealthySlabs(1, 1)
	if unhealthy[0] != slabID2 {
		t.Fatalf("expected slab ID %v, got %v", slabID2, unhealthy[0])
	}
	resetNextRepairAttemptTime()

	// make the contract good again and assert no unhealthy slabs
	_, err = store.pool.Exec(context.Background(), "UPDATE contracts SET good = TRUE")
	if err != nil {
		t.Fatal(err)
	}
	assertUnhealthySlabs(0, 10)

	// update the contract to be no longer active or pending and assert both slabs are unhealthy
	_, err = store.pool.Exec(context.Background(), "UPDATE contracts SET state = $1", sqlContractState(contracts.ContractStateExpired))
	if err != nil {
		t.Fatal(err)
	}
	assertUnhealthySlabs(2, 10)
	resetNextRepairAttemptTime()

	// set the state back to active
	_, err = store.pool.Exec(context.Background(), "UPDATE contracts SET state = $1", sqlContractState(contracts.ContractStateActive))
	if err != nil {
		t.Fatal(err)
	}

	// remove a sector from its host - the unhealthy slab should be back
	_, err = store.pool.Exec(context.Background(), "UPDATE sectors SET host_id = NULL, contract_sectors_map_id = NULL WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}

	// assert slab1 is unhealthy
	unhealthy = assertUnhealthySlabs(1, 10)
	if err != nil {
		t.Fatal(err)
	} else if unhealthy[0] != slabID1 {
		t.Fatalf("expected slab ID %v, got %v", slabID1, unhealthy[0])
	}
	resetNextRepairAttemptTime()

	// add the sector back - the unhealthy slab should be gone
	_, err = store.pool.Exec(context.Background(), "UPDATE sectors SET host_id = 1, contract_sectors_map_id = NULL WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	assertUnhealthySlabs(0, 10)

	// recalculate sector stats
	_, err = store.pool.Exec(context.Background(), `
		UPDATE stats
		SET num_pinned_sectors = (
			SELECT COUNT(id)
			FROM sectors
			WHERE host_id IS NOT NULL AND contract_sectors_map_id IS NOT NULL
		)`)
	if err != nil {
		t.Fatal(err)
	}

	// prune expired contract
	err = store.PruneContractSectorsMap(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := store.pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM contract_sectors_map").Scan(&count); err != nil {
		t.Fatal(err)
	} else if count != 0 {
		t.Fatalf("expected 0 contract sectors map rows, got %d", count)
	}

	// assert slab1 is not considered unhealthy since it is considered uploaded
	// to a host but not yet pinned
	assertUnhealthySlabs(0, 10)
}

func TestUnpinnedSectors(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// create host with account and contract
	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// create 4 sectors
	_, err := store.PinSlabs(context.Background(), account, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{
				HostKey: hk,
				Root:    types.Hash256{1},
			},
			{
				HostKey: hk,
				Root:    types.Hash256{2},
			},
			{
				HostKey: hk,
				Root:    types.Hash256{3},
			},
			{
				HostKey: hk,
				Root:    types.Hash256{4},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// helper to update sector's pinned state
	updateSector := func(sid int64, hostID, contractID *int64, uploadedAt time.Time) {
		t.Helper()
		res, err := store.pool.Exec(context.Background(), `UPDATE sectors SET contract_sectors_map_id=$1, host_id=$2, uploaded_at=$3 WHERE id=$4`, contractID, hostID, uploadedAt, sid)
		if err != nil {
			t.Fatal(err)
		} else if res.RowsAffected() != 1 {
			t.Fatal("no rows updated")
		}
	}

	// prepare sectors
	one := int64(1)
	now := time.Now().Round(time.Microsecond)
	updateSector(1, &one, &one, now)                // has host and is pinned to contract
	updateSector(2, &one, nil, now)                 // has host but is not pinned to contract
	updateSector(3, nil, nil, now)                  // has neither host nor is it pinned
	updateSector(4, &one, nil, now.Add(-time.Hour)) // also not pinned but older than 2

	// check unpinned sectors
	unpinned, err := store.UnpinnedSectors(context.Background(), hk, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(unpinned) != 2 {
		t.Fatalf("expected 2 unpinned sector, got %d", len(unpinned))
	} else if unpinned[0] != (types.Hash256{4}) {
		t.Fatalf("expected root %v, got %v", types.Hash256{4}, unpinned[0])
	} else if unpinned[1] != (types.Hash256{2}) {
		t.Fatalf("expected root %v, got %v", types.Hash256{2}, unpinned[1])
	}

	// again with lower limit
	unpinned, err = store.UnpinnedSectors(context.Background(), hk, 1)
	if err != nil {
		t.Fatal(err)
	} else if len(unpinned) != 1 {
		t.Fatalf("expected 1 unpinned sector, got %d", len(unpinned))
	} else if unpinned[0] != (types.Hash256{4}) {
		t.Fatalf("expected root %v, got %v", types.Hash256{4}, unpinned[0])
	}
}

// BenchmarkMarkSectorsLost benchmarks MarkSectorsLost in various batch sizes.
func BenchmarkMarkSectorsLost(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// create account, host and contract
	account := proto.Account{1}
	store.addTestAccount(b, types.PublicKey(account))
	hk := store.addTestHost(b)
	store.addTestContract(b, hk)

	// prepare base db
	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors
		nSectors   = dbBaseSize / proto.SectorSize
	)

	// insert sectors in batches
	for remainingSectors := nSectors; remainingSectors > 0; {
		batchSize := min(remainingSectors, 10000)
		remainingSectors -= batchSize
		var sectors []slabs.PinnedSector
		for range batchSize {
			sectors = append(sectors, slabs.PinnedSector{
				Root:    frand.Entropy256(),
				HostKey: hk,
			})
		}
		_, err := store.PinSlabs(context.Background(), account, time.Time{}, slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	// helper to mark sectors "unlost".
	markNotLost := func() {
		b.Helper()
		_, err := store.pool.Exec(context.Background(), `
			UPDATE sectors
			SET contract_sectors_map_id = 1, host_id = 1
			WHERE contract_sectors_map_id IS NULL AND host_id IS NULL
		`)
		if err != nil {
			b.Fatal(err)
		}

		// recalculate sector stats
		_, err = store.pool.Exec(context.Background(), `
		UPDATE stats
		SET num_pinned_sectors = (
			SELECT COUNT(id)
			FROM sectors
			WHERE host_id IS NOT NULL AND contract_sectors_map_id IS NOT NULL
		)`)
		if err != nil {
			b.Fatal(err)
		}
	}

	// helper to find sectors to mark as lost
	sectorsToMark := func(batchSize int) []types.Hash256 {
		b.Helper()
		rows, err := store.pool.Query(context.Background(), `
			SELECT sector_root
			FROM sectors
			WHERE host_id IS NOT NULL
			LIMIT $1
		`, batchSize)
		if err != nil {
			b.Fatal(err)
		}
		defer rows.Close()
		var roots []types.Hash256
		for rows.Next() {
			var root types.Hash256
			if err := rows.Scan((*sqlHash256)(&root)); err != nil {
				b.Fatal(err)
			}
			roots = append(roots, root)
		}
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		return roots
	}

	// run benchmark for various batch sizes
	for _, batchSize := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			markNotLost()
			goodSectors := nSectors
			b.SetBytes(int64(batchSize) * proto.SectorSize)
			b.ResetTimer()

			for b.Loop() {
				b.StopTimer()
				toMark := sectorsToMark(batchSize)
				if len(toMark) != batchSize {
					b.Fatalf("expected %d sectors to mark, got %d", batchSize, len(toMark))
				}
				goodSectors -= batchSize
				b.StartTimer()

				if goodSectors < batchSize {
					// reset all sectors to avoid running out
					b.StopTimer()
					markNotLost()
					goodSectors = nSectors
					b.StartTimer()
				} else {
					// pin fetched sectors to fetch different ones next
					err := store.MarkSectorsLost(context.Background(), hk, toMark)
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkMarkFailingSectorsLost benchmarks MarkFailingSectorsLost
func BenchmarkMarkFailingSectorsLost(b *testing.B) {
	store := initPostgres(b, zap.NewNop())
	account := proto.Account{1}

	store.addTestAccount(b, types.PublicKey(account))

	// add a host
	hk := store.addTestHost(b)

	// prepare base db
	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors
		nSectors   = dbBaseSize / proto.SectorSize
	)

	// insert sectors in batches
	for remainingSectors := nSectors; remainingSectors > 0; {
		batchSize := min(remainingSectors, 10000)
		remainingSectors -= batchSize
		var sectors []slabs.PinnedSector
		for range batchSize {
			root := frand.Entropy256()
			sectors = append(sectors, slabs.PinnedSector{
				Root:    root,
				HostKey: hk,
			})
		}
		if _, err := store.PinSlabs(context.Background(), account, time.Now().Add(time.Hour), slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		}); err != nil {
			b.Fatal(err)
		}
	}

	// 10% of the sectors are bad
	reset := func() {
		b.Helper()
		_, err := store.pool.Exec(context.Background(), `UPDATE sectors SET consecutive_failed_checks = 1 WHERE consecutive_failed_checks = 0 AND id % 10 = 0`)
		if err != nil {
			b.Fatal(err)
		}
	}
	reset()

	for b.Loop() {
		b.SetBytes(proto.SectorSize * nSectors / 10)

		err := store.MarkFailingSectorsLost(context.Background(), hk, 1)
		if err != nil {
			b.Fatal(err)
		}

		// reset db
		b.StopTimer()
		reset()
		b.StartTimer()
	}
}

// BenchmarkMarkSectorsUnpinnable benchmarks MarkSectorsUnpinnable
func BenchmarkMarkSectorsUnpinnable(b *testing.B) {
	store := initPostgres(b, zap.NewNop())
	account := proto.Account{1}

	store.addTestAccount(b, types.PublicKey(account))

	// add a host
	hk := store.addTestHost(b)

	// prepare base db
	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors
		nSectors   = dbBaseSize / proto.SectorSize
	)

	// insert sectors in batches
	for remainingSectors := nSectors; remainingSectors > 0; {
		batchSize := min(remainingSectors, 10000)
		remainingSectors -= batchSize
		var sectors []slabs.PinnedSector
		for range batchSize {
			root := frand.Entropy256()
			sectors = append(sectors, slabs.PinnedSector{
				Root:    root,
				HostKey: hk,
			})
		}
		if _, err := store.PinSlabs(context.Background(), account, time.Now().Add(time.Hour), slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		}); err != nil {
			b.Fatal(err)
		}
	}

	now := time.Now()
	const day = 24 * time.Hour
	reset := func(fraction int64) {
		b.Helper()
		_, err := store.pool.Exec(context.Background(), `UPDATE sectors SET host_id = 1, contract_sectors_map_id = NULL, uploaded_at = $1`, now)
		if err != nil {
			b.Fatal(err)
		}
		_, err = store.pool.Exec(context.Background(), `UPDATE sectors SET uploaded_at = $1 WHERE id % $2 = 0`, now.Add(-4*day), fraction)
		if err != nil {
			b.Fatal(err)
		}
	}

	// 1/10 = 10%, 1/100 = 1%, 1/1000 = 0.1%
	for _, fraction := range []int64{10, 100, 1000} {
		reset(fraction)
		b.Run(fmt.Sprintf("%.3f%%", 1.0/float32(fraction)*100), func(b *testing.B) {
			for b.Loop() {
				b.SetBytes(proto.SectorSize * nSectors / fraction)
				err := store.MarkSectorsUnpinnable(context.Background(), now.Add(-3*day))
				if err != nil {
					b.Fatal(err)
				}

				// reset db
				b.StopTimer()
				reset(fraction)
				b.StartTimer()
			}
		})
	}
}

// BenchmarkMigrateSector benchmarks MigrateSector.
func BenchmarkMigrateSector(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// create account, host and contract
	account := proto.Account{1}
	store.addTestAccount(b, types.PublicKey(account))

	// add 100 hosts and contracts
	var hks []types.PublicKey
	for range 100 {
		hk := store.addTestHost(b)
		store.addTestContract(b, hk)
		hks = append(hks, hk)
	}

	// prepare base db
	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors
		nSectors   = dbBaseSize / proto.SectorSize
	)

	// insert sectors in batches
	hostIdx := 0
	var roots []types.Hash256
	rootsByContract := make(map[types.FileContractID][]types.Hash256)
	for remainingSectors := nSectors; remainingSectors > 0; {
		batchSize := min(remainingSectors, 10000)
		remainingSectors -= batchSize
		var sectors []slabs.PinnedSector
		for range batchSize {
			hk := hks[hostIdx]
			root := frand.Entropy256()
			sectors = append(sectors, slabs.PinnedSector{
				Root:    root,
				HostKey: hks[hostIdx],
			})
			rootsByContract[types.FileContractID(hk)] = append(rootsByContract[types.FileContractID(hk)], root)
			roots = append(roots, root)
			hostIdx = (hostIdx + 1) % len(hks)
		}

		// pin slab
		_, err := store.PinSlabs(context.Background(), account, time.Time{}, slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	// pin all sectors to contracts
	for contractID, roots := range rootsByContract {
		err := store.PinSectors(context.Background(), contractID, roots)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.SetBytes(proto.SectorSize)
	for b.Loop() {
		// migrate random sector to random host
		root := roots[frand.Intn(len(roots))]
		hostKey := hks[frand.Intn(len(hks))]
		_, err := store.MigrateSector(context.Background(), root, hostKey)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPinSectors benchmarks PinSectors in various batch sizes.
func BenchmarkPinSectors(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// create account, host and contract
	account := proto.Account{1}
	store.addTestAccount(b, types.PublicKey(account))
	hk := store.addTestHost(b)
	store.addTestContract(b, hk)

	// prepare base db
	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors
		nSectors   = dbBaseSize / proto.SectorSize
	)

	// insert sectors in batches
	for remainingSectors := nSectors; remainingSectors > 0; {
		batchSize := min(remainingSectors, 10000)
		remainingSectors -= batchSize
		var sectors []slabs.PinnedSector
		for range batchSize {
			sectors = append(sectors, slabs.PinnedSector{
				Root:    frand.Entropy256(),
				HostKey: hk,
			})
		}
		_, err := store.PinSlabs(context.Background(), account, time.Time{}, slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	// helper to unpin all sectors
	unpinSectors := func() {
		b.Helper()
		_, err := store.pool.Exec(context.Background(), `
			UPDATE sectors
			SET contract_sectors_map_id = NULL,
			uploaded_at = NOW() - interval '1 week' * random()
			WHERE contract_sectors_map_id IS NOT NULL`)
		if err != nil {
			b.Fatal(err)
		}
	}

	// run benchmark for various batch sizes
	for _, batchSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			unpinSectors()
			unpinnedSectors := nSectors
			b.SetBytes(int64(batchSize) * proto.SectorSize)
			b.ResetTimer()

			for b.Loop() {
				b.StopTimer()

				// make sure there are enough unpinned sectors left
				if unpinnedSectors < batchSize {
					// unpin all sectors to avoid running out
					unpinSectors()
					unpinnedSectors = nSectors
				}

				// fetch sectors to pin
				unpinned, err := store.UnpinnedSectors(context.Background(), hk, batchSize)
				if err != nil {
					b.Fatal(err)
				} else if len(unpinned) != batchSize {
					b.Fatalf("expected %d unpinned sector, got %d (%d unpinned)", batchSize, len(unpinned), unpinnedSectors)
				}
				unpinnedSectors -= batchSize

				// pin fetched sectors to fetch different ones next
				b.StartTimer()
				err = store.PinSectors(context.Background(), types.FileContractID(hk), unpinned)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRecordIntegrityChecks benchmarks RecordIntegrityCheck
func BenchmarkRecordIntegrityChecks(b *testing.B) {
	store := initPostgres(b, zap.NewNop())
	account := proto.Account{1}

	store.addTestAccount(b, types.PublicKey(account))

	// add a host
	hk := store.addTestHost(b)

	// prepare base db
	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors
		nSectors   = dbBaseSize / proto.SectorSize
	)

	// insert sectors in batches
	sectorRoots := make([]types.Hash256, 0, nSectors)
	for remainingSectors := nSectors; remainingSectors > 0; {
		batchSize := min(remainingSectors, 10000)
		remainingSectors -= batchSize
		var sectors []slabs.PinnedSector
		for range batchSize {
			root := frand.Entropy256()
			sectors = append(sectors, slabs.PinnedSector{
				Root:    root,
				HostKey: hk,
			})
			sectorRoots = append(sectorRoots, root)
		}
		if _, err := store.PinSlabs(context.Background(), account, time.Now().Add(time.Hour), slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		}); err != nil {
			b.Fatal(err)
		}
	}

	for _, batchSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			b.SetBytes(int64(batchSize) * proto.SectorSize)
			b.ResetTimer()

			for b.Loop() {
				b.StopTimer()
				frand.Shuffle(len(sectorRoots), func(i, j int) {
					sectorRoots[i], sectorRoots[j] = sectorRoots[j], sectorRoots[i]
				})
				batch := sectorRoots[:batchSize]
				success := frand.Intn(2) == 0
				b.StartTimer()

				err := store.RecordIntegrityCheck(context.Background(), success, time.Now(), hk, batch)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSectorsForIntegrityCheck benchmarks SectorsForIntegrityCheck
func BenchmarkSectorsForIntegrityCheck(b *testing.B) {
	store := initPostgres(b, zap.NewNop())
	account := proto.Account{1}

	store.addTestAccount(b, types.PublicKey(account))

	// add a host
	hk := store.addTestHost(b)

	// prepare base db
	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors
		nSectors   = dbBaseSize / proto.SectorSize
	)

	// insert sectors in batches
	for remainingSectors := nSectors; remainingSectors > 0; {
		batchSize := min(remainingSectors, 10000)
		remainingSectors -= batchSize
		var sectors []slabs.PinnedSector
		for range batchSize {
			sectors = append(sectors, slabs.PinnedSector{
				Root:    frand.Entropy256(),
				HostKey: hk,
			})
		}
		if _, err := store.PinSlabs(context.Background(), account, time.Now().Add(time.Hour), slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		}); err != nil {
			b.Fatal(err)
		}
	}

	// update next_integrity_check to random value in the past
	_, err := store.pool.Exec(context.Background(), `
		UPDATE sectors SET next_integrity_check = NOW() - interval '1 week' * random()
	`)
	if err != nil {
		b.Fatal(err)
	}

	for _, batchSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			b.SetBytes(int64(batchSize) * proto.SectorSize)
			b.ResetTimer()

			for b.Loop() {
				batch, err := store.SectorsForIntegrityCheck(context.Background(), hk, batchSize)
				if err != nil {
					b.Fatal(err)
				} else if len(batch) != batchSize {
					b.Fatalf("no full batch was returned: %d", len(batch))
				}
			}
		})
	}
}

// BenchmarkUnpinnedSectors benchmarks UnpinnedSectors in various batch sizes.
func BenchmarkUnpinnedSectors(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// create account, host and contract
	account := proto.Account{1}
	store.addTestAccount(b, types.PublicKey(account))
	hk := store.addTestHost(b)
	store.addTestContract(b, hk)

	// prepare base db
	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors
		nSectors   = dbBaseSize / proto.SectorSize
	)

	// insert sectors in batches
	for remainingSectors := nSectors; remainingSectors > 0; {
		batchSize := min(remainingSectors, 10000)
		remainingSectors -= batchSize
		var sectors []slabs.PinnedSector
		for range batchSize {
			sectors = append(sectors, slabs.PinnedSector{
				Root:    frand.Entropy256(),
				HostKey: hk,
			})
		}
		_, err := store.PinSlabs(context.Background(), account, time.Time{}, slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	// randomize the uploaded_at time for all sectors
	_, err := store.pool.Exec(context.Background(), `UPDATE sectors SET uploaded_at = NOW() - interval '1 week' * random()`)
	if err != nil {
		b.Fatal(err)
	}

	// define a helper to unpin all sectors between runs
	unpinSectors := func() {
		b.Helper()
		_, err := store.pool.Exec(context.Background(), `UPDATE sectors SET contract_sectors_map_id = NULL`)
		if err != nil {
			b.Fatal(err)
		}

		// recalculate sector stats
		_, err = store.pool.Exec(context.Background(), `
			UPDATE stats
			SET num_unpinned_sectors = (
				SELECT COUNT(id)
				FROM sectors
				WHERE host_id IS NOT NULL AND contract_sectors_map_id IS NULL
			)`)
		if err != nil {
			b.Fatal(err)
		}
	}

	// run benchmark for various batch sizes
	for _, batchSize := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			unpinSectors()
			b.SetBytes(int64(batchSize) * proto.SectorSize)
			b.ResetTimer()

			for b.Loop() {
				// fetch unpinned sectors
				unpinned, err := store.UnpinnedSectors(context.Background(), hk, batchSize)
				if err != nil {
					b.Fatal(err)
				}

				// check if unpinned sectors are exhausted
				b.StopTimer()
				if len(unpinned) < batchSize {
					unpinSectors()
				}

				// pin sectors to ensure we fetch different ones next time
				err = store.PinSectors(context.Background(), types.FileContractID(hk), unpinned)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}

func (s *Store) pinTestSlab(t testing.TB, account proto.Account, minShards uint, hks []types.PublicKey) slabs.SlabID {
	params := slabs.SlabPinParams{
		EncryptionKey: [32]byte(types.GeneratePrivateKey()),
		MinShards:     minShards,
		Sectors:       make([]slabs.PinnedSector, len(hks)),
	}

	for i, hk := range hks {
		params.Sectors[i] = slabs.PinnedSector{
			Root:    frand.Entropy256(),
			HostKey: hk,
		}
	}
	slabIDs, err := s.PinSlabs(context.Background(), account, time.Time{}, params)
	if err != nil {
		t.Fatal(err)
	}
	return slabIDs[0]
}
