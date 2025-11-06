package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestMarkSlabRepaired(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// add account
	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))

	// add host
	host := store.addTestHost(t)
	store.addTestContract(t, host)

	// add slab
	slabIDs, err := store.PinSlabs(context.Background(), account, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors: []slabs.PinnedSector{
			{Root: frand.Entropy256(), HostKey: host},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	assertSlabState := func(expectedRepairs int, expectedNextAttempt time.Time) {
		t.Helper()

		roughlyEqual := func(a, b time.Time, slack time.Duration) bool {
			if diff := a.Sub(b); diff < -slack || diff > slack {
				return false
			}
			return true
		}

		var consecutiveFailedRepairs int
		var nextRepairAttempt time.Time
		if err := store.pool.QueryRow(t.Context(), `
			SELECT consecutive_failed_repairs, next_repair_attempt 
			FROM slabs 
			WHERE digest = $1`, sqlHash256(slabIDs[0])).Scan(&consecutiveFailedRepairs, &nextRepairAttempt); err != nil {
			t.Fatal(err)
		} else if consecutiveFailedRepairs != expectedRepairs {
			t.Fatalf("expected %d consecutive failed repairs, got %d", expectedRepairs, consecutiveFailedRepairs)
		} else if !roughlyEqual(nextRepairAttempt, expectedNextAttempt, time.Second) {
			t.Fatalf("expected next repair attempt %s, got %s", expectedNextAttempt, nextRepairAttempt)
		}
	}

	simulateFailedRepair := func() {
		t.Helper()
		if err = store.MarkSlabRepaired(t.Context(), slabIDs[0], false); err != nil {
			t.Fatal(err)
		}
	}

	simulateSuccessfulRepair := func() {
		t.Helper()
		if err = store.MarkSlabRepaired(t.Context(), slabIDs[0], true); err != nil {
			t.Fatal(err)
		}
	}

	// assert initial state
	assertSlabState(0, time.Now())

	// assert state after failed repair
	simulateFailedRepair()
	assertSlabState(1, time.Now().Add(minRepairBackoff))

	// assert backoff is capped at maxRepairBackoff (at 6 consec. failures we exceed it)
	for i := range 6 {
		simulateFailedRepair()
		if i < 4 {
			assertSlabState(i+2, time.Now().Add(minRepairBackoff*time.Duration(1<<(i+1))))
		} else {
			assertSlabState(i+2, time.Now().Add(maxRepairBackoff))
		}
	}

	// assert state after successful repair
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	_, err = store.pool.Exec(t.Context(), "UPDATE slabs SET next_repair_attempt = $1", oneHourAgo)
	if err != nil {
		t.Fatal(err)
	}
	simulateSuccessfulRepair()
	assertSlabState(0, oneHourAgo)
}

func TestPinSlabs(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	account := proto.Account{1}
	account2 := proto.Account{2}

	// pin without an account
	nextCheck := time.Now().Round(time.Microsecond).Add(time.Hour)
	_, err := store.PinSlabs(context.Background(), account, nextCheck, slabs.SlabPinParams{})
	if !errors.Is(err, accounts.ErrNotFound) {
		t.Fatal("expected ErrNotFound, got", err)
	}

	slabSize := uint64(2 * proto.SectorSize)

	// add accounts - account1 can pin 2 slabs and account2 can pin 3 slabs
	store.addTestAccount(t, types.PublicKey(account), accounts.WithMaxPinnedData(2*slabSize))
	store.addTestAccount(t, types.PublicKey(account2), accounts.WithMaxPinnedData(3*slabSize))

	// add two hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)
	store.addTestContract(t, hk1)
	store.addTestContract(t, hk2)

	// helper to create slabs
	newSlab := func(i byte) (slabs.SlabID, slabs.SlabPinParams) {
		slab := slabs.SlabPinParams{
			EncryptionKey: [32]byte{i},
			MinShards:     1,
			Sectors: []slabs.PinnedSector{
				{
					Root:    frand.Entropy256(),
					HostKey: hk1,
				},
				{
					Root:    frand.Entropy256(),
					HostKey: hk2,
				},
			},
		}
		slabID, err := slab.Digest()
		if err != nil {
			t.Fatal(err)
		}
		return slabID, slab
	}

	assertUnpinnedSectors := func(expected uint64) {
		t.Helper()
		var got uint64
		err := store.pool.QueryRow(context.Background(), "SELECT num_unpinned_sectors FROM stats WHERE id = 0").Scan(&got)
		if err != nil {
			t.Fatal(err)
		} else if got != expected {
			t.Fatalf("expected %d unpinned sectors, got %d", expected, got)
		}
	}

	assertPinnedData := func(acc proto.Account, pinned uint64) {
		t.Helper()
		var pinnedData uint64
		err := store.pool.QueryRow(context.Background(), "SELECT pinned_data FROM accounts WHERE public_key = $1", sqlPublicKey(acc)).Scan(&pinnedData)
		if err != nil {
			t.Fatal(err)
		} else if pinnedData != pinned {
			t.Fatalf("expected %d pinned data for account %v, got %d", pinned, acc, pinnedData)
		}
	}
	assertPinnedData(account, 0)
	assertPinnedData(account2, 0)
	assertUnpinnedSectors(0)

	// pin slabs
	slab1ID, slab1 := newSlab(1)
	slab2ID, slab2 := newSlab(2)
	toPin := []slabs.SlabPinParams{slab1, slab2}
	expectedIDs := []slabs.SlabID{slab1ID, slab2ID}
	for i := range toPin {
		slabIDs, err := store.PinSlabs(context.Background(), proto.Account{1}, nextCheck, toPin[i])
		if err != nil {
			t.Fatal(err)
		} else if slabIDs[0] != expectedIDs[i] {
			t.Fatalf("expected slab ID %v, got %v", expectedIDs[i], slabIDs[0])
		}
	}
	assertPinnedData(account, 2*slabSize)
	assertPinnedData(account2, 0)
	assertUnpinnedSectors(4)

	// check that pinning with too large MinShards fails
	_, slab3 := newSlab(3)
	slab3.MinShards = 100
	_, err = store.PinSlabs(context.Background(), proto.Account{1}, nextCheck, slab3)
	if err == nil || !errors.Is(err, slabs.ErrMinShards) {
		t.Fatalf("expected error %v, got %v", slabs.ErrMinShards, err)
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

	assertSlab := func(slabID slabs.SlabID, params slabs.SlabPinParams, slab slabs.Slab) {
		t.Helper()
		if slab.ID != slabID {
			t.Fatalf("expected slab ID %v, got %v", slabID, slab.ID)
		} else if slab.EncryptionKey != params.EncryptionKey {
			t.Fatalf("expected encryption key %x, got %x", params.EncryptionKey, slab.EncryptionKey)
		} else if slab.MinShards != params.MinShards {
			t.Fatalf("expected min shards %d, got %d", params.MinShards, slab.MinShards)
		}
		if len(slab.Sectors) != len(params.Sectors) {
			t.Fatalf("expected %d sectors, got %d", len(params.Sectors), len(slab.Sectors))
		}
		for i, sector := range slab.Sectors {
			if sector.Root != params.Sectors[i].Root {
				t.Fatalf("expected sector root %x, got %x", params.Sectors[i].Root, sector.Root)
			} else if *sector.HostKey != params.Sectors[i].HostKey {
				t.Fatalf("expected host key %x, got %x", params.Sectors[i].HostKey, sector.HostKey)
			} else if sector.ContractID != nil {
				t.Fatalf("expected nil contract ID, got %v", sector.ContractID)
			}
		}
	}

	// fetch inserted slabs
	fetched, err := store.Slabs(context.Background(), account, expectedIDs)
	if err != nil {
		t.Fatal(err)
	} else if len(fetched) != len(toPin) {
		t.Fatalf("expected %d slabs, got %d", len(toPin), len(fetched))
	}
	assertSlab(slab1ID, slab1, fetched[0])
	assertSlab(slab2ID, slab2, fetched[1])

	// again but for wrong account
	_, err = store.Slabs(context.Background(), account2, expectedIDs)
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal(err)
	}

	// pin same slabs for account 2 again which should add links to the join
	// table
	for i := range toPin {
		var beforeUploadedAt []time.Time
		for _, sector := range toPin[i].Sectors {
			beforeUploadedAt = append(beforeUploadedAt, sectorUploadedAt(sector.Root))
		}

		slabIDs, err := store.PinSlabs(context.Background(), account2, nextCheck, toPin[i])
		if err != nil {
			t.Fatal(err)
		} else if slabIDs[0] != expectedIDs[i] {
			t.Fatalf("expected slab IDs %v, got %v", expectedIDs[i], slabIDs[0])
		}

		for i, sector := range toPin[i].Sectors {
			afterUploadedAt := sectorUploadedAt(sector.Root)
			if afterUploadedAt.Compare(beforeUploadedAt[i]) != 1 {
				t.Fatal("expected after uploaded at timestamp to be greater than before timestamp")
			}
		}
	}
	assertPinnedData(account, 2*slabSize)
	assertPinnedData(account2, 2*slabSize)
	assertUnpinnedSectors(4)

	// fetch slabs for account 2
	fetched, err = store.Slabs(context.Background(), account2, expectedIDs)
	if err != nil {
		t.Fatal(err)
	} else if len(fetched) != len(toPin) {
		t.Fatalf("expected %d slabs, got %d", len(toPin), len(fetched))
	}
	assertSlab(slab1ID, slab1, fetched[0])
	assertSlab(slab2ID, slab2, fetched[1])

	// assert database has the right number of sectors, slabs and accounts to
	// make sure deduplication works
	assertCount := func(table string, rows int64) {
		t.Helper()
		var count int64
		err := store.pool.QueryRow(context.Background(), fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&count)
		if err != nil {
			t.Fatal(err)
		} else if count != rows {
			t.Fatalf("expected %d rows in %s, got %d", rows, table, count)
		}
	}
	assertCount("accounts", 2)      // 2 accounts
	assertCount("account_slabs", 4) // 2 slabs for each account
	assertCount("slabs", 2)         // 2 slabs
	assertCount("sectors", 4)       // 2 sectors per slab

	// swap roots of slab 2 and re-pin on account 2
	slab2.Sectors[0].Root, slab2.Sectors[1].Root = slab2.Sectors[1].Root, slab2.Sectors[0].Root
	slabIDs, err := store.PinSlabs(context.Background(), account2, nextCheck, slab2)
	if err != nil {
		t.Fatal(err)
	} else if slabIDs[0] == expectedIDs[0] || slabIDs[0] == expectedIDs[1] {
		t.Fatalf("expected new slab ID, got %v (%v)", slabIDs[0], expectedIDs)
	}
	assertPinnedData(account, 2*slabSize)
	assertPinnedData(account2, 3*slabSize)
	assertUnpinnedSectors(4)

	// assert we still have 3 slabs now, but still only have 4 sectors
	assertCount("account_slabs", 5) // 2 slabs for each account + the new one
	assertCount("slabs", 3)         // 3 slabs
	assertCount("sectors", 4)       // 2 sectors per slab + 0 new ones

	// fetch first slab, get pinned at time
	ids := []slabs.SlabID{slab1ID}
	slabs, err := store.Slabs(context.Background(), account, ids)
	if err != nil {
		t.Fatal(err)
	} else if len(slabs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(slabs))
	}
	slab1Full := slabs[0]
	pinnedAt := slab1Full.PinnedAt

	// pin slab 1 again and fetch it again
	_, err = store.PinSlabs(context.Background(), account2, nextCheck, toPin[0])
	if err != nil {
		t.Fatal(err)
	} else if slabs, err := store.Slabs(context.Background(), account, ids); err != nil {
		t.Fatal(err)
	} else if len(slabs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(slabs))
	} else if !slabs[0].PinnedAt.After(pinnedAt) {
		t.Fatal("expected pinnedAt to be updated")
	}
	assertPinnedData(account, 2*slabSize)
	assertPinnedData(account2, 3*slabSize)
	assertUnpinnedSectors(4)

	// pinning one more slab should fail
	_, slab3 = newSlab(3)
	_, err = store.PinSlabs(context.Background(), account, nextCheck, slab3)
	if !errors.Is(err, accounts.ErrStorageLimitExceeded) {
		t.Fatal("expected ErrStorageLimitExceeded, got", err)
	}
	assertPinnedData(account, 2*slabSize)
	assertPinnedData(account2, 3*slabSize)
	assertUnpinnedSectors(4)
}

func TestPinSlabsBadHost(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	account := proto.Account{1}

	// add accounts - account1 can pin 2 slabs and account2 can pin 3 slabs
	store.addTestAccount(t, types.PublicKey(account))

	// this host is good because it has an active good contract on it
	hk1 := store.addTestHost(t)
	store.addTestContract(t, hk1)
	// this host is considered bad by PinSlabs because there are no
	// contracts formed on it
	hk2 := store.addTestHost(t)

	// helper to create slabs
	newSlab := func(i byte, hks ...types.PublicKey) (slabs.SlabID, slabs.SlabPinParams) {
		slab := slabs.SlabPinParams{
			EncryptionKey: [32]byte{i},
			MinShards:     1,
		}
		for _, hk := range hks {
			slab.Sectors = append(slab.Sectors, slabs.PinnedSector{
				Root:    frand.Entropy256(),
				HostKey: hk,
			})
		}
		slabID, err := slab.Digest()
		if err != nil {
			t.Fatal(err)
		}
		return slabID, slab
	}
	nextCheck := time.Now().Round(time.Microsecond).Add(time.Hour)

	// pin slabs
	slab1ID, slab1 := newSlab(1, hk1)
	if slabIDs, err := store.PinSlabs(context.Background(), proto.Account{1}, nextCheck, slab1); err != nil {
		t.Fatal(err)
	} else if slabIDs[0] != slab1ID {
		t.Fatalf("expected slab ID %v, got %v", slab1ID, slabIDs[0])
	}

	_, slab2 := newSlab(1, hk2)
	if _, err := store.PinSlabs(context.Background(), proto.Account{1}, nextCheck, slab2); err == nil || !errors.Is(err, slabs.ErrBadHosts) {
		t.Fatalf("expected error %v, got %v", slabs.ErrBadHosts, err)
	}
}

func TestPinSlabsConflict(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	account := proto.Account{1}
	nextCheck := time.Now().Round(time.Microsecond).Add(time.Hour)

	store.addTestAccount(t, types.PublicKey(account))
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// helper to create slabs
	newSlab := func() (slabs.SlabID, slabs.SlabPinParams) {
		slab := slabs.SlabPinParams{
			EncryptionKey: [32]byte{9},
			MinShards:     1,
			Sectors: []slabs.PinnedSector{{
				Root:    frand.Entropy256(),
				HostKey: hk,
			}},
		}
		slabID, err := slab.Digest()
		if err != nil {
			t.Fatal(err)
		}
		return slabID, slab
	}

	slabID, slab := newSlab()

	// first pin
	_, err := store.PinSlabs(context.Background(), account, nextCheck, slab)
	if err != nil {
		t.Fatal(err)
	}

	// fetch pinned_at
	slabs1, err := store.Slabs(context.Background(), account, []slabs.SlabID{slabID})
	if err != nil {
		t.Fatal(err)
	}
	pinnedAt1 := slabs1[0].PinnedAt

	time.Sleep(time.Millisecond) // ensure timestamp difference

	// second pin (same slab, should hit conflict update)
	_, err = store.PinSlabs(context.Background(), account, nextCheck, slab)
	if err != nil {
		t.Fatal(err)
	}

	// fetch again
	slabs2, err := store.Slabs(context.Background(), account, []slabs.SlabID{slabID})
	if err != nil {
		t.Fatal(err)
	}
	pinnedAt2 := slabs2[0].PinnedAt

	if !pinnedAt2.After(pinnedAt1) {
		t.Fatalf("expected pinned_at to update on conflict")
	}
}

func TestPinnedSlab(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	account := proto.Account{1}

	// add account
	store.addTestAccount(t, types.PublicKey(account))

	// add hosts
	hosts := make([]types.PublicKey, 30)
	for i := range hosts {
		hosts[i] = store.addTestHost(t)
		store.addTestContract(t, hosts[i])
	}

	pinned := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     10,
		Sectors:       make([]slabs.PinnedSector, 0, len(hosts)),
	}
	for _, host := range hosts {
		pinned.Sectors = append(pinned.Sectors, slabs.PinnedSector{
			Root:    frand.Entropy256(),
			HostKey: host,
		})
	}
	digest, err := pinned.Digest()
	if err != nil {
		t.Fatal(err)
	}
	expected := slabs.PinnedSlab{
		ID:            digest,
		EncryptionKey: pinned.EncryptionKey,
		MinShards:     pinned.MinShards,
		Sectors:       make([]slabs.PinnedSector, len(pinned.Sectors)),
	}
	for i, sector := range pinned.Sectors {
		expected.Sectors[i] = slabs.PinnedSector(sector)
	}

	slabIDs, err := store.PinSlabs(context.Background(), account, time.Time{}, pinned)
	if err != nil {
		t.Fatal(err)
	}
	slabID := slabIDs[0]

	slab, err := store.PinnedSlab(context.Background(), account, slabID)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(slab, expected) {
		t.Fatalf("expected slab %v, got %v", expected, slab)
	}

	// mark some of the sectors as lost
	for i := range slab.Sectors[:10] {
		if err := store.MarkSectorsLost(context.Background(), slab.Sectors[i].HostKey, []types.Hash256{slab.Sectors[i].Root}); err != nil {
			t.Fatal(err)
		}
	}

	// assert the slab no longer contains the lost sectors
	slab, err = store.PinnedSlab(context.Background(), account, slabID)
	if err != nil {
		t.Fatal(err)
	}
	expected.Sectors = expected.Sectors[10:] // first 10 sectors are lost
	if !reflect.DeepEqual(slab, expected) {
		t.Fatalf("expected slab %v, got %v", expected, slab)
	}

	// mark the remaining sectors as lost
	for i := range slab.Sectors {
		if err := store.MarkSectorsLost(context.Background(), slab.Sectors[i].HostKey, []types.Hash256{slab.Sectors[i].Root}); err != nil {
			t.Fatal(err)
		}
	}

	_, err = store.PinnedSlab(context.Background(), account, slabID)
	if !errors.Is(err, slabs.ErrUnrecoverable) {
		t.Fatalf("expected ErrUnrecoverable, got %v", err)
	}
}

func TestSlab(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	account := proto.Account{1}

	// add account
	store.addTestAccount(t, types.PublicKey(account))

	// add hosts
	hosts := make([]types.PublicKey, 30)
	for i := range hosts {
		hosts[i] = store.addTestHost(t)
		store.addTestContract(t, hosts[i], frand.Entropy256())
	}

	// pin slab
	params := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     10,
		Sectors:       make([]slabs.PinnedSector, 0, len(hosts)),
	}
	var expectedSectors []slabs.Sector
	for _, host := range hosts {
		root := frand.Entropy256()
		params.Sectors = append(params.Sectors, slabs.PinnedSector{
			Root:    root,
			HostKey: host,
		})
		expectedSectors = append(expectedSectors, slabs.Sector{
			Root:       root,
			HostKey:    &host,
			ContractID: nil, // not pinned to a contract
		})
	}

	// pin slab
	slabIDs, err := store.PinSlabs(context.Background(), account, time.Time{}, params)
	if err != nil {
		t.Fatal(err)
	}

	// fetch slab
	got, err := store.Slab(context.Background(), slabIDs[0])
	if err != nil {
		t.Fatal(err)
	}

	// assert it matches the expected slab
	expectedID, err := params.Digest()
	if err != nil {
		t.Fatal(err)
	}
	expected := slabs.Slab{
		ID:            expectedID,
		EncryptionKey: params.EncryptionKey,
		MinShards:     params.MinShards,
		Sectors:       expectedSectors,
		PinnedAt:      got.PinnedAt, // ignore pinned at
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected slab %v, got %v", expected, got)
	} else if expected.PinnedAt.IsZero() {
		t.Fatal("expected slab to be pinned at a non-zero time")
	}

	// pin the first sector to a contract
	hk := hosts[0]
	fcid := types.FileContractID(hk)
	if err := store.AddFormedContract(context.Background(), hk, fcid, newTestRevision(hk), types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, proto.Usage{}); err != nil {
		t.Fatal(err)
	} else if err := store.PinSectors(context.Background(), fcid, []types.Hash256{params.Sectors[0].Root}); err != nil {
		t.Fatal(err)
	}

	// fetch slab again
	got, err = store.Slab(context.Background(), slabIDs[0])
	if err != nil {
		t.Fatal(err)
	}

	// assert it matches the expected slab with the pinned sector
	expected.Sectors[0].ContractID = &fcid
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected slab %v, got %v", expected, got)
	}
}

func TestSlabIDs(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	// add 2 accounts
	a1 := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(a1))
	a2 := proto.Account{2}
	store.addTestAccount(t, types.PublicKey(a2))

	// add two hosts
	hk1 := store.addTestHost(t)
	hk2 := store.addTestHost(t)
	store.addTestContract(t, hk1)
	store.addTestContract(t, hk2)

	// helper to create slab pin params
	params := func() slabs.SlabPinParams {
		return slabs.SlabPinParams{
			EncryptionKey: frand.Entropy256(),
			MinShards:     1,
			Sectors: []slabs.PinnedSector{
				{
					Root:    frand.Entropy256(),
					HostKey: hk1,
				},
				{
					Root:    frand.Entropy256(),
					HostKey: hk2,
				},
			},
		}
	}

	// pin 2 slabs on account 1
	slabIDs1, err := store.PinSlabs(context.Background(), a1, time.Time{}, params())
	if err != nil {
		t.Fatal(err)
	}
	slabIDs2, err := store.PinSlabs(context.Background(), a1, time.Time{}, params())
	if err != nil {
		t.Fatal(err)
	}
	slabID1 := slabIDs1[0]
	slabID2 := slabIDs2[0]

	// assert account 2 has no slab IDs
	slabIDs, err := store.SlabIDs(context.Background(), a2, 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(slabIDs) != 0 {
		t.Fatalf("expected 0 slab IDs for account 2, got %d", len(slabIDs))
	}

	// assert account 1 has 2 slab IDs
	slabIDs, err = store.SlabIDs(context.Background(), a1, 0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(slabIDs) != 2 {
		t.Fatalf("expected 2 slab IDs for account 1, got %d", len(slabIDs))
	} else if !(slabIDs[0] == slabID2 && slabIDs[1] == slabID1) {
		t.Fatalf("unexpected slab IDs %v, expected [%v,%v]", slabIDs, slabID2, slabID1)
	}

	// assert offset and limit are applied
	if slabIDs, err = store.SlabIDs(context.Background(), a1, 0, 1); err != nil {
		t.Fatal(err)
	} else if len(slabIDs) != 1 {
		t.Fatal("unexpected", len(slabIDs))
	} else if slabIDs[0] != slabID2 {
		t.Fatalf("expected slab ID %v, got %v", slabID2, slabIDs[0])
	} else if slabIDs, err = store.SlabIDs(context.Background(), a1, 1, 1); err != nil {
		t.Fatal(err)
	} else if len(slabIDs) != 1 {
		t.Fatal("unexpected", len(slabIDs))
	} else if slabIDs[0] != slabID1 {
		t.Fatalf("expected slab ID %v, got %v", slabID1, slabIDs[0])
	} else if slabIDs, err = store.SlabIDs(context.Background(), a1, 2, 1); err != nil {
		t.Fatal(err)
	} else if len(slabIDs) != 0 {
		t.Fatalf("expected 0 slab IDs, got %d", len(slabIDs))
	}
}

func TestSlabPruning(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create 2 accounts
	acc1, acc2 := proto.Account{1}, proto.Account{2}
	for _, acc := range []proto.Account{acc1, acc2} {
		store.addTestAccount(t, types.PublicKey(acc))
	}

	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// pin slab for both accounts
	slab1 := slabs.SlabPinParams{
		MinShards: 1,
		Sectors: []slabs.PinnedSector{{
			Root:    frand.Entropy256(),
			HostKey: hk,
		}},
	}
	for _, acc := range []proto.Account{acc1, acc2} {
		if _, err := store.PinSlabs(context.Background(), acc, time.Time{}, slab1); err != nil {
			t.Fatal(err)
		}
	}

	// pin second slab for first account
	slab2 := slabs.SlabPinParams{
		MinShards: 1,
		Sectors: []slabs.PinnedSector{{
			Root:    frand.Entropy256(),
			HostKey: hk,
		}},
	}
	if _, err := store.PinSlabs(context.Background(), acc1, time.Time{}, slab2); err != nil {
		t.Fatal(err)
	}

	// add objects for both accounts
	slab1ID, _ := slab1.Digest()
	obj1 := slabs.SealedObject{
		EncryptedMasterKey: frand.Bytes(72),
		Slabs: []slabs.SlabSlice{
			{
				SlabID: slab1ID,
				Offset: 10,
				Length: 100,
			},
			{
				SlabID: slab1ID,
				Offset: 110,
				Length: 200,
			},
		},
		Signature: types.Signature(frand.Bytes(64)),
	}
	obj1Key := obj1.ID()
	for _, acc := range []proto.Account{acc1, acc2} {
		// note: unique key and signature are required per object. It does not change the object ID
		obj1.EncryptedMasterKey = frand.Bytes(72)
		obj1.Signature = types.Signature(frand.Bytes(64))
		if err := store.SaveObject(context.Background(), acc, obj1); err != nil {
			t.Fatal(err)
		}
	}

	// pin this object to first account only
	slab2ID, _ := slab2.Digest()
	obj2 := slabs.SealedObject{
		EncryptedMasterKey: frand.Bytes(72),
		Slabs: []slabs.SlabSlice{
			{
				SlabID: slab2ID,
				Offset: 10,
				Length: 100,
			},
			{
				SlabID: slab2ID,
				Offset: 110,
				Length: 200,
			},
		},
	}

	if err := store.SaveObject(context.Background(), acc1, obj2); err != nil {
		t.Fatal(err)
	}

	assertSlabs := func(acc proto.Account, expected ...slabs.SlabID) {
		t.Helper()

		got, err := store.SlabIDs(context.Background(), acc, 0, math.MaxInt64)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(expected, got) {
			t.Fatal("mismatched slab IDs")
		}
	}

	assertSlabs(acc1, slab2ID, slab1ID)
	assertSlabs(acc2, slab1ID)

	// delete object for acc1
	if err := store.DeleteObject(context.Background(), acc1, obj1Key); err != nil {
		t.Fatal(err)
	}

	assertSlabs(acc1, slab2ID, slab1ID)
	assertSlabs(acc2, slab1ID)

	// prune slabs for acc1
	if err := store.PruneSlabs(context.Background(), acc1); err != nil {
		t.Fatal(err)
	}

	assertSlabs(acc1, slab2ID)
	assertSlabs(acc2, slab1ID)

	// delete object for acc2
	if err := store.DeleteObject(context.Background(), acc2, obj1Key); err != nil {
		t.Fatal(err)
	}

	assertSlabs(acc1, slab2ID)
	assertSlabs(acc2, slab1ID)

	// prune slabs for acc2
	if err := store.PruneSlabs(context.Background(), acc2); err != nil {
		t.Fatal(err)
	}

	assertSlabs(acc1, slab2ID)
	assertSlabs(acc2)
}

func TestUnpinSlab(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))

	assertAccountSlabs := func(acc proto.Account, expected int64) {
		t.Helper()
		var got int64
		query := `SELECT COUNT(*) FROM account_slabs INNER JOIN accounts ON account_slabs.account_id = accounts.id WHERE accounts.public_key = $1`
		err := store.pool.QueryRow(context.Background(), query, sqlHash256(acc)).Scan(&got)
		if err != nil {
			t.Fatal(err)
		} else if got != expected {
			t.Fatalf("expected %d slabs for account %v, got %d", expected, acc, got)
		}
	}

	assertCount := func(name string, expected int64) {
		t.Helper()
		var got int64
		if err := store.pool.QueryRow(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM %s", name)).Scan(&got); err != nil {
			t.Fatal(err)
		} else if got != expected {
			t.Fatalf("expected %d rows in %s, got %d", expected, name, got)
		}
	}

	assertPinnedData := func(acc proto.Account, pinned uint64) {
		t.Helper()
		var pinnedData uint64
		err := store.pool.QueryRow(context.Background(), "SELECT pinned_data FROM accounts WHERE public_key = $1", sqlPublicKey(acc)).Scan(&pinnedData)
		if err != nil {
			t.Fatal(err)
		} else if pinnedData != pinned {
			t.Fatalf("expected %d pinned data for account %v, got %d", pinned, acc, pinnedData)
		}
	}

	// add host
	hk := store.addTestHost(t)
	store.addTestContract(t, hk)

	// precreate 3 slabs, 2 sectors each
	var params []slabs.SlabPinParams
	slabSize := uint64(2 * proto.SectorSize) // 2 sectors per slab
	for range 3 {
		params = append(params, slabs.SlabPinParams{
			EncryptionKey: [32]byte{},
			MinShards:     2,
			Sectors: []slabs.PinnedSector{
				{Root: frand.Entropy256(), HostKey: hk},
				{Root: frand.Entropy256(), HostKey: hk},
			},
		})
	}
	slab1, _ := params[0].Digest()
	slab2, _ := params[1].Digest()
	slab3, _ := params[2].Digest()

	// add an account with 2 slabs, 2 sectors each
	acc1 := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(acc1))
	if _, err := store.PinSlabs(context.Background(), acc1, time.Time{}, params[0]); err != nil {
		t.Fatal(err)
	} else if _, err := store.PinSlabs(context.Background(), acc1, time.Time{}, params[1]); err != nil {
		t.Fatal(err)
	}

	// add another account with 2 slabs, the first one is shared with acc1
	acc2 := proto.Account{2}
	store.addTestAccount(t, types.PublicKey(acc2))
	if _, err := store.PinSlabs(context.Background(), acc2, time.Time{}, params[1]); err != nil {
		t.Fatal(err)
	} else if _, err := store.PinSlabs(context.Background(), acc2, time.Time{}, params[2]); err != nil {
		t.Fatal(err)
	}

	// assert counts
	assertAccountSlabs(acc1, 2)
	assertAccountSlabs(acc2, 2)
	assertCount("slabs", 3)
	assertCount("sectors", 6)
	assertCount("account_slabs", 4)
	assertPinnedData(acc1, 2*slabSize)
	assertPinnedData(acc2, 2*slabSize)

	// unpinning a slab that's not pinned to an account should return [slabs.ErrNotFound]
	err := store.UnpinSlab(context.Background(), acc2, slab1)
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal("unexpected error:", err)
	}

	// unpin first slab
	err = store.UnpinSlab(context.Background(), acc1, slab1)
	if err != nil {
		t.Fatal(err)
	}

	// assert counts
	assertAccountSlabs(acc1, 1)
	assertAccountSlabs(acc2, 2)
	assertCount("slabs", 2)
	assertCount("sectors", 4)
	assertCount("account_slabs", 3)
	assertPinnedData(acc1, slabSize)
	assertPinnedData(acc2, 2*slabSize)

	// unpin second slab
	err = store.UnpinSlab(context.Background(), acc1, slab2)
	if err != nil {
		t.Fatal(err)
	}

	// assert counts
	assertAccountSlabs(acc1, 0)
	assertAccountSlabs(acc2, 2)
	assertCount("slabs", 2)
	assertCount("sectors", 4)
	assertCount("account_slabs", 2)
	assertPinnedData(acc1, 0)
	assertPinnedData(acc2, 2*slabSize)

	// unpin second slab on second account
	err = store.UnpinSlab(context.Background(), acc2, slab2)
	if err != nil {
		t.Fatal(err)
	}

	// assert counts
	assertAccountSlabs(acc1, 0)
	assertAccountSlabs(acc2, 1)
	assertCount("slabs", 1)
	assertCount("sectors", 2)
	assertCount("account_slabs", 1)
	assertPinnedData(acc1, 0)
	assertPinnedData(acc2, slabSize)

	// unpin third slab on second account
	err = store.UnpinSlab(context.Background(), acc2, slab3)
	if err != nil {
		t.Fatal(err)
	}

	// assert counts
	assertAccountSlabs(acc1, 0)
	assertAccountSlabs(acc2, 0)
	assertCount("slabs", 0)
	assertCount("sectors", 0)
	assertCount("account_slabs", 0)
	assertPinnedData(acc1, 0)
	assertPinnedData(acc2, 0)
}

func BenchmarkPruneSlabs(b *testing.B) {
	const (
		numAccounts       = 1000
		objectsPerAccount = 500
		slabsPerObject    = 3
	)

	store := initPostgres(b, zap.NewNop())

	batch := &pgx.Batch{}
	var accs []proto.Account
	var slabID, objectID int64
	for i := range numAccounts {
		pk := types.GeneratePrivateKey().PublicKey()
		accs = append(accs, proto.Account(pk))

		batch.Queue(`INSERT INTO accounts(public_key, max_pinned_data) VALUES ($1, 1000000);`, sqlPublicKey(pk))
		for j := range objectsPerAccount {
			accountID := i + 1

			var encryptionKey [32]byte
			frand.Read(encryptionKey[:])

			objectKey := sqlHash256(frand.Entropy256())
			if j%2 == 0 {
				objectID++
				batch.Queue(`INSERT INTO objects(object_key, account_id) VALUES ($1, $2)`, objectKey, accountID)
			}
			for k := range slabsPerObject {
				slabID++
				slabDigest := sqlHash256(frand.Entropy256())

				batch.Queue(`INSERT INTO slabs(digest, encryption_key, min_shards) VALUES ($1, $2, 1);`, slabDigest, sqlHash256(encryptionKey))
				batch.Queue(`INSERT INTO account_slabs(account_id, slab_id) VALUES ($1, $2)`, accountID, slabID)
				if j%2 == 0 {
					batch.Queue(`INSERT INTO object_slabs(object_id, slab_digest, slab_index, slab_offset, slab_length) VALUES ($1, $2, $3, 0, 0)`, objectID, slabDigest, k)
				}
			}
		}
	}
	batch.Queue(`UPDATE stats SET num_slabs = $1`, slabID)
	if err := store.pool.SendBatch(b.Context(), batch).Close(); err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		b.ReportMetric(float64(objectsPerAccount)*float64(slabsPerObject)/2.0, "slabs/op")

		if err := store.PruneSlabs(b.Context(), accs[frand.Intn(len(accs))]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSlabs benchmarks Slabs and PinSlabs in various batch sizes. The
// results are expressed in time per operation as well as equivalent
// upload/download throughput.
func BenchmarkSlabs(b *testing.B) {
	store := initPostgres(b, zaptest.NewLogger(b).Named("postgres"))
	account := proto.Account{1}

	store.addTestAccount(b, types.PublicKey(account))

	// 30 hosts to simulate default redundancy
	var hks []types.PublicKey
	for i := byte(0); i < 30; i++ {
		hks = append(hks, store.addTestHost(b, types.PublicKey{i}))
	}
	// add 500 other hosts to reflect mainnet
	for range 500 {
		store.addTestHost(b)
	}

	// helper to create slabs
	newSlab := func() slabs.SlabPinParams {
		var sectors []slabs.PinnedSector
		for i := range hks {
			sectors = append(sectors, slabs.PinnedSector{
				Root:    frand.Entropy256(),
				HostKey: hks[i],
			})
		}
		slab := slabs.SlabPinParams{
			EncryptionKey: frand.Entropy256(),
			MinShards:     1,
			Sectors:       sectors,
		}
		return slab
	}

	const dbBaseSize = 1 << 40         // 1TiB of sectors
	const slabSize = 40 * int64(1<<20) // 40MiB

	// prepare base db
	var initialSlabIDs []slabs.SlabID
	for range dbBaseSize / slabSize {
		slabIDs, err := store.PinSlabs(context.Background(), account, time.Time{}, newSlab())
		if err != nil {
			b.Fatal(err)
		}
		initialSlabIDs = append(initialSlabIDs, slabIDs[0])
	}

	runSlabsBenchmark := func(b *testing.B, nSlabs int) {
		b.SetBytes(slabSize)
		b.ResetTimer()
		for b.Loop() {
			b.StopTimer()
			frand.Shuffle(len(initialSlabIDs), func(i, j int) {
				initialSlabIDs[i], initialSlabIDs[j] = initialSlabIDs[j], initialSlabIDs[i]
			})
			slabIDs := initialSlabIDs[:nSlabs]
			b.StartTimer()

			_, err := store.Slabs(context.Background(), proto.Account{1}, slabIDs)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	// insert 40MiB of slab data
	b.Run("PinSlab", func(b *testing.B) {
		b.SetBytes(slabSize)
		b.ResetTimer()
		for b.Loop() {
			_, err := store.PinSlabs(context.Background(), proto.Account{1}, time.Time{}, newSlab())
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Slab", func(b *testing.B) {
		ids, err := store.PinSlabs(context.Background(), proto.Account{1}, time.Time{}, newSlab())
		if err != nil {
			b.Fatal(err)
		}

		b.SetBytes(slabSize)
		b.ResetTimer()
		for b.Loop() {
			_, err := store.Slab(context.Background(), ids[0])
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("PinnedSlab", func(b *testing.B) {
		ids, err := store.PinSlabs(context.Background(), proto.Account{1}, time.Time{}, newSlab())
		if err != nil {
			b.Fatal(err)
		}

		b.SetBytes(slabSize)
		b.ResetTimer()
		for b.Loop() {
			_, err := store.PinnedSlab(context.Background(), account, ids[0])
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// fetch 40MiB of data at a time
	b.Run("Slabs-40MiB", func(b *testing.B) {
		runSlabsBenchmark(b, 1)
	})

	// fetch 400MiB of data at a time
	b.Run("Slabs-400MiB", func(b *testing.B) {
		runSlabsBenchmark(b, 10)
	})

	// fetch 4GiB of data at a time
	b.Run("Slabs-4GiB", func(b *testing.B) {
		runSlabsBenchmark(b, 100)
	})

	// fetch SlabIDs for account at random offsets
	b.Run("SlabIDs", func(b *testing.B) {
		for b.Loop() {
			offset := frand.Intn(len(initialSlabIDs) - 1000)
			ids, err := store.SlabIDs(context.Background(), proto.Account{1}, offset, 1000)
			if err != nil {
				b.Fatal(err)
			} else if len(ids) != 1000 {
				b.Fatalf("expected 1000 slab IDs, got %d", len(ids))
			}
		}
	})
}

// BenchmarkUnhealthySlabs benchmarks UnhealthySlabs
func BenchmarkUnhealthySlabs(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	account := proto.Account{1}
	store.addTestAccount(b, types.PublicKey(account))

	// 30 hosts to simulate default redundancy
	var hks []types.PublicKey
	for i := range byte(30) {
		hk := store.addTestHost(b, types.PublicKey{i})
		store.addTestContract(b, hk, types.FileContractID(hk))
		hks = append(hks, hk)
	}

	// helper to create slabs
	hostSectors := make([][]types.Hash256, len(hks))
	newSlab := func() slabs.SlabPinParams {
		var sectors []slabs.PinnedSector
		for i := range hks {
			hostSectors[i] = append(hostSectors[i], frand.Entropy256())
			sectors = append(sectors, slabs.PinnedSector{
				Root:    hostSectors[i][len(hostSectors[i])-1],
				HostKey: hks[i],
			})
		}
		slab := slabs.SlabPinParams{
			EncryptionKey: frand.Entropy256(),
			MinShards:     1,
			Sectors:       sectors,
		}
		return slab
	}

	const dbBaseSize = 1 << 40    // 1TiB
	const slabSize = 40 * 1 << 20 // 40MiB

	// prepare base db
	for range dbBaseSize / slabSize {
		_, err := store.PinSlabs(b.Context(), account, time.Time{}, newSlab())
		if err != nil {
			b.Fatal(err)
		}
	}

	// pin sectors
	for i, hk := range hks {
		err := store.PinSectors(b.Context(), types.FileContractID(hk), hostSectors[i])
		if err != nil {
			b.Fatal(err)
		}
	}

	// 25% of the sectors are stored on a bad contract
	_, err := store.pool.Exec(b.Context(), "UPDATE contracts SET good = FALSE WHERE id % 4 = 0")
	if err != nil {
		b.Fatal(err)
	}

	// 25% of the sectors don't have a host at all
	_, err = store.pool.Exec(b.Context(), `UPDATE sectors SET host_id = NULL, contract_sectors_map_id = NULL WHERE id % 4 = 1`)
	if err != nil {
		b.Fatal(err)
	}

	// reset next_repair_attempt
	resetUnhealthySlabs := func() {
		b.Helper()
		_, err = store.pool.Exec(b.Context(), "UPDATE slabs SET next_repair_attempt = (NOW() - interval '3 day') + interval '1 week' * random()")
		if err != nil {
			b.Fatal(err)
		}
	}

	// analyze tables to ensure query planner has up-to-date statistics
	_, vErr1 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) slabs;`)
	_, vErr2 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) sectors;`)
	_, vErr3 := store.pool.Exec(b.Context(), `VACUUM (ANALYZE) contract_sectors_map;`)
	if err := errors.Join(vErr1, vErr2, vErr3); err != nil {
		b.Fatal(err)
	}

	for _, batchSize := range []int{50, 100, 250} {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			var sanityCheck bool
			for b.Loop() {
				slabIDs, err := store.UnhealthySlabs(b.Context(), batchSize)
				if err != nil {
					b.Fatal(err)
				} else if len(slabIDs) < batchSize {
					b.StopTimer()
					resetUnhealthySlabs()
					b.StartTimer()
					continue
				} else if len(slabIDs) != batchSize {
					b.Fatalf("expected %d unhealthy slabs, got %d", batchSize, len(slabIDs))
				}
				sanityCheck = sanityCheck || len(slabIDs) > 0
			}
			if !sanityCheck {
				b.Fatal("sanity check failed, no unhealthy slabs were ever returned")
			}
		})
	}
}

// BenchmarkUnpinSlab precreates slabs and benchmarks the performance of
// UnpinSlab. All slabs are referenced by the same account, every slab is only
// referenced once, that means that unpinning the slab will delete the
// reference, as well as the slab itself.
func BenchmarkUnpinSlab(b *testing.B) {
	store := initPostgres(b, zap.NewNop())

	// add account
	account := proto.Account{1}
	store.addTestAccount(b, types.PublicKey(account))

	// add host with one contract
	hk := store.addTestHost(b)
	store.addTestContract(b, hk)

	// prepare base db
	const (
		dbBaseSize = 1 << 40 // 1TiB of sectors
		nSlabs     = dbBaseSize / (30 * proto.SectorSize)
	)

	// insert slabs
	slabIDs := make([]slabs.SlabID, nSlabs)
	sectors := make([]slabs.PinnedSector, 30)
	for i := range nSlabs {
		for j := range 30 {
			sectors[j] = slabs.PinnedSector{
				Root:    frand.Entropy256(),
				HostKey: hk,
			}
		}
		slabIDs, err := store.PinSlabs(context.Background(), account, time.Time{}, slabs.SlabPinParams{
			MinShards:     1,
			EncryptionKey: frand.Entropy256(),
			Sectors:       sectors,
		})
		if err != nil {
			b.Fatal(err)
		}
		slabIDs[i] = slabIDs[0]
	}

	var iter int
	for b.Loop() {
		rIdx := frand.Intn(len(slabIDs))
		if err := store.UnpinSlab(context.Background(), account, slabIDs[rIdx]); err != nil {
			b.Fatal(err)
		}

		slabIDs = slices.Delete(slabIDs, rIdx, rIdx+1)
		if len(slabIDs) == 0 {
			if iter < 100 {
				b.Fatalf("expected at least 100 iterations, got %d", iter) // sanity check
			}
			b.StopTimer()
			break // exhausted
		}
		iter++
	}
}
