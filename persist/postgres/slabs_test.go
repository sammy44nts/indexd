package postgres

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestSlab(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	account := proto.Account{1}

	// add account
	store.addTestAccount(t, types.PublicKey(account))

	// add hosts
	hosts := make([]types.PublicKey, 30)
	for i := range hosts {
		hosts[i] = store.addTestHost(t)
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

func TestMarkSlabRepaired(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// add account
	account := proto.Account{1}
	store.addTestAccount(t, types.PublicKey(account))

	// add slab
	slabIDs, err := store.PinSlabs(context.Background(), account, time.Time{}, slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
		Sectors:       nil,
	})
	if err != nil {
		t.Fatal(err)
	}

	assertSlabState := func(expectedRepairs int, expectedLastAttempt time.Time, expectedNextAttempt time.Time) {
		t.Helper()

		roughlyEqual := func(a, b time.Time, slack time.Duration) bool {
			if diff := a.Sub(b); diff < -slack || diff > slack {
				return false
			}
			return true
		}

		var consecutiveFailedRepairs int
		var lastRepairAttempt, nextRepairAttempt time.Time
		if err := store.pool.QueryRow(t.Context(), `
			SELECT consecutive_failed_repairs, last_repair_attempt, next_repair_attempt 
			FROM slabs 
			WHERE digest = $1`, sqlHash256(slabIDs[0])).Scan(&consecutiveFailedRepairs, &lastRepairAttempt, &nextRepairAttempt); err != nil {
			t.Fatal(err)
		} else if consecutiveFailedRepairs != expectedRepairs {
			t.Fatalf("expected %d consecutive failed repairs, got %d", expectedRepairs, consecutiveFailedRepairs)
		} else if !roughlyEqual(lastRepairAttempt, expectedLastAttempt, time.Second) {
			t.Fatalf("expected last repair attempt %s, got %s", expectedLastAttempt, lastRepairAttempt)
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
	assertSlabState(0, time.Now(), time.Now())

	// assert state after failed repair
	simulateFailedRepair()
	assertSlabState(1, time.Now(), time.Now().Add(minRepairBackoff))

	// assert backoff is capped at maxRepairBackoff (at 6 consec. failures we exceed it)
	for i := range 6 {
		simulateFailedRepair()
		if i < 4 {
			assertSlabState(i+2, time.Now(), time.Now().Add(minRepairBackoff*time.Duration(1<<(i+1))))
		} else {
			assertSlabState(i+2, time.Now(), time.Now().Add(maxRepairBackoff))
		}
	}

	// assert last_repair_attempt is updated on failure
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	_, err = store.pool.Exec(t.Context(), "UPDATE slabs SET last_repair_attempt = $1", oneHourAgo)
	if err != nil {
		t.Fatal(err)
	}
	simulateFailedRepair()
	assertSlabState(8, time.Now(), time.Now().Add(maxRepairBackoff))

	// assert state after successful repair
	_, err = store.pool.Exec(t.Context(), "UPDATE slabs SET last_repair_attempt = $1, next_repair_attempt = $2", oneHourAgo, oneHourAgo)
	if err != nil {
		t.Fatal(err)
	}
	simulateSuccessfulRepair()
	assertSlabState(0, time.Now(), oneHourAgo)
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

func TestSlabPruning(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create 2 accounts
	acc1, acc2 := proto.Account{1}, proto.Account{2}
	for _, acc := range []proto.Account{acc1, acc2} {
		store.addTestAccount(t, types.PublicKey(acc))
	}

	// pin slab for both accounts
	slab1 := slabs.SlabPinParams{MinShards: 1}
	for _, acc := range []proto.Account{acc1, acc2} {
		if _, err := store.PinSlabs(context.Background(), acc, time.Time{}, slab1); err != nil {
			t.Fatal(err)
		}
	}

	// pin second slab for first account
	slab2 := slabs.SlabPinParams{MinShards: 2}
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
	contractID := store.addTestContract(t, hk, types.FileContractID(hk))

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

	// assert slab becomes unrepairable after max consecutive failed repairs
	_, err = store.pool.Exec(context.Background(), "UPDATE sectors SET host_id = NULL, contract_sectors_map_id = NULL WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.pool.Exec(context.Background(), "UPDATE slabs SET consecutive_failed_repairs = $1", maxConsecutiveRepairFailures)
	if err != nil {
		t.Fatal(err)
	}
	assertUnhealthySlabs(0, 10)
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
