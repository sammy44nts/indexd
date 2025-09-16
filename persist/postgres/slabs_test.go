package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/accounts"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestSlab(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	account := proto.Account{1}

	// add account
	if err := store.AddAccount(context.Background(), types.PublicKey(account), accounts.AccountMeta{}); err != nil {
		t.Fatal(err)
	}

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
	slabID, err := store.PinSlab(context.Background(), account, time.Time{}, params)
	if err != nil {
		t.Fatal(err)
	}

	// fetch slab
	got, err := store.Slab(context.Background(), slabID)
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
	if err := store.AddFormedContract(context.Background(), hk, fcid, newTestRevision(hk), types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency); err != nil {
		t.Fatal(err)
	} else if err := store.PinSectors(context.Background(), fcid, []types.Hash256{params.Sectors[0].Root}); err != nil {
		t.Fatal(err)
	}

	// fetch slab again
	got, err = store.Slab(context.Background(), slabID)
	if err != nil {
		t.Fatal(err)
	}

	// assert it matches the expected slab with the pinned sector
	expected.Sectors[0].ContractID = &fcid
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected slab %v, got %v", expected, got)
	}
}

func TestPinnedSlab(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	account := proto.Account{1}

	// add account
	if err := store.AddAccount(context.Background(), types.PublicKey(account), accounts.AccountMeta{}); err != nil {
		t.Fatal(err)
	}

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

	slabID, err := store.PinSlab(context.Background(), account, time.Time{}, pinned)
	if err != nil {
		t.Fatal(err)
	}

	slab, err := store.PinnedSlab(context.Background(), slabID)
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
	slab, err = store.PinnedSlab(context.Background(), slabID)
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

	_, err = store.PinnedSlab(context.Background(), slabID)
	if !errors.Is(err, slabs.ErrUnrecoverable) {
		t.Fatalf("expected ErrUnrecoverable, got %v", err)
	}
}

func TestSlabPruning(t *testing.T) {
	store := initPostgres(t, zap.NewNop())

	// create 2 accounts
	acc1, acc2 := proto4.Account{1}, proto4.Account{2}
	for _, acc := range []proto4.Account{acc1, acc2} {
		err := store.AddAccount(context.Background(), types.PublicKey(acc), accounts.AccountMeta{})
		if err != nil {
			t.Fatal(err)
		}
	}

	// pin slab for both accounts
	slab := slabs.SlabPinParams{MinShards: 1}
	for _, acc := range []proto4.Account{acc1, acc2} {
		_, err := store.PinSlab(context.Background(), acc, time.Time{}, slab)
		if err != nil {
			t.Fatal(err)
		}
	}

	// add objects for both accounts
	objKey := frand.Entropy256()
	slabID, _ := slab.Digest()
	obj := slabs.Object{
		Key: objKey,
		Slabs: []slabs.SlabSlice{
			{
				SlabID: slabID,
				Offset: 10,
				Length: 100,
			},
			{
				SlabID: slabID,
				Offset: 110,
				Length: 200,
			},
		},
		Meta: []byte("hello world"),
	}
	for _, acc := range []proto4.Account{acc1, acc2} {
		err := store.SaveObject(context.Background(), acc, obj)
		if err != nil {
			t.Fatal(err)
		}
	}

	assertObjects := func(acc proto4.Account, n int) []slabs.Object {
		t.Helper()
		objects, err := store.ListObjects(context.Background(), acc, slabs.Cursor{}, 10)
		if err != nil {
			t.Fatal(err)
		} else if len(objects) != n {
			t.Fatalf("expected %d objects, got %d", n, len(objects))
		}
		return objects
	}
	assertObj := func(obj, other slabs.Object) {
		t.Helper()
		if other.CreatedAt.IsZero() || other.UpdatedAt.IsZero() {
			t.Fatalf("expected non-zero timestamps, got %v and %v", other.CreatedAt, other.UpdatedAt)
		}
		other.CreatedAt = time.Time{}
		other.UpdatedAt = time.Time{}
		if !reflect.DeepEqual(obj, other) {
			t.Fatalf("objects not equal: expected %+v, got %+v", obj, other)
		}
	}

	// 1 object should exist for both accounts
	objs := assertObjects(acc1, 1)
	assertObj(obj, objs[0])

	objs = assertObjects(acc2, 1)
	assertObj(obj, objs[0])

	// delete object for acc1
	if err := store.DeleteObject(context.Background(), acc1, objKey); err != nil {
		t.Fatal(err)
	}
}
