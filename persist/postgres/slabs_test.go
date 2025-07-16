package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

func TestSlab(t *testing.T) {
	store := initPostgres(t, zaptest.NewLogger(t).Named("postgres"))
	account := proto.Account{1}

	// add account
	if err := store.AddAccount(context.Background(), types.PublicKey(account)); err != nil {
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
		Sectors:       make([]slabs.SectorPinParams, 0, len(hosts)),
	}
	for _, host := range hosts {
		pinned.Sectors = append(pinned.Sectors, slabs.SectorPinParams{
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
		expected.Sectors[i] = slabs.PinnedSector{
			Root:    sector.Root,
			HostKey: sector.HostKey,
		}
	}

	slabID, err := store.PinSlab(context.Background(), account, time.Time{}, pinned)
	if err != nil {
		t.Fatal(err)
	}

	slab, err := store.Slab(context.Background(), slabID)
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
	slab, err = store.Slab(context.Background(), slabID)
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

	_, err = store.Slab(context.Background(), slabID)
	if !errors.Is(err, slabs.ErrUnrecoverable) {
		t.Fatalf("expected ErrUnrecoverable, got %v", err)
	}
}
