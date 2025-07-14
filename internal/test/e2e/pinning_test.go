package e2e

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api/admin"
	"go.sia.tech/indexd/internal/test"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
)

func TestSlabPinning(t *testing.T) {
	// create cluster
	logger := test.NewLogger(false)
	cluster := test.NewCluster(t, test.WithLogger(logger), test.WithHosts(3))
	indexer := cluster.Indexer
	time.Sleep(time.Second)

	// add an account
	a1 := types.GeneratePrivateKey()
	err := indexer.AccountsAdd(context.Background(), a1.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	// assert we have 3 usable hosts
	time.Sleep(time.Second)
	hosts, err := indexer.Hosts(context.Background(), admin.WithUsable(true), admin.WithActiveContracts(true))
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 3 {
		t.Fatalf("expected 3 usable hosts, got %d", len(hosts))
	}

	// convenience variables
	acc := proto.Account(a1.PublicKey())
	client := indexer.App(a1)

	// prepare pin params
	params := slabs.SlabPinParams{
		EncryptionKey: frand.Entropy256(),
		MinShards:     1,
	}

	// upload a random sector to each host
	for _, host := range hosts {
		var sector [proto.SectorSize]byte
		frand.Read(sector[:])
		_, err = indexer.HostClient(host.PublicKey).WriteSector(context.Background(), host.Settings.Prices, acc.Token(a1, host.PublicKey), bytes.NewReader(sector[:]), proto.SectorSize)
		if err != nil {
			t.Fatal(err)
		}
		params.Sectors = append(params.Sectors, slabs.SectorPinParams{Root: proto.SectorRoot(&sector), HostKey: host.PublicKey})
	}

	// pin the slab
	slabID, err := client.PinSlab(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}
	slabIDs := []slabs.SlabID{slabID}

	// assert the slab is pinned
	time.Sleep(time.Second)
	res, err := indexer.Database().Slabs(context.Background(), acc, slabIDs)
	if err != nil {
		t.Fatal(err)
	} else if len(res) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(res))
	}

	// assert the sectors were pinned
	for _, sector := range res[0].Sectors {
		if sector.HostKey == nil || sector.ContractID == nil {
			t.Fatal("sector is not pinned")
		}
	}

	// unpin the slab
	if err := client.UnpinSlab(context.Background(), slabID); err != nil {
		t.Fatal(err)
	}

	// assert the slab is unpinned
	_, err = indexer.Database().Slabs(context.Background(), acc, slabIDs)
	if !errors.Is(err, slabs.ErrSlabNotFound) {
		t.Fatal("unexpected error", err)
	}
}
