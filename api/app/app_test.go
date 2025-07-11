package app_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/internal/testutils"
	"go.sia.tech/indexd/slabs"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

func TestApplicationAPI(t *testing.T) {
	// create cluster with three hosts
	logger := testutils.NewLogger(false)
	c := testutils.NewConsensusNode(t, logger)
	h1 := c.NewHost(t, types.GeneratePrivateKey(), zap.NewNop())
	h2 := c.NewHost(t, types.GeneratePrivateKey(), zap.NewNop())
	h3 := c.NewHost(t, types.GeneratePrivateKey(), zap.NewNop())

	// create indexer
	indexer := testutils.NewIndexer(t, c, logger)

	// fund hosts and indexer wallet
	c.MineBlocks(t, h1.WalletAddress(), 1)
	c.MineBlocks(t, h2.WalletAddress(), 1)
	c.MineBlocks(t, h3.WalletAddress(), 1)
	c.MineBlocks(t, indexer.WalletAddr(), 1)
	c.MineBlocks(t, types.Address{}, c.Network().MaturityDelay)

	// announce hosts
	if err := errors.Join(h1.Announce(), h2.Announce(), h3.Announce()); err != nil {
		t.Fatal(err)
	}
	c.MineBlocks(t, types.Address{}, 1)
	time.Sleep(time.Second)

	// assert hosts are registered
	hosts, err := indexer.Hosts(context.Background())
	if err != nil {
		t.Fatal("failed to get hosts:", err)
	} else if len(hosts) != 3 {
		t.Fatal("expected 3 hosts, got", len(hosts))
	}

	// prepare account
	sk := types.GeneratePrivateKey()
	if err := indexer.AccountsAdd(context.Background(), sk.PublicKey()); err != nil {
		t.Fatal(err)
	}

	// helper to generate slab pin parameters
	params := func() slabs.SlabPinParams {
		return slabs.SlabPinParams{
			EncryptionKey: frand.Entropy256(),
			MinShards:     1,
			Sectors: []slabs.SectorPinParams{
				{
					Root:    frand.Entropy256(),
					HostKey: h1.PublicKey(),
				},
				{
					Root:    frand.Entropy256(),
					HostKey: h2.PublicKey(),
				},
				{
					Root:    frand.Entropy256(),
					HostKey: h3.PublicKey(),
				},
			},
		}
	}

	// pin the slab
	slabID, err := indexer.App(sk).PinSlab(context.Background(), params())
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}

	// unpin the slab
	if err := indexer.App(sk).UnpinSlab(context.Background(), slabID); err != nil {
		t.Fatal("failed to unpin slab:", err)
	}

	// assert minimum redundancy is enforced
	p := params()
	p.Sectors = p.Sectors[:2]
	_, err = indexer.App(sk).PinSlab(context.Background(), p)
	if err == nil || !strings.Contains(err.Error(), slabs.ErrInsufficientRedundancy.Error()) {
		t.Fatal("expected [slabs.ErrInsufficientRedundancy], got:", err)
	}

	// pin 2 slabs
	slab1Params := params()
	slab2Params := params()
	slabID1, err := indexer.App(sk).PinSlab(context.Background(), slab1Params)
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}
	slabID2, err := indexer.App(sk).PinSlab(context.Background(), slab2Params)
	if err != nil {
		t.Fatal("failed to pin slab:", err)
	}

	// assert slab IDs are returned
	slabsIDs, err := indexer.App(sk).SlabDigests(context.Background())
	if err != nil {
		t.Fatal("failed to fetch slabs:", err)
	} else if len(slabsIDs) != 2 {
		t.Fatal("expected 2 slabs, got", len(slabsIDs))
	} else if !reflect.DeepEqual(slabsIDs, []slabs.SlabID{slabID2, slabID1}) {
		t.Fatal("expected slabs to match pinned slabs, got:", slabsIDs)
	}

	// assert offset and limit are passed
	slabsIDs, err = indexer.App(sk).SlabDigests(context.Background(), api.WithOffset(1), api.WithLimit(1))
	if err != nil {
		t.Fatal("failed to fetch slabs with offset and limit:", err)
	} else if len(slabsIDs) != 1 {
		t.Fatal("expected 1 slab, got", len(slabsIDs))
	} else if slabsIDs[0] != slabID1 {
		t.Fatal("expected slabID1, got:", slabsIDs[0])
	}

	// assert slab is returned
	slab1, err := indexer.App(sk).Slab(context.Background(), slabID1)
	if err != nil {
		t.Fatal("failed to fetch slab:", err)
	} else if slab1.EncryptionKey != slab1Params.EncryptionKey {
		t.Fatal("unexpected")
	} else if slab1.Sectors[0].Root != slab1Params.Sectors[0].Root ||
		slab1.Sectors[1].Root != slab1Params.Sectors[1].Root ||
		slab1.Sectors[2].Root != slab1Params.Sectors[2].Root {
		t.Fatal("unexpected sector roots in slab")
	}

	// assert slab is returned
	slab2, err := indexer.App(sk).Slab(context.Background(), slabID2)
	if err != nil {
		t.Fatal("failed to fetch slab:", err)
	} else if slab2.EncryptionKey != slab2Params.EncryptionKey {
		t.Fatal("unexpected")
	} else if slab2.Sectors[0].Root != slab2Params.Sectors[0].Root ||
		slab2.Sectors[1].Root != slab2Params.Sectors[1].Root ||
		slab2.Sectors[2].Root != slab2Params.Sectors[2].Root {
		t.Fatal("unexpected sector roots in slab")
	}
}
