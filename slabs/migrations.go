package slabs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

type (
	// Shard represents a sector present on a host.
	Shard struct {
		Root    types.Hash256
		HostKey types.PublicKey
	}
)

func (m *SlabManager) migrateSlabs(ctx context.Context, slabs []Slab, l *zap.Logger) error {
	logger := l.Named(hex.EncodeToString(frand.Bytes(16))) // unique id per batch

	// fetch all available contracts
	var availableContracts []contracts.Contract
	const batchSize = 50
	for offset := 0; ; offset += batchSize {
		batch, err := m.store.Contracts(ctx, offset, batchSize,
			contracts.WithRevisable(true), contracts.WithGood(true))
		if err != nil {
			return fmt.Errorf("failed to fetch available contracts: %w", err)
		}
		availableContracts = append(availableContracts, batch...)
		if len(batch) < batchSize {
			break
		}
	}

	// fetch all available hosts
	var availableHosts []hosts.Host
	for offset := 0; ; offset += batchSize {
		batch, err := m.store.Hosts(ctx, 0, 0, hosts.WithUsable(true),
			hosts.WithBlocked(false), hosts.WithActiveContracts(true))
		if err != nil {
			return fmt.Errorf("failed to fetch available hosts: %w", err)
		}
		availableHosts = append(availableHosts, batch...)
		if len(batch) < batchSize {
			break
		}
	}

	// maintenance settings
	ms, err := m.store.MaintenanceSettings(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch maintenance settings: %w", err)
	}

	var wg sync.WaitGroup
	for _, slab := range slabs {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := m.migrateSlab(ctx, slab, availableHosts, availableContracts, ms.Period, logger); err != nil {
				logger.Error("failed to migrate slab", zap.Error(err))
				return
			}
		}()
	}
	wg.Wait()
	return nil
}

func (m *SlabManager) migrateSlab(ctx context.Context, slab Slab, hosts []hosts.Host, contracts []contracts.Contract, period uint64, l *zap.Logger) error {
	logger := l.Named(slab.ID.String())

	indices, hosts := contractsForRepair(slab, hosts, contracts, period)
	if len(indices) == 0 {
		logger.Debug("tried to migrate slab but no indices require migration")
		return nil
	}

	// generous timeout for repairing a slab
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	shards, err := downloadSlab(ctx, m.client, slab)
	if err != nil {
		return fmt.Errorf("failed to download slab %s: %w", slab.ID, err)
	}

	toMigrate := shards[:0]
	for _, i := range indices {
		toMigrate = append(toMigrate, shards[i])
	}

	migratedShards, err := uploadShards(ctx, m.client, toMigrate, hosts)
	if err != nil {
		return fmt.Errorf("failed to upload migrated shards for slab %s: %w", slab.ID, err)
	} else if len(migratedShards) == 0 {
		logger.Debug("no shards were migrated")
		return nil
	}

	for _, shard := range migratedShards {
		if _, err := m.store.MigrateSector(ctx, shard.Root, shard.HostKey); err != nil {
			return fmt.Errorf("failed to migrate sector %s: %w", shard.Root, err)
		}
	}

	logger.Debug("successfully migrated slab", zap.Int("toMigrate", len(toMigrate)), zap.Int("migrated", len(migratedShards)))
	return nil
}

// contractsForRepair filters the sectors of a slab and returns the indices of the sectors that
// require migration together with the contracts to use for them.
func contractsForRepair(slab Slab, availableHosts []hosts.Host, availableContracts []contracts.Contract, period uint64) ([]int, []hosts.Host) {
	// prepare a map of good hosts
	hostsMap := make(map[types.PublicKey]hosts.Host)
	for _, host := range availableHosts {
		if host.IsGood() {
			hostsMap[host.PublicKey] = host
		}
	}

	// prepare a map of good contracts
	goodContractMap := make(map[types.FileContractID]contracts.Contract)
	for _, contract := range availableContracts {
		host, ok := hostsMap[contract.HostKey]
		if ok && contract.GoodForUpload(host.Settings.Prices, host.Settings.MaxCollateral, period) {
			goodContractMap[contract.ID] = contract
		}
	}

	// remember the CIDRs of the hosts that good sectors are stored on. We don't
	// care if two good sectors are stored on the same CIDR but we don't want to
	// migrate bad sectors to the same CIDR.
	usedCIDRs := make(map[string]struct{})

	// determine whether the sector needs to be migrated. That's the case if
	// one of the following is true:
	// - the sector was marked lost (contract ID and host key are nil)
	// - the sector is stored on a bad contract
	var toMigrate []int
	for i, sector := range slab.Sectors {
		isLost := sector.ContractID == nil && sector.HostKey == nil
		goodContract := sector.ContractID != nil && goodContractMap[*sector.ContractID] != contracts.Contract{}
		if isLost || !goodContract {
			toMigrate = append(toMigrate, i)
			continue
		}

		// remove contract from the map since we don't want to use it again
		delete(goodContractMap, *sector.ContractID)

		// add the CIDRs of the host to the map
		for _, network := range hostsMap[*sector.HostKey].Networks {
			usedCIDRs[network.String()] = struct{}{}
		}
	}

	// return all hosts with contracts that are good, not in use and are not
	// stored on bad hosts
	var remainingHosts []hosts.Host
	usedHost := make(map[types.PublicKey]struct{})
LOOP:
	for _, contract := range goodContractMap {
		h := hostsMap[contract.HostKey]
		for _, network := range h.Networks {
			if _, ok := usedCIDRs[network.String()]; ok {
				continue LOOP
			}
		}
		if _, ok := usedHost[contract.HostKey]; ok {
			continue LOOP
		}
		remainingHosts = append(remainingHosts, h)
		usedHost[contract.HostKey] = struct{}{}
	}
	return toMigrate, remainingHosts
}

func downloadSlab(ctx context.Context, client HostClient, slab Slab) ([][]byte, error) {
	return nil, errors.New("not implemented")
}

func uploadShards(ctx context.Context, client HostClient, shards [][]byte, hosts []hosts.Host) ([]Shard, error) {
	return nil, errors.New("not implemented")
}
