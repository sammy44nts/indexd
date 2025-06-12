package slabs

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/contracts"
	"go.sia.tech/indexd/hosts"
	"go.uber.org/zap"
	"lukechampine.com/frand"
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

	// TODO: make sure prices are up-to-date

	var wg sync.WaitGroup
	for _, slab := range slabs {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := m.migrateSlab(ctx, slab, logger); err != nil {
				logger.Error("failed to migrate slab", zap.Error(err))
				return
			}
		}()
	}
	wg.Wait()
	return nil
}

func (m *SlabManager) migrateSlab(ctx context.Context, slab Slab, l *zap.Logger) error {
	logger := l.Named(slab.ID.String())

	if true {
		panic("migrate")
	}

	logger.Debug("successfully migrated slab")
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
