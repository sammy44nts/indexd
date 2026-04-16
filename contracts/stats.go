package contracts

// ContractsStats contains statistics about the contracts in the database.
type ContractsStats struct {
	Contracts    uint64 `json:"contracts"`
	BadContracts uint64 `json:"badContracts"`
	Renewing     uint64 `json:"renewing"`

	TotalCapacity uint64 `json:"totalCapacity"`
	TotalSize     uint64 `json:"totalSize"`
}

// ContractsStats returns statistics about the contracts in the database.
func (cm *ContractManager) ContractsStats() (ContractsStats, error) {
	return cm.store.ContractsStats()
}
