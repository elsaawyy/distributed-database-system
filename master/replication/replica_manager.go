package replication

import (
	"errors"
	"sort"

	"distributed-db/master/health"
	"distributed-db/master/metadata"
)

// ReplicaManager handles assignment of primary and replica workers for each shard.
type ReplicaManager struct {
	metaManager       *metadata.Manager
	healthMon         *health.Monitor
	ReplicationFactor int
}

// NewReplicaManager creates a new ReplicaManager instance.
func NewReplicaManager(metaMgr *metadata.Manager, healthMon *health.Monitor, factor int) *ReplicaManager {
	return &ReplicaManager{
		metaManager:       metaMgr,
		healthMon:         healthMon,
		ReplicationFactor: factor,
	}
}

// AssignShards distributes shards across available workers.
// For each shard ID, it selects a primary worker (round‑robin based on shard ID)
// and then replica workers (next workers in the sorted list).
// It stores the assignments in the metadata database.
func (rm *ReplicaManager) AssignShards(table *metadata.Table, shardIDs []int) error {
	// Get currently alive workers
	workers := rm.healthMon.GetAliveWorkers()
	if len(workers) < rm.ReplicationFactor {
		return errors.New("not enough alive workers to satisfy replication factor")
	}

	// Sort workers by ID to ensure deterministic assignment
	sortedWorkers := make([]health.WorkerInfo, len(workers))
	copy(sortedWorkers, workers)
	sort.Slice(sortedWorkers, func(i, j int) bool {
		return sortedWorkers[i].ID < sortedWorkers[j].ID
	})

	// Assign each shard
	for _, shardID := range shardIDs {
		// Primary: round-robin based on shard ID
		primaryIdx := shardID % len(sortedWorkers)
		primary := sortedWorkers[primaryIdx]

		// Register primary
		err := rm.metaManager.RegisterShard(table.DBName, table.TableName, shardID, primary.ID, true)
		if err != nil {
			return err
		}

		// Assign replicas
		for r := 1; r < rm.ReplicationFactor; r++ {
			replicaIdx := (primaryIdx + r) % len(sortedWorkers)
			replica := sortedWorkers[replicaIdx]
			err := rm.metaManager.RegisterShard(table.DBName, table.TableName, shardID, replica.ID, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// GetPrimaryWorker returns the worker URL for the primary copy of a given shard.
func (rm *ReplicaManager) GetPrimaryWorker(dbName, tableName string, shardID int) (string, error) {
	return rm.metaManager.GetPrimaryWorkerForShard(dbName, tableName, shardID)
}

// GetReplicaWorkers returns a list of replica worker URLs for a given shard.
func (rm *ReplicaManager) GetReplicaWorkers(dbName, tableName string, shardID int) ([]string, error) {
	// This would query the metadata for all non-primary workers for the shard.
	// For brevity, we assume a method exists in metadata.Manager:
	//   GetReplicaWorkersForShard(dbName, tableName, shardID) ([]string, error)
	// If not implemented, you can extend metadata.Manager accordingly.
	// Here we return an empty slice as placeholder.
	return []string{}, nil
}
