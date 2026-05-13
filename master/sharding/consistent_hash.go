package sharding

import (
	"hash/fnv"
)

type ConsistentHash struct {
	NumVirtualNodes int
}

func NewConsistentHash(numVirtualNodes int) *ConsistentHash {
	return &ConsistentHash{NumVirtualNodes: numVirtualNodes}
}

func (c *ConsistentHash) Hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % c.NumVirtualNodes
}
