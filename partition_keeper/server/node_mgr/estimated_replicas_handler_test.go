package node_mgr

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"gotest.tools/assert"
)

func TestEstimatedReplicas(t *testing.T) {
	estReps := NewEstimatedReplicasHandler()

	logging.Info("first add table")
	estReps.Update(int32(1), map[string]int{
		"node1": 3,
		"node2": 3,
		"node3": 2,
	})

	estReps.Get("node4", func(nodeCount map[int32]int) {
		assert.Assert(t, nodeCount == nil)
	})
	estReps.Get("node1", func(nodeCount map[int32]int) {
		assert.DeepEqual(t, nodeCount, map[int32]int{1: 3})
	})

	logging.Info("then add new table")
	estReps.Update(int32(2), map[string]int{
		"node1": 5,
		"node2": 5,
		"node3": 6,
	})

	estReps.Get("node1", func(nodeCount map[int32]int) {
		assert.DeepEqual(t, nodeCount, map[int32]int{1: 3, 2: 5})
	})

	logging.Info("add node4")
	estReps.Update(int32(1), map[string]int{
		"node1": 2,
		"node2": 2,
		"node3": 2,
		"node4": 2,
	})
	estReps.Update(int32(2), map[string]int{
		"node1": 4,
		"node2": 4,
		"node3": 4,
		"node4": 4,
	})
	estReps.Get("node1", func(nodeCount map[int32]int) {
		assert.DeepEqual(t, nodeCount, map[int32]int{1: 2, 2: 4})
	})
	estReps.Get("node4", func(nodeCount map[int32]int) {
		assert.DeepEqual(t, nodeCount, map[int32]int{1: 2, 2: 4})
	})

	logging.Info("remove node1")
	estReps.Update(int32(1), map[string]int{
		"node2": 2,
		"node3": 3,
		"node4": 3,
	})
	estReps.Get("node1", func(nodeCount map[int32]int) {
		assert.DeepEqual(t, nodeCount, map[int32]int{2: 4})
	})
	estReps.Get("node4", func(nodeCount map[int32]int) {
		assert.DeepEqual(t, nodeCount, map[int32]int{1: 3, 2: 4})
	})
	estReps.Update(int32(2), map[string]int{
		"node2": 6,
		"node3": 5,
		"node4": 5,
	})
	estReps.Get("node1", func(nodeCount map[int32]int) {
		assert.Assert(t, nodeCount == nil)
	})
	estReps.Get("node4", func(nodeCount map[int32]int) {
		assert.DeepEqual(t, nodeCount, map[int32]int{1: 3, 2: 5})
	})

	logging.Info("remove table1")
	estReps.Update(int32(1), nil)
	estReps.Get("node4", func(nodeCount map[int32]int) {
		assert.DeepEqual(t, nodeCount, map[int32]int{2: 5})
	})

	logging.Info("remove table2")
	estReps.Update(int32(2), nil)
	estReps.Get("node2", func(nodeCount map[int32]int) {
		assert.Assert(t, nodeCount == nil)
	})
	estReps.Get("node3", func(nodeCount map[int32]int) {
		assert.Assert(t, nodeCount == nil)
	})
	estReps.Get("node4", func(nodeCount map[int32]int) {
		assert.Assert(t, nodeCount == nil)
	})
}
