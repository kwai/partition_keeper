package recorder

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"gotest.tools/assert"
)

func TestNodePartitions(t *testing.T) {
	p1 := NewNodePartitions("hello").(*NodePartitions)

	p1.Add(1, 1, pb.ReplicaRole_kPrimary, 2)
	p1.Add(1, 2, pb.ReplicaRole_kPrimary, 3)
	p1.Add(1, 3, pb.ReplicaRole_kSecondary, 1)

	p2 := p1.Clone().(*NodePartitions)
	assert.DeepEqual(t, p1, p2)
	assert.Equal(t, p2.Count(pb.ReplicaRole_kPrimary), 2)
	assert.Equal(t, p2.Count(pb.ReplicaRole_kLearner), 0)
	assert.Equal(t, p2.WeightedCount(pb.ReplicaRole_kPrimary), 5)
	assert.Equal(t, p2.WeightedCount(pb.ReplicaRole_kLearner), 0)
	assert.Equal(t, p2.CountAll(), 3)
	assert.Equal(t, p2.WeightedCountAll(), 6)
}
