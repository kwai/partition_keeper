package recorder

import (
	"fmt"
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"gotest.tools/assert"
)

func TestAllNodesRecorder(t *testing.T) {
	input := NewAllNodesRecorder(NewBriefNode)

	input.Add("a", 1, 1, pb.ReplicaRole_kPrimary, 1)
	input.Add("b", 1, 2, pb.ReplicaRole_kLearner, 2)
	input.Add("a", 1, 3, pb.ReplicaRole_kPrimary, 3)

	output := input.Clone().(*AllNodesRecorder)
	assert.Equal(t, fmt.Sprintf("%p", input.build), fmt.Sprintf("%p", output.build))
	assert.DeepEqual(t, input.nodes, output.nodes)

	assert.Equal(t, output.GetNode("a").CountAll(), 2)
	assert.Equal(t, output.GetNode("a").WeightedCountAll(), 4)
	assert.Equal(t, output.GetNode("b").Count(pb.ReplicaRole_kLearner), 1)
	assert.Equal(t, output.GetNode("b").WeightedCount(pb.ReplicaRole_kLearner), 2)
	assert.Equal(t, output.GetNode("b").Count(pb.ReplicaRole_kSecondary), 0)
	assert.Equal(t, output.GetNode("b").WeightedCount(pb.ReplicaRole_kSecondary), 0)
}
