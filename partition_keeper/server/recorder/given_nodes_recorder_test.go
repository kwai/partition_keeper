package recorder

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"gotest.tools/assert"
)

func TestGivenNodesRecorder(t *testing.T) {
	input := RecordGivenNodes(map[string]bool{"aa": true, "bb": true, "c": true}, NewNodePartitions)
	input.Add("aa", 1, 1, pb.ReplicaRole_kPrimary, 1)
	input.Add("aa", 1, 2, pb.ReplicaRole_kSecondary, 2)
	input.Add("bb", 1, 2, pb.ReplicaRole_kLearner, 3)

	output := input.Clone().(*GivenNodesRecorder)
	assert.DeepEqual(t, input, output, cmp.AllowUnexported(GivenNodesRecorder{}))

	assert.DeepEqual(t, output.GetNode("aa").CountAll(), 2)
	assert.DeepEqual(t, output.GetNode("aa").WeightedCountAll(), 3)
	assert.DeepEqual(t, output.GetNode("bb").Count(pb.ReplicaRole_kLearner), 1)
	assert.DeepEqual(t, output.GetNode("bb").WeightedCount(pb.ReplicaRole_kLearner), 3)
	assert.DeepEqual(t, output.GetNode("bb").Count(pb.ReplicaRole_kPrimary), 0)
	assert.Equal(t, output.GetNode("cc"), nil)
}
