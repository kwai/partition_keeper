package recorder

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"gotest.tools/assert"
)

func TestBriefNode(t *testing.T) {
	input1 := NewBriefNode("aa").(*BriefNode)
	input1.Record[pb.ReplicaRole_kPrimary] = 3
	input1.Record[pb.ReplicaRole_kSecondary] = 5

	output1 := input1.Clone()
	assert.DeepEqual(t, input1, output1)
}
