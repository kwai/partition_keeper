package base

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"gotest.tools/assert"
)

func TestStdTransformLearner(t *testing.T) {
	self := &pb.PartitionReplica{
		Role:           pb.ReplicaRole_kSecondary,
		StatisticsInfo: map[string]string{pb.StdReplicaStat_unavailable.String(): ""},
		ReadyToPromote: true,
	}
	target := StdTransformLearner(self)
	assert.Equal(t, target, pb.ReplicaRole_kSecondary)

	self.Role = pb.ReplicaRole_kLearner
	target = StdTransformLearner(self)
	assert.Equal(t, target, pb.ReplicaRole_kLearner)

	self.StatisticsInfo = map[string]string{"a": "b"}
	target = StdTransformLearner(self)
	assert.Equal(t, target, pb.ReplicaRole_kSecondary)

	self.StatisticsInfo = nil
	target = StdTransformLearner(self)
	assert.Equal(t, target, pb.ReplicaRole_kSecondary)

	self.ReadyToPromote = false
	target = StdTransformLearner(self)
	assert.Equal(t, target, pb.ReplicaRole_kLearner)
}

func TestStdDowngrade(t *testing.T) {
	self := &pb.PartitionReplica{
		Role:           pb.ReplicaRole_kLearner,
		StatisticsInfo: nil,
		ReadyToPromote: true,
	}
	target := StdDowngrade(self)
	assert.Equal(t, target, pb.ReplicaRole_kLearner)

	self.Role = pb.ReplicaRole_kPrimary
	target = StdDowngrade(self)
	assert.Equal(t, target, pb.ReplicaRole_kPrimary)

	self.StatisticsInfo = map[string]string{"a": "b"}
	target = StdDowngrade(self)
	assert.Equal(t, target, pb.ReplicaRole_kPrimary)

	self.StatisticsInfo = map[string]string{pb.StdReplicaStat_unavailable.String(): ""}
	target = StdDowngrade(self)
	assert.Equal(t, target, pb.ReplicaRole_kLearner)
}
