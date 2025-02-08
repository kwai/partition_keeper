package sim

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"gotest.tools/assert"
)

func TestSimTransform(t *testing.T) {
	st := Create()

	runningOpts := &base.TableRunningOptions{
		IsRestoring: false,
	}
	logging.Info("can't promote if node itself not ready")
	self := &pb.PartitionReplica{
		Role: pb.ReplicaRole_kSecondary,
		Node: &pb.RpcNode{
			NodeName: "127.0.0.1",
			Port:     1001,
		},
		StatisticsInfo: nil,
		ReadyToPromote: true,
		HubName:        "yz1",
		NodeUniqueId:   "node1",
	}
	assert.Equal(
		t,
		st.Transform(utils.MakeTblPartID(1, 1), self, nil, nil, runningOpts),
		pb.ReplicaRole_kSecondary,
	)

	self.StatisticsInfo = map[string]string{
		"schema_changing": "true",
	}
	assert.Equal(
		t,
		st.Transform(utils.MakeTblPartID(1, 1), self, nil, nil, runningOpts),
		pb.ReplicaRole_kLearner,
	)

	self.Role = pb.ReplicaRole_kLearner
	self.StatisticsInfo = map[string]string{
		"schema_changing": "false",
	}
	assert.Equal(
		t,
		st.Transform(utils.MakeTblPartID(1, 1), self, nil, nil, runningOpts),
		pb.ReplicaRole_kSecondary,
	)
}
