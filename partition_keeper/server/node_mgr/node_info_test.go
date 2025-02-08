package node_mgr

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/est"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

func TestNodeAllowPrimary(t *testing.T) {
	info := &NodeInfo{
		Id:  "node",
		Op:  pb.AdminNodeOp_kNoop,
		Hub: "yz",
		NodePing: NodePing{
			IsAlive:   true,
			Az:        "YZ",
			Address:   utils.FromHostPort("127.0.0.1:1001"),
			ProcessId: "123",
			BizPort:   1002,
		},
	}

	logging.Info("don't allow primary if node not in any hub")
	allow := info.AllowRole([]*pb.ReplicaHub{
		{Name: "zw", Az: "ZW"},
		{Name: "gz", Az: "GZ1"},
	}, pb.ReplicaRole_kPrimary)
	assert.Assert(t, !allow)

	logging.Info("allow primary if hub don't have constraint")
	allow = info.AllowRole([]*pb.ReplicaHub{
		{Name: "yz", Az: "YZ", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kSecondary}},
		{Name: "zw", Az: "ZW"},
		{Name: "gz", Az: "GZ1"},
	}, pb.ReplicaRole_kPrimary)
	assert.Assert(t, allow)

	logging.Info("don't allow primary if hub has constraint")
	allow = info.AllowRole([]*pb.ReplicaHub{
		{Name: "yz", Az: "YZ", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kPrimary}},
		{Name: "zw", Az: "ZW"},
		{Name: "gz", Az: "GZ1"},
	}, pb.ReplicaRole_kPrimary)
	assert.Assert(t, !allow)
}

func TestGetWeightScore(t *testing.T) {
	info := &NodeInfo{}
	info.Weight = INVALID_WEIGHT
	assert.Equal(t, info.GetWeight(), DEFAULT_WEIGHT)
	info.Weight = 2
	assert.Equal(t, info.GetWeight(), WeightType(2))
	info.Weight = MAX_WEIGHT + 1
	assert.Equal(t, info.GetWeight(), MAX_WEIGHT)

	info.Weight = 1
	info.Score = 15
	assert.Equal(t, info.WeightedScore(), est.ScoreType(15))
}
