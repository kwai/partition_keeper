package sched

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

func CmpActions(t *testing.T, left, right *actions.PartitionActions) {
	assert.Equal(t, left.ActionCount(), right.ActionCount())
	for i := 0; i < left.ActionCount(); i++ {
		assert.Equal(t, left.GetAction(i).String(), right.GetAction(i).String())
	}
}

func TestHubAllNodesReady(t *testing.T) {
	assert.Equal(t, HubAllNodesReady("test", "test", nil), false)
	assert.Equal(t, HubAllNodesReady("test", "test", []*node_mgr.NodeInfo{}), false)
	assert.Equal(t, HubAllNodesReady("test", "test", []*node_mgr.NodeInfo{
		{
			Id:     "node1",
			Op:     pb.AdminNodeOp_kOffline,
			Hub:    "test",
			Weight: 10,
			Score:  0,
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "",
				BizPort:   2001,
				NodeIndex: "0.0",
			},
		},
	}), false)

	assert.Equal(t, HubAllNodesReady("test", "test", []*node_mgr.NodeInfo{
		{
			Id:     "node1",
			Op:     pb.AdminNodeOp_kOffline,
			Hub:    "test",
			Weight: 10,
			Score:  0,
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "",
				BizPort:   2001,
				NodeIndex: "0.0",
			},
		},
		{
			Id:     "node2",
			Op:     pb.AdminNodeOp_kNoop,
			Hub:    "test",
			Weight: 10,
			Score:  0,
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "",
				BizPort:   2001,
				NodeIndex: "0.0",
			},
		},
	}), false)

	assert.Equal(t, HubAllNodesReady("test", "test", []*node_mgr.NodeInfo{
		{
			Id:     "node1",
			Op:     pb.AdminNodeOp_kOffline,
			Hub:    "test",
			Weight: 10,
			Score:  0,
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "",
				BizPort:   2001,
				NodeIndex: "0.0",
			},
		},
		{
			Id:     "node2",
			Op:     pb.AdminNodeOp_kNoop,
			Hub:    "test",
			Weight: 0,
			Score:  0,
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "",
				BizPort:   2001,
				NodeIndex: "0.0",
			},
		},
	}), false)

	assert.Equal(t, HubAllNodesReady("test", "test", []*node_mgr.NodeInfo{
		{
			Id:     "node1",
			Op:     pb.AdminNodeOp_kOffline,
			Hub:    "test",
			Weight: 10,
			Score:  0,
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "",
				BizPort:   2001,
				NodeIndex: "0.0",
			},
		},
		{
			Id:     "node2",
			Op:     pb.AdminNodeOp_kNoop,
			Hub:    "test",
			Weight: 10,
			Score:  1,
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "",
				BizPort:   2001,
				NodeIndex: "0.0",
			},
		},
	}), true)
}

func TestEstimateReplicas(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 2, []int{2, 3}, 32)

	te.nodes.MustGetNodeInfo("node_yz0_0").Op = pb.AdminNodeOp_kOffline
	te.nodes.MustGetNodeInfo("node_yz0_1").Op = pb.AdminNodeOp_kOffline
	count, err := EstimateTableReplicas("test", te.nodes, te.table)
	assert.NilError(t, err)
	minVal := 10000
	maxVal := 0
	total := 0
	for _, count := range count {
		minVal = utils.Min(minVal, count)
		maxVal = utils.Max(maxVal, count)
		total += count
	}
	assert.Equal(t, total, 32)
	assert.Equal(t, minVal, 10)
	assert.Equal(t, maxVal, 11)

	te.nodes.MustGetNodeInfo("node_yz1_0").Score = 0
	te.nodes.MustGetNodeInfo("node_yz1_1").Score = 0
	te.nodes.MustGetNodeInfo("node_yz1_2").Score = 0
	_, err = EstimateTableReplicas("test", te.nodes, te.table)
	assert.Assert(t, err != nil)

	te.nodes.MustGetNodeInfo("node_yz1_0").Score = 100
	te.nodes.MustGetNodeInfo("node_yz1_0").Weight = 1
	te.nodes.MustGetNodeInfo("node_yz1_1").Score = 200
	te.nodes.MustGetNodeInfo("node_yz1_1").Weight = 0.1
	te.nodes.MustGetNodeInfo("node_yz1_2").Score = 0
	te.nodes.MustGetNodeInfo("node_yz1_2").Weight = 0
	count, err = EstimateTableReplicas("test", te.nodes, te.table)
	assert.NilError(t, err)

	assert.Equal(t, count["node_yz1_2"], 0)
	assert.Assert(t, count["node_yz1_1"] > 0)
	assert.Assert(t, count["node_yz1_0"] > count["node_yz1_1"])
	assert.Equal(t, count["node_yz1_0"]+count["node_yz1_1"], 32)
}
