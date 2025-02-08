package node_mgr

import (
	"context"
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
	"gotest.tools/assert/cmp"
)

func TestStaticAllocateHub(t *testing.T) {
	te := setupNodeStatTestEnv(
		t,
		map[string]string{"yz_1": "yz", "zw_1": "zw"},
		pb.NodeFailureDomainType_HOST,
	)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"node2_yz1", pb.AdminNodeOp_kNoop, "yz_1", 10, 0,
			NodePing{true, "yz", utils.FromHostPort("127.0.1.1:1001"), "10001", 2001, "1.0"},
			nil,
		},
		{
			"node1", pb.AdminNodeOp_kNoop, "zw_1", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.0.1:1001"), "10001", 2001, "1.1"},
			nil,
		},
		{
			"node5", pb.AdminNodeOp_kNoop, "zw_1", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.0.5:1001"), "10001", 2001, "1.2"},
			nil,
		},
	}
	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	handle := utils.MapHubs([]*pb.ReplicaHub{
		{Name: "zw_1", Az: "zw"},
		{Name: "yz_1", Az: "yz"},
	})
	te.nodeStats.LoadFromZookeeper(handle, false)
	assert.Equal(t, len(te.nodeStats.idNodes), len(infos))

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()

	newPing := map[string]*NodePing{
		"node2": {true, "zw", utils.FromHostPort("127.0.0.2:1001"), "10002", 2001, "1.0"},
		"node3": {true, "zw", utils.FromHostPort("127.0.0.3:1001"), "10003", 2001, "1.1"},
		"node4": {true, "zw", utils.FromHostPort("127.0.0.4:1001"), "10004", 2001, "0.1"},
		"node5": {false, "zw", utils.FromHostPort("127.0.0.5:1001"), "10001", 2001, "1.2"},
		"node6": {true, "zw", utils.FromHostPort("127.0.0.6:1001"), "10001", 2001, "1.2"},
	}

	te.nodeStats.UpdateStats(newPing)
	assert.Equal(t, len(te.nodeStats.idNodes), 7)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node1").Op, pb.AdminNodeOp_kOffline)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node5").Op, pb.AdminNodeOp_kOffline)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node5").IsAlive, false)

	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node3").Op, pb.AdminNodeOp_kNoop)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node3").Hub, "zw_1")

	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2").Op, pb.AdminNodeOp_kNoop)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2").Hub, "zw_1")

	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node4").Op, pb.AdminNodeOp_kOffline)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node4").Hub, "zw_0")

	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node6").Op, pb.AdminNodeOp_kNoop)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node6").Hub, "zw_1")

	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2_yz1").Op, pb.AdminNodeOp_kNoop)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2_yz1").Hub, "yz_1")

	te.checkZkSynced()
}
func TestStaticShrinkAz(t *testing.T) {
	te := setupNodeStatTestEnv(
		t,
		map[string]string{"ZW_0": "ZW", "ZW_1": "ZW"},
		pb.NodeFailureDomainType_HOST,
	)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"zw0_node1", pb.AdminNodeOp_kNoop, "ZW_0", 10, 0,
			NodePing{true, "ZW", utils.FromHostPort("127.0.0.1:1001"), "10001", 2001, "0.1"},
			nil,
		},
		{
			"zw0_node2", pb.AdminNodeOp_kNoop, "ZW_0", 10, 0,
			NodePing{false, "ZW", utils.FromHostPort("127.0.0.2:1001"), "10001", 2001, "0.0"},
			nil,
		},
		{
			"zw1_node1", pb.AdminNodeOp_kNoop, "ZW_1", 10, 0,
			NodePing{true, "ZW", utils.FromHostPort("127.0.1.1:1001"), "10001", 2001, "1.0"},
			nil,
		},
		{
			"zw1_node2", pb.AdminNodeOp_kNoop, "ZW_1", 10, 0,
			NodePing{false, "ZW", utils.FromHostPort("127.0.1.2:1001"), "10001", 2001, "1.1"},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	handle := utils.MapHubs([]*pb.ReplicaHub{
		{Name: "ZW_0", Az: "ZW"},
		{Name: "ZW_1", Az: "ZW"},
	})
	te.nodeStats.LoadFromZookeeper(handle, false)
	assert.Equal(t, len(te.nodeStats.idNodes), len(infos))

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()
	resp := &pb.ShrinkAzResponse{}
	te.nodeStats.ShrinkAz("ZW", 3, nil, true, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	resp.Status.Code = 0
	te.nodeStats.ShrinkAz("YZ", 2, nil, true, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	resp.Status.Code = 0
	te.nodeStats.ShrinkAz("ZW", 6, nil, true, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, cmp.Len(resp.Shrinked, 0))

	te.nodeStats.ShrinkAz("ZW", 2, nil, true, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, cmp.Len(resp.Shrinked, 2))

	infos[0].Op = pb.AdminNodeOp_kOffline
	infos[3].Op = pb.AdminNodeOp_kOffline

	for _, info := range infos {
		node := te.nodeStats.MustGetNodeInfo(info.Id)
		assert.DeepEqual(t, node, info)
	}
	te.checkZkSynced()
}
