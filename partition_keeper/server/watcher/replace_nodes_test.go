package watcher

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
	"gotest.tools/assert/cmp"
)

func TestReplaceNodesWatcher(t *testing.T) {
	hubs := []*pb.ReplicaHub{
		{Name: "YZ_0", Az: "YZ"},
		{Name: "YZ_1", Az: "YZ"},
	}
	te := setupNodeWatcherTestEnv(t, hubs, []int{2, 2})
	defer te.teardown()

	req := &pb.ReplaceNodesRequest{
		ServiceName: "",
		SrcNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.0", Port: 0},
			{NodeName: "127.0.0.1", Port: 0},
		},
		DstNodes: []*pb.RpcNode{
			{NodeName: "128.0.0.0", Port: 0},
			{NodeName: "128.0.0.1", Port: 0},
		},
	}

	watcher := NewWatcher(ReplaceNodesWatcherName)
	watcher.UpdateParameters(req)

	data := watcher.Serialize()
	req2 := &pb.ReplaceNodesRequest{}
	utils.UnmarshalJsonOrDie(data, req2)
	assert.Assert(t, proto.Equal(req, req2))

	watcher.Deserialize(data)
	data2 := watcher.Serialize()
	assert.DeepEqual(t, data, data2)

	watcher2 := watcher.Clone()
	watcher2.UpdateParameters(&pb.ReplaceNodesRequest{ServiceName: "xx"})
	assert.Assert(t, proto.Equal(req, watcher.GetParameters().(*pb.ReplaceNodesRequest)))

	ops, flag := watcher.StatSatisfied("test", te.nodeStats)
	assert.Assert(t, cmp.Len(ops, 0))
	assert.Equal(t, flag, false)

	hints := map[string]*pb.NodeHints{
		"128.0.0.0:0": {Hub: "YZ_0"},
		"128.0.0.1:0": {Hub: "YZ_1"},
	}
	pings := map[string]*node_mgr.NodePing{
		"new_node_0_0": {
			IsAlive:   true,
			Az:        "YZ",
			Address:   &utils.RpcNode{NodeName: "128.0.0.0", Port: 0},
			ProcessId: "1",
			BizPort:   1000,
		},
		"new_node_1_0": {
			IsAlive:   true,
			Az:        "YZ",
			Address:   &utils.RpcNode{NodeName: "128.0.0.1", Port: 0},
			ProcessId: "1",
			BizPort:   1000,
		},
	}

	te.lock.LockWrite()
	err := te.nodeStats.AddHints(hints, true)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))

	te.nodeStats.UpdateStats(pings)
	te.lock.UnlockWrite()

	ops, flag = watcher.StatSatisfied("test", te.nodeStats)
	assert.Equal(t, flag, false)

	expectOps := map[pb.AdminNodeOp][]*utils.RpcNode{
		pb.AdminNodeOp_kOffline: {
			{NodeName: "127.0.0.0", Port: 0},
			{NodeName: "127.0.0.1", Port: 0},
		},
	}
	assert.DeepEqual(t, ops, expectOps)

	te.lock.LockWrite()
	result := te.nodeStats.AdminNodes(
		ops[pb.AdminNodeOp_kOffline],
		true,
		pb.AdminNodeOp_kOffline,
		nil,
	)
	te.lock.UnlockWrite()
	assert.Assert(t, cmp.Len(result, 2))
	for _, res := range result {
		assert.Equal(t, res.Code, int32(pb.AdminError_kOk))
	}

	ops, flag = watcher.StatSatisfied("test", te.nodeStats)
	assert.Assert(t, cmp.Len(ops, 0))
	assert.Assert(t, flag)
}
