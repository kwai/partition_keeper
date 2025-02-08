package server

import (
	"context"
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/watcher"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

func TestNodeWatcher(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{false, true, "zk", 32})
	te.service.nodes.WithSkipHintCheck(true)
	defer te.stop()

	tbl := te.service.tableNames["test"]
	te.TriggerDetectAndWaitFinish()
	te.service.lock.LockRead()
	assert.Equal(t, len(te.service.nodes.AllNodes()), 2)
	te.service.lock.UnlockRead()

	resizeReq := &pb.ExpandAzsRequest{
		ServiceName: "",
		AzOptions: []*pb.ExpandAzsRequest_AzOption{
			{Az: "YZ1", NewSize: 2},
		},
	}
	err := te.service.ExpandAzs(resizeReq)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(te.service.watchers), 1)

	replaceReq := &pb.ReplaceNodesRequest{
		ServiceName: "",
		SrcNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.1", Port: 1002},
		},
		DstNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.1", Port: 1004},
		},
	}
	err = te.service.ReplaceNodes(replaceReq)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(te.service.watchers), 2)

	logging.Info("open scheduler")
	err = te.service.SwitchSchedulerStatus(true)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))

	logging.Info("node watcher has registered, so no schedule will be trigger")
	te.TriggerScheduleAndWaitFinish()
	for i := 0; i < len(tbl.currParts); i++ {
		assert.Equal(t, tbl.currParts[i].members.MembershipVersion, int64(0))
	}

	logging.Info("then add a node for YZ2, then node2 will be marked offline")
	ping4 := &node_mgr.NodePing{
		IsAlive:   true,
		Az:        "YZ2",
		Address:   utils.FromHostPort("127.0.0.1:1004"),
		ProcessId: "12343",
		BizPort:   2004,
	}
	node_mgr.MockedKessUpdateNodePing(te.mockKess, "node4", ping4)
	te.TriggerDetectAndWaitFinish()
	assert.Equal(t, len(te.service.nodes.AllNodes()), 3)

	te.TriggerScheduleAndWaitFinish()
	for i := 0; i < len(tbl.currParts); i++ {
		assert.Equal(t, tbl.currParts[i].members.MembershipVersion, int64(0))
	}

	queryReq := &pb.QueryNodeInfoRequest{
		NodeName:  "127.0.0.1",
		Port:      1002,
		TableName: "",
		OnlyBrief: false,
		MatchPort: true,
	}
	queryResp := &pb.QueryNodeInfoResponse{}
	te.service.QueryNodeInfo(queryReq, queryResp)
	assert.Equal(t, queryResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryResp.Brief.Op, pb.AdminNodeOp_kOffline)

	logging.Info("then replace watcher will be removed, only hub size watcher block the schedule")
	te.TriggerScheduleAndWaitFinish()
	for i := 0; i < len(tbl.currParts); i++ {
		assert.Equal(t, tbl.currParts[i].members.MembershipVersion, int64(0))
	}
	assert.Equal(t, len(te.service.watchers), 1)

	logging.Info("reload, only 1 watcher left")
	te.service.stop()
	te.service.LoadFromZookeeper()
	assert.Equal(t, len(te.service.watchers), 1)

	reqData := utils.MarshalJsonOrDie(resizeReq)
	assert.DeepEqual(t, te.service.watchers[watcher.AzSizeWatcherName].Serialize(), reqData)
	tbl = te.service.tableNames["test"]

	logging.Info("schedule, still no action")
	te.TriggerScheduleAndWaitFinish()
	for i := 0; i < len(tbl.currParts); i++ {
		assert.Equal(t, tbl.currParts[i].members.MembershipVersion, int64(0))
	}

	logging.Info("add new node, then hub size watcher will be satisfied")
	ping3 := &node_mgr.NodePing{
		IsAlive:   true,
		Az:        "YZ1",
		Address:   utils.FromHostPort("127.0.0.1:1003"),
		ProcessId: "12342",
		BizPort:   2003,
	}
	node_mgr.MockedKessUpdateNodePing(te.mockKess, "node3", ping3)
	te.TriggerDetectAndWaitFinish()
	assert.Equal(t, len(te.service.nodes.AllNodes()), 4)

	te.TriggerScheduleAndWaitFinish()
	for i := 0; i < len(tbl.currParts); i++ {
		assert.Equal(t, tbl.currParts[i].members.MembershipVersion, int64(1), "partition %d", i)
	}
	assert.Equal(t, len(te.service.watchers), 0)
}

func TestExpandAzs(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.3:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node4",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.4:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{false, true, "zk", 32})
	te.service.nodes.WithSkipHintCheck(true)
	defer te.stop()

	te.TriggerDetectAndWaitFinish()
	te.service.lock.LockRead()
	assert.Equal(t, len(te.service.nodes.AllNodes()), 4)
	te.service.lock.UnlockRead()

	req := &pb.ExpandAzsRequest{
		ServiceName: "",
		AzOptions: []*pb.ExpandAzsRequest_AzOption{
			{Az: "YZ3", NewSize: 3},
		},
	}
	result := te.service.ExpandAzs(req)
	assert.Equal(t, result.Code, int32(pb.AdminError_kInvalidParameter))

	req = &pb.ExpandAzsRequest{
		ServiceName: "",
		AzOptions: []*pb.ExpandAzsRequest_AzOption{
			{Az: "YZ1", NewSize: 2},
			{Az: "YZ2", NewSize: 2},
		},
	}
	result = te.service.ExpandAzs(req)
	assert.Equal(t, result.Code, int32(pb.AdminError_kInvalidParameter))

	req = &pb.ExpandAzsRequest{
		ServiceName: "",
		AzOptions: []*pb.ExpandAzsRequest_AzOption{
			{Az: "YZ1", NewSize: 3},
		},
	}
	result = te.service.ExpandAzs(req)
	assert.Equal(t, result.Code, int32(pb.AdminError_kOk))
	hubsizeWatcher := te.service.watchers[watcher.AzSizeWatcherName]

	zkPath := te.service.getWatcherPath(hubsizeWatcher.Name())
	data, exists, succ := te.metaStore.Get(context.Background(), zkPath)
	assert.Assert(t, succ && exists)
	assert.DeepEqual(t, data, hubsizeWatcher.Serialize())
	assert.DeepEqual(t, data, utils.MarshalJsonOrDie(req))

	req = &pb.ExpandAzsRequest{
		ServiceName: "",
		AzOptions: []*pb.ExpandAzsRequest_AzOption{
			{Az: "YZ2", NewSize: 4},
		},
	}
	result = te.service.ExpandAzs(req)
	assert.Equal(t, result.Code, int32(pb.AdminError_kOk))
	hubsizeWatcher = te.service.watchers[watcher.AzSizeWatcherName]
	zkPath = te.service.getWatcherPath(hubsizeWatcher.Name())
	data, exists, succ = te.metaStore.Get(context.Background(), zkPath)
	assert.Assert(t, succ && exists)
	assert.DeepEqual(t, data, hubsizeWatcher.Serialize())
	req2 := &pb.ExpandAzsRequest{}
	utils.UnmarshalJsonOrDie(data, req2)
	assert.DeepEqual(t, req2.GetExpandMap(), map[string]int32{"YZ1": 3, "YZ2": 4})

	req = &pb.ExpandAzsRequest{
		ServiceName: "",
		AzOptions: []*pb.ExpandAzsRequest_AzOption{
			{Az: "YZ1", NewSize: 4},
			{Az: "YZ2", NewSize: 5},
		},
	}
	result = te.service.ExpandAzs(req)
	assert.Equal(t, result.Code, int32(pb.AdminError_kOk))

	data, exists, succ = te.metaStore.Get(context.Background(), zkPath)
	assert.Assert(t, succ && exists)
	assert.DeepEqual(t, data, te.service.watchers[watcher.AzSizeWatcherName].Serialize())
	utils.UnmarshalJsonOrDie(data, req2)
	assert.DeepEqual(t, req2.GetExpandMap(), req.GetExpandMap())

	cancelReq := &pb.CancelExpandAzsRequest{Azs: []string{"AZ80"}}
	result = te.service.CancelExpandAzs(cancelReq)
	assert.Equal(t, result.Code, int32(pb.AdminError_kOk))

	data, exists, succ = te.metaStore.Get(context.Background(), zkPath)
	assert.Assert(t, succ && exists)
	assert.DeepEqual(t, data, te.service.watchers[watcher.AzSizeWatcherName].Serialize())
	utils.UnmarshalJsonOrDie(data, req2)
	assert.DeepEqual(t, req2.GetExpandMap(), map[string]int32{"YZ1": 4, "YZ2": 5})

	cancelReq = &pb.CancelExpandAzsRequest{Azs: []string{"YZ1"}}
	result = te.service.CancelExpandAzs(cancelReq)
	assert.Equal(t, result.Code, int32(pb.AdminError_kOk))

	data, exists, succ = te.metaStore.Get(context.Background(), zkPath)
	assert.Assert(t, succ && exists)
	assert.DeepEqual(t, data, te.service.watchers[watcher.AzSizeWatcherName].Serialize())
	utils.UnmarshalJsonOrDie(data, req2)
	assert.DeepEqual(t, req2.GetExpandMap(), map[string]int32{"YZ2": 5})

	cancelReq = &pb.CancelExpandAzsRequest{Azs: []string{"YZ1"}}
	result = te.service.CancelExpandAzs(cancelReq)
	assert.Equal(t, result.Code, int32(pb.AdminError_kOk))

	data, exists, succ = te.metaStore.Get(context.Background(), zkPath)
	assert.Assert(t, succ && exists)
	assert.DeepEqual(t, data, te.service.watchers[watcher.AzSizeWatcherName].Serialize())
	utils.UnmarshalJsonOrDie(data, req2)
	assert.DeepEqual(t, req2.GetExpandMap(), map[string]int32{"YZ2": 5})

	cancelReq = &pb.CancelExpandAzsRequest{Azs: []string{"YZ2"}}
	result = te.service.CancelExpandAzs(cancelReq)
	assert.Equal(t, result.Code, int32(pb.AdminError_kOk))

	_, exists, succ = te.metaStore.Get(context.Background(), zkPath)
	assert.Assert(t, succ && !exists)
	assert.Assert(t, te.service.cloneWatcher(watcher.AzSizeWatcherName), nil)

	cancelReq = &pb.CancelExpandAzsRequest{Azs: []string{"YZ2"}}
	result = te.service.CancelExpandAzs(cancelReq)
	assert.Equal(t, result.Code, int32(pb.AdminError_kInvalidParameter))
}

func TestReplaceNodes(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{false, true, "zk", 32})
	te.service.nodes.WithSkipHintCheck(true)
	defer te.stop()

	te.TriggerDetectAndWaitFinish()
	te.service.lock.LockRead()
	assert.Equal(t, len(te.service.nodes.AllNodes()), 2)
	te.service.lock.UnlockRead()

	logging.Info("src & dst must match")
	replaceReq := &pb.ReplaceNodesRequest{
		ServiceName: "",
		SrcNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.1", Port: 1001},
		},
		DstNodes: nil,
	}
	result := te.service.ReplaceNodes(replaceReq)
	assert.Equal(t, result.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("src shouldn't duplicate")
	replaceReq = &pb.ReplaceNodesRequest{
		ServiceName: "",
		SrcNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.1", Port: 1001},
			{NodeName: "127.0.0.1", Port: 1001},
		},
		DstNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.3", Port: 1001},
			{NodeName: "127.0.0.4", Port: 1001},
		},
	}
	result = te.service.ReplaceNodes(replaceReq)
	assert.Equal(t, result.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("src must exists")
	replaceReq = &pb.ReplaceNodesRequest{
		ServiceName: "",
		SrcNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.1", Port: 1001},
			{NodeName: "127.0.0.5", Port: 1001},
		},
		DstNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.3", Port: 1001},
			{NodeName: "127.0.0.4", Port: 1001},
		},
	}
	result = te.service.ReplaceNodes(replaceReq)
	assert.Equal(t, result.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("dst shouldn't duplicate")
	replaceReq = &pb.ReplaceNodesRequest{
		ServiceName: "",
		SrcNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.1", Port: 1001},
			{NodeName: "127.0.0.2", Port: 1001},
		},
		DstNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.3", Port: 1001},
			{NodeName: "127.0.0.3", Port: 1001},
		},
	}
	result = te.service.ReplaceNodes(replaceReq)
	assert.Equal(t, result.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("dst shouldn't exists")
	replaceReq = &pb.ReplaceNodesRequest{
		ServiceName: "",
		SrcNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.1", Port: 1001},
			{NodeName: "127.0.0.2", Port: 1001},
		},
		DstNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.2", Port: 1001},
			{NodeName: "127.0.0.1", Port: 1001},
		},
	}
	result = te.service.ReplaceNodes(replaceReq)
	assert.Equal(t, result.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("add ok")
	replaceReq = &pb.ReplaceNodesRequest{
		ServiceName: "",
		SrcNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.1", Port: 1001},
			{NodeName: "127.0.0.2", Port: 1001},
		},
		DstNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.3", Port: 1001},
			{NodeName: "127.0.0.4", Port: 1001},
		},
	}
	result = te.service.ReplaceNodes(replaceReq)
	assert.Equal(t, result.Code, int32(pb.AdminError_kOk))

	replaceWatcher := te.service.watchers[watcher.ReplaceNodesWatcherName]

	zkPath := te.service.getWatcherPath(replaceWatcher.Name())
	data, exists, succ := te.metaStore.Get(context.Background(), zkPath)
	assert.Assert(t, succ && exists)
	assert.DeepEqual(t, data, replaceWatcher.Serialize())
	assert.DeepEqual(t, data, utils.MarshalJsonOrDie(replaceReq))

	logging.Info("request not allowed if already has one")
	result = te.service.ReplaceNodes(replaceReq)
	assert.Equal(t, result.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("remove watcher of replace nodes")
	result = te.service.RemoveWatcher(watcher.ReplaceNodesWatcherName)
	assert.Equal(t, result.Code, int32(pb.AdminError_kOk))
	_, exists, succ = te.metaStore.Get(context.Background(), zkPath)
	assert.Assert(t, succ && !exists)
	_, exists = te.service.watchers[watcher.ReplaceNodesWatcherName]
	assert.Assert(t, !exists)
}
