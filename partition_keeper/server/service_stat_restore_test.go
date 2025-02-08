package server

import (
	"context"
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd"
	cmd_base "github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/checkpoint"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

func TestRestoreTableWhenCreate(t *testing.T) {
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
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, false, "zk", 32})
	defer te.stop()

	tablePb := &pb.Table{
		TableId:           0,
		TableName:         "test",
		HashMethod:        "crc32",
		PartsCount:        32,
		JsonArgs:          `{"a": "b", "c": "d"}`,
		KconfPath:         "reco.rodisFea.partitionKeeperUnitTest",
		ScheduleGrayscale: kScheduleGrayscaleMax + 1,
	}

	path := "/tmp/colossusdb_restore_test/random_dir"
	status := te.service.AddTable(tablePb, path, "")
	assert.Equal(t, status.Code, int32(pb.AdminError_kOk))

	table := te.service.tableNames["test"]
	assert.Assert(t, table.restoreCtrl != nil)
	assert.Equal(t, table.RestoreVersion, int64(0))
	assert.Equal(t, table.restoreCtrl.stats.RestorePath, path)
	assert.Equal(t, table.restoreCtrl.stats.MaxConcurrentPartsPerNode, int32(-1))
	assert.Equal(t, table.restoreCtrl.stats.MaxConcurrentNodesPerHub, int32(-1))

	logging.Info("first schedule will trigger checkpoint handler to mimic a new execution")
	te.TriggerScheduleAndWaitFinish()
	assert.Equal(t, table.tasks[checkpoint.TASK_NAME].Freezed, true)
	assert.Equal(t, table.RestoreVersion, int64(0))
	assert.DeepEqual(
		t,
		table.tasks[checkpoint.TASK_NAME].execInfo.Args,
		map[string]string{"restore_path": path},
	)

	logging.Info(
		"before checkpoint's command issued, the restore version will stay 0, and no command will send to ps",
	)
	te.TriggerScheduleAndWaitFinish()
	assert.Equal(t, table.RestoreVersion, int64(0))
	for i := range table.currParts {
		assert.Equal(t, table.currParts[i].members.MembershipVersion, int64(0))
	}

	logging.Info(
		"trigger checkpoint to issue command, so as to make the mimicked execution finished",
	)
	table.tasks[checkpoint.TASK_NAME].tryIssueTaskCommand()

	logging.Info("then schedule will trigger table version refreshed")
	te.TriggerScheduleAndCollect()
	assert.Assert(t, table.RestoreVersion > 0)
	data, exists, succ := te.metaStore.Get(context.Background(), table.zkPath)
	assert.Equal(t, succ && exists, true)
	durablePb := &pb.Table{}
	utils.UnmarshalJsonOrDie(data, durablePb)
	assert.Equal(t, durablePb.RestoreVersion, table.RestoreVersion)
	assert.Equal(t, table.restoreCtrl.stats.Stage, RestoreRunning)

	logging.Info("after 5 rounds of scheduling, all replicas are added, but no primary selected")
	for i := 0; i < 5; i++ {
		te.TriggerScheduleAndCollect()
	}
	for i := range table.currParts {
		members := &(table.currParts[i].members)
		pri, secs, learners := members.DivideRoles()
		assert.Equal(t, len(pri), 0)
		assert.Equal(t, len(secs), 3)
		assert.Equal(t, len(learners), 0)
	}

	logging.Info("then schedule will make restore finished")
	te.TriggerScheduleAndWaitFinish()
	assert.Assert(t, table.restoreCtrl == nil)
	assert.Equal(t, table.tasks[checkpoint.TASK_NAME].Freezed, false)

	logging.Info("all replicas are directly added with new version, no remove happen")
	for _, client := range te.lpb.LocalClients {
		assert.Equal(t, len(client.RemovedReplicas()), 0)
		for _, addReplicaReq := range client.KeptReplicas() {
			assert.Equal(t, addReplicaReq.ForRestore, true)
			for _, rep := range addReplicaReq.PeerInfo.Peers {
				assert.Equal(t, rep.RestoreVersion, table.RestoreVersion)
			}
		}
	}
}

func TestRestoreServingTable(t *testing.T) {
	logging.Info("first mock checkpoint handler")
	oldCheckpointHandlerCreator := cmd.UnregisterCmdHandler(checkpoint.TASK_NAME)
	cmd.RegisterCmdHandler(
		checkpoint.TASK_NAME,
		func(params *cmd_base.HandlerParams) cmd_base.CmdHandler {
			return &mockedTaskEffectHandler{
				params: params,
			}
		},
	)
	defer func() {
		cmd.UnregisterCmdHandler(checkpoint.TASK_NAME)
		cmd.RegisterCmdHandler(checkpoint.TASK_NAME, oldCheckpointHandlerCreator)
	}()

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
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, false, "zk", 32})
	defer te.stop()

	tablePb := &pb.Table{
		TableId:           0,
		TableName:         "test",
		HashMethod:        "crc32",
		PartsCount:        32,
		JsonArgs:          `{"a": "b", "c": "d"}`,
		KconfPath:         "reco.rodisFea.partitionKeeperUnitTest",
		ScheduleGrayscale: kScheduleGrayscaleMax + 1,
	}

	status := te.service.AddTable(tablePb, "", "")
	assert.Equal(t, status.Code, int32(pb.AdminError_kOk))

	table := te.service.tableNames["test"]
	assert.Assert(t, table.restoreCtrl == nil)

	te.makeTestTableNormal()
	ps1 := te.lpb.GetClient(nodes[0].Address.String())
	ps2 := te.lpb.GetClient(nodes[1].Address.String())
	ps3 := te.lpb.GetClient(nodes[2].Address.String())

	ps1.CleanMetrics()
	ps2.CleanMetrics()
	ps3.CleanMetrics()

	table = te.service.tableNames["test"]
	logging.Info("start a restore")
	restoreReq := &pb.RestoreTableRequest{
		ServiceName: "test",
		TableName:   "test",
		RestorePath: "/tmp/colossusdb_test/table_12345/111_222",
		Opts: &pb.RestoreOpts{
			MaxConcurrentNodesPerHub:  -1,
			MaxConcurrentPartsPerNode: -1,
		},
	}
	ans := te.service.RestoreTable(restoreReq)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, table.restoreCtrl != nil)

	logging.Info("first schedule will trigger checkpoint handler to mimic a new execution")
	te.TriggerScheduleAndWaitFinish()
	assert.Equal(t, table.tasks[checkpoint.TASK_NAME].Freezed, true)
	assert.Equal(t, table.RestoreVersion, int64(0))
	assert.DeepEqual(
		t,
		table.tasks[checkpoint.TASK_NAME].execInfo.Args,
		map[string]string{"restore_path": restoreReq.RestorePath},
	)

	logging.Info(
		"trigger checkpoint to issue command, so as to make the mimicked execution finished",
	)
	table.tasks[checkpoint.TASK_NAME].tryIssueTaskCommand()

	logging.Info("then schedule will trigger table version refreshed")
	te.TriggerScheduleAndCollect()
	assert.Assert(t, table.RestoreVersion > 0)
	data, exists, succ := te.metaStore.Get(context.Background(), table.zkPath)
	assert.Equal(t, succ && exists, true)
	durablePb := &pb.Table{}
	utils.UnmarshalJsonOrDie(data, durablePb)
	assert.Equal(t, durablePb.RestoreVersion, table.RestoreVersion)
	assert.Equal(t, table.restoreCtrl.stats.Stage, RestoreRunning)

	logging.Info("primary will be downgraded by this schedule")
	for i := range table.currParts {
		p := &(table.currParts[i])
		pri, _, _ := p.members.DivideRoles()
		assert.Equal(t, len(pri), 0)
	}

	logging.Info("then after 9 round of scheduling, all replicas will be recreated")
	for i := 0; i < 9; i++ {
		te.TriggerScheduleAndCollect()
	}

	for _, client := range te.lpb.LocalClients {
		assert.Equal(t, client.GetMetric("remove_replica"), 32)
		assert.Equal(t, client.GetMetric("add_replica"), 32)
		for _, req := range client.KeptReplicas() {
			for _, peer := range req.PeerInfo.Peers {
				assert.Equal(t, peer.RestoreVersion, table.RestoreVersion)
			}
		}
	}

	logging.Info("then schedule will make restore finished")
	te.TriggerScheduleAndWaitFinish()
	assert.Assert(t, table.restoreCtrl == nil)
	assert.Equal(t, table.tasks[checkpoint.TASK_NAME].Freezed, false)

	logging.Info("then schedule will reassign primary")
	te.TriggerScheduleAndCollect()
	for i := range table.currParts {
		p := &(table.currParts[i])
		pri, _, _ := p.members.DivideRoles()
		assert.Equal(t, len(pri), 1)
	}
}

func TestRestoreCreateNewReplicaBeforeOldDeleted(t *testing.T) {
	logging.Info("first mock checkpoint handler")
	oldCheckpointHandlerCreator := cmd.UnregisterCmdHandler(checkpoint.TASK_NAME)
	cmd.RegisterCmdHandler(
		checkpoint.TASK_NAME,
		func(params *cmd_base.HandlerParams) cmd_base.CmdHandler {
			return &mockedTaskEffectHandler{
				params: params,
			}
		},
	)
	defer func() {
		cmd.UnregisterCmdHandler(checkpoint.TASK_NAME)
		cmd.RegisterCmdHandler(checkpoint.TASK_NAME, oldCheckpointHandlerCreator)
	}()

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
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, false, "zk", 32})
	defer te.stop()

	tablePb := &pb.Table{
		TableId:           0,
		TableName:         "test",
		HashMethod:        "crc32",
		PartsCount:        1,
		JsonArgs:          `{"a": "b", "c": "d"}`,
		KconfPath:         "reco.rodisFea.partitionKeeperUnitTest",
		ScheduleGrayscale: kScheduleGrayscaleMax + 1,
	}

	status := te.service.AddTable(tablePb, "", "")
	assert.Equal(t, status.Code, int32(pb.AdminError_kOk))
	p := table_model.PartitionMembership{
		MembershipVersion: 2,
		Peers: map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kSecondary,
		},
	}
	te.initGivenMembership(&p)

	table := te.service.tableNames["test"]
	ps := te.lpb.GetClient(nodes[0].Address.String())

	logging.Info("start a restore, node1 at part 0 will be removed")
	restoreReq := &pb.RestoreTableRequest{
		ServiceName: "test",
		TableName:   "test",
		RestorePath: "/tmp/colossusdb_test/table_12345/111_222",
		Opts: &pb.RestoreOpts{
			MaxConcurrentNodesPerHub:  -1,
			MaxConcurrentPartsPerNode: -1,
		},
	}
	ans := te.service.RestoreTable(restoreReq)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, table.restoreCtrl != nil)

	te.TriggerScheduleAndWaitFinish()
	assert.Equal(t, table.RestoreVersion, int64(0))
	table.tasks[checkpoint.TASK_NAME].tryIssueTaskCommand()
	te.TriggerScheduleAndWaitFinish()

	assert.Equal(t, len(table.currParts[0].members.Peers), 0)
	assert.Equal(t, len(table.currParts[0].facts), 0)

	logging.Info("then collect will try to remove node on ps, but in vein")
	ps.SetReqBehaviors("remove_replica", &rpc.RpcMockBehaviors{
		Delay:   0,
		RpcFail: true,
	})
	te.TriggerCollectAndWaitFinish()
	ps.SetReqBehaviors("remove_replica", nil)
	replicas, err := ps.GetReplicas(context.Background(), &pb.GetReplicasRequest{})
	assert.NilError(t, err)
	assert.Equal(t, len(replicas.Infos), 1)
	assert.Equal(t, replicas.Infos[0].PartitionId, int32(0))
	assert.Equal(t, replicas.Infos[0].PeerInfo.Peers[0].RestoreVersion, int64(0))

	logging.Info(
		"next round of schedule will add replica with new restore version, but add replica req to ps will be rejected",
	)
	ps.CleanMetrics()
	te.TriggerScheduleAndWaitFinish()
	assert.Equal(t, len(table.currParts[0].members.Peers), 1)
	assert.Equal(t, table.currParts[0].members.MembershipVersion, int64(4))
	assert.Equal(t, table.currParts[0].members.RestoreVersion["node1"], table.RestoreVersion)
	assert.Equal(t, ps.GetMetric("add_replica"), 1)
	assert.Equal(t, ps.DuplicateReq, true)
	replicas, err = ps.GetReplicas(context.Background(), &pb.GetReplicasRequest{})
	assert.NilError(t, err)
	assert.Equal(t, len(replicas.Infos), 1)
	assert.Equal(t, replicas.Infos[0].PartitionId, int32(0))
	assert.Equal(t, replicas.Infos[0].PeerInfo.Peers[0].RestoreVersion, int64(0))

	logging.Info("then collecting will detect ps's restore version smaller, then remove it")
	ps.CleanMetrics()
	te.TriggerCollectAndWaitFinish()
	assert.Equal(t, ps.GetMetric("remove_replica"), 1)
	replicas, err = ps.GetReplicas(context.Background(), &pb.GetReplicasRequest{})
	assert.NilError(t, err)
	assert.Equal(t, len(replicas.Infos), 0)

	logging.Info("then schedule will add replica with new restore version")
	ps.CleanMetrics()
	te.TriggerScheduleAndWaitFinish()
	replicas, err = ps.GetReplicas(context.Background(), &pb.GetReplicasRequest{})
	assert.NilError(t, err)
	assert.Equal(t, len(replicas.Infos), 1)
	assert.Equal(t, replicas.Infos[0].PartitionId, int32(0))
	assert.Equal(t, replicas.Infos[0].PeerInfo.Peers[0].RestoreVersion, table.RestoreVersion)
}
