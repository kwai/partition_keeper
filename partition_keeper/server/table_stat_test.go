package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/delay_execute"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/checkpoint"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/est"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
)

type testTableEnv struct {
	testZkRoot    string
	t             *testing.T
	lock          *utils.LooseLock
	nodeStats     *node_mgr.NodeStats
	hubs          []*pb.ReplicaHub
	lpb           *rpc.LocalPSClientPoolBuilder
	zkStore       metastore.MetaStore
	delayExecutor *delay_execute.DelayedExecutorManager
	table         *TableStats
}

func setupTestTableEnv(
	t *testing.T,
	nodesCount, partsCount, hubsCount int,
	st strategy.ServiceStrategy,
	args string,
	initNewTable bool,
	metaStoreType string,
) *testTableEnv {
	var zkStore metastore.MetaStore
	if metaStoreType == "zk" {
		acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
		zkStore = metastore.CreateZookeeperStore(
			[]string{"127.0.0.1:2181"},
			time.Second*10,
			acl,
			scheme,
			auth,
		)
	} else {
		zkStore = metastore.NewMockMetaStore()
	}
	out := &testTableEnv{
		testZkRoot: utils.SpliceZkRootPath("/test/pk/table_stat"),
		t:          t,
		lpb:        rpc.NewLocalPSClientPoolBuilder(),
		zkStore:    zkStore,
	}

	ans := out.zkStore.RecursiveDelete(context.Background(), out.testZkRoot)
	assert.Assert(t, ans)
	ans = out.zkStore.RecursiveCreate(context.Background(), out.testZkRoot+"/nodes")
	assert.Assert(t, ans)
	ans = out.zkStore.RecursiveCreate(context.Background(), out.testZkRoot+"/hints")
	assert.Assert(t, ans)

	out.lock = utils.NewLooseLock()
	out.hubs = []*pb.ReplicaHub{}
	for i := 0; i < hubsCount; i++ {
		out.hubs = append(out.hubs, &pb.ReplicaHub{
			Name: fmt.Sprintf("yz%d", i),
			Az:   "YZ",
		})
	}

	out.nodeStats = node_mgr.NewNodeStats(
		"test",
		out.lock,
		out.testZkRoot+"/nodes",
		out.testZkRoot+"/hints",
		zkStore,
	)
	out.nodeStats.LoadFromZookeeper(utils.MapHubs(out.hubs), true)

	out.lock.LockWrite()
	for i := 1; i <= nodesCount; i++ {
		id := fmt.Sprintf("node%d", i)
		addr := &utils.RpcNode{NodeName: fmt.Sprintf("127.0.0.%d", i), Port: 1001}
		np := &node_mgr.NodePing{
			IsAlive:   true,
			Az:        "YZ",
			Address:   addr,
			ProcessId: "12300",
			BizPort:   int32(2001),
		}
		out.nodeStats.UpdateStats(map[string]*node_mgr.NodePing{id: np})
	}
	out.nodeStats.RefreshScore(est.NewEstimator(est.DEFAULT_ESTIMATOR), true)
	out.lock.UnlockWrite()
	out.delayExecutor = delay_execute.NewDelayedExecutorManager(
		zkStore,
		out.testZkRoot+"/"+kDelayedExecutorNode,
	)
	out.delayExecutor.InitFromZookeeper()

	out.table = NewTableStats(
		"test",
		"test",
		false,
		out.lock,
		out.nodeStats,
		out.hubs,
		st,
		out.lpb.Build(),
		out.zkStore,
		out.delayExecutor,
		out.testZkRoot,
		nil,
	)
	if initNewTable {
		tableDesc := &pb.Table{
			TableId:                    1,
			TableName:                  "test",
			HashMethod:                 "crc32",
			PartsCount:                 int32(partsCount),
			JsonArgs:                   args,
			KconfPath:                  "reco.rodisFea.partitionKeeperHDFSTest",
			RecoverPartitionsFromRoute: false,
			ScheduleGrayscale:          kScheduleGrayscaleMax + 1,
		}
		out.table.InitializeNew(tableDesc, "")
	}
	return out
}

func (te *testTableEnv) teardown() {
	te.table.Stop(true)
	te.table.delayedExecutorManager.Stop()
	ans := te.zkStore.RecursiveDelete(
		context.Background(),
		te.testZkRoot,
	)
	assert.Assert(te.t, ans)
	te.zkStore.Close()
}

func (te *testTableEnv) updateFactsFromMembers() {
	for i := int32(0); i < te.table.PartsCount; i++ {
		part := &(te.table.currParts[i])
		for node, role := range part.members.Peers {
			part.facts[node] = &replicaFact{
				membershipVersion: part.members.MembershipVersion,
				partSplitVersion:  part.members.SplitVersion,
				PartitionReplica: &pb.PartitionReplica{
					Role:                role,
					ReadyToPromote:      true,
					RestoreVersion:      part.members.RestoreVersion[node],
					SplitCleanupVersion: part.members.SplitVersion,
				},
			}
		}
	}
}

func (te *testTableEnv) applyPlanAndUpdateFacts(plan sched.SchedulePlan) {
	te.table.AddPlan(plan)
	for ans := te.table.scheduleAsPlan(); len(ans) > 0; ans = te.table.scheduleAsPlan() {
		te.updateFactsFromMembers()
	}
}

func testZkMembershipEqual(te *testTableEnv, zkPath string, m *table_model.PartitionMembership) {
	data, exists, _ := te.zkStore.Get(context.Background(), zkPath)
	assert.Equal(te.t, exists, true)

	gotMembers := table_model.PartitionMembership{}
	err := json.Unmarshal(data, &gotMembers)
	assert.NilError(te.t, err)

	assert.DeepEqual(te.t, &gotMembers, m)
}

func testNodeReplicasSameWithMembers(te *testTableEnv, nodeId string, np *node_mgr.NodeInfo) {
	client := te.lpb.GetClient(np.Address.String())
	assert.Assert(te.t, client != nil)

	req := &pb.GetReplicasRequest{}
	replicas, err := client.GetReplicas(context.Background(), req)
	assert.NilError(te.t, err)

	nr := recorder.NewAllNodesRecorder(recorder.NewBriefNode)
	te.table.RecordNodesReplicas(nr, false)

	assert.Equal(te.t, len(replicas.Infos), nr.GetNode(nodeId).CountAll())
	for _, info := range replicas.Infos {
		expectPart := &(te.table.currParts[info.PartitionId])
		assert.Equal(te.t, expectPart.members.MembershipVersion, info.PeerInfo.MembershipVersion)
		selfPeer := info.PeerInfo.FindPeer(np.Address.ToPb())
		assert.Assert(te.t, selfPeer != nil)
		assert.Equal(te.t, selfPeer.Role, expectPart.members.Peers[nodeId])
	}
}

func TestShadedSchedulerProperSet(t *testing.T) {
	te := setupTestTableEnv(
		t,
		5,
		43,
		2,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	assert.Equal(t, te.table.scheduler.Name(), sched.ADAPTIVE_SCHEDULER)
	shadedTable := NewTableStats(
		"test",
		"test",
		false,
		te.lock,
		te.nodeStats,
		te.hubs,
		te.table.svcStrategy,
		te.lpb.Build(),
		te.zkStore,
		te.delayExecutor,
		te.testZkRoot,
		te.table,
	)
	tableDesc := &pb.Table{
		TableId:                    2,
		TableName:                  "test_shaded",
		HashMethod:                 "crc32",
		PartsCount:                 int32(te.table.PartsCount),
		JsonArgs:                   "",
		KconfPath:                  "reco.rodisFea.partitionKeeperHDFSTest",
		RecoverPartitionsFromRoute: false,
		ScheduleGrayscale:          kScheduleGrayscaleMax + 1,
	}
	shadedTable.InitializeNew(tableDesc, "")
	defer shadedTable.Stop(true)

	assert.Equal(t, shadedTable.scheduler.Name(), sched.SHADED_SCHEDUELR)
	assert.DeepEqual(t, te.table.GetEstimatedReplicas(), shadedTable.GetEstimatedReplicas())
}

func TestHashGroupArgsProperSet(t *testing.T) {
	te := setupTestTableEnv(
		t,
		4,
		8,
		2,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		`{"msg_queue_shards": 4}`,
		true,
		"zk",
	)
	defer te.teardown()

	assert.Equal(t, te.table.scheduler.Name(), sched.HASH_GROUP_SCHEDULER)
	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()
	te.table.Schedule(true, &pb.ScheduleOptions{
		EnablePrimaryScheduler: true,
		MaxSchedRatio:          sched.SCHEDULE_RATIO_MAX_VALUE,
	})
	for i := 0; i < int(te.table.PartsCount); i++ {
		member := te.table.GetMembership(int32(i))
		assert.Equal(t, len(member.Peers), 1)
		for node := range member.Peers {
			assert.Equal(t, te.table.nodes.MustGetNodeInfo(node).Hub, "yz0")
		}
	}
}

func TestExecPartitionUpdate(t *testing.T) {
	te := setupTestTableEnv(
		t,
		3,
		8,
		3,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	plans := map[int32]*actions.PartitionActions{}
	for i := 0; i < 8; i++ {
		ans := &actions.PartitionActions{}
		ans.AddAction(&actions.AddLearnerAction{Node: fmt.Sprintf("node%d", (i%3)+1)})
		plans[int32(i)] = ans
	}

	te.table.serviceLock.LockWrite()
	te.table.ApplyPlan(plans)
	te.table.serviceLock.UnlockWrite()

	// first check the local state is changed
	for i := range te.table.currParts {
		p := &(te.table.currParts[i])
		assert.Equal(t, p.hasZkPath, true)
		assert.Equal(t, p.membershipChanged, false)
		assert.Equal(t, p.members.MembershipVersion, int64(1))
	}
	// then check zookeeper are updated
	for i := range te.table.currParts {
		p := &(te.table.currParts[i])
		testZkMembershipEqual(te, te.table.getPartitionPath(int32(i)), &p.members)
	}

	// then check nodes are notified
	for id, ping := range te.table.nodes.AllNodes() {
		testNodeReplicasSameWithMembers(te, id, ping)
	}
}

func TestExecPartitionCustomRunner(t *testing.T) {
	te := setupTestTableEnv(
		t,
		3,
		4,
		2,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_clotho),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	plans := map[int32]*actions.PartitionActions{}
	for i := 0; i < 4; i++ {
		ans := &actions.PartitionActions{}
		ans.AddAction(&actions.AddLearnerAction{Node: "node1"})
		ans.AddAction(&actions.TransformAction{Node: "node1", ToRole: pb.ReplicaRole_kSecondary})
		ans.AddAction(&actions.TransformAction{Node: "node1", ToRole: pb.ReplicaRole_kPrimary})

		ans.AddAction(&actions.AddLearnerAction{Node: "node2"})
		ans.AddAction(&actions.TransformAction{Node: "node2", ToRole: pb.ReplicaRole_kSecondary})
		plans[int32(i)] = ans
	}

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	te.table.ApplyPlan(plans)
	for ans := te.table.scheduleAsPlan(); len(ans) > 0; ans = te.table.scheduleAsPlan() {
		logging.Info("still has plan to apply")
		time.Sleep(time.Second * 2)
	}

	assert.Equal(t, len(te.table.currParts[0].members.Peers), 2)
	assert.Equal(t, te.table.currParts[0].members.Peers["node1"], pb.ReplicaRole_kPrimary)
	assert.Equal(t, te.table.currParts[0].members.Peers["node2"], pb.ReplicaRole_kSecondary)

	p := &(te.table.currParts[0])
	p.facts["node1"] = &replicaFact{
		membershipVersion: p.members.MembershipVersion,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kPrimary,
			ReadyToPromote: true,
			StatisticsInfo: make(map[string]string),
		},
	}
	p.facts["node2"] = &replicaFact{
		membershipVersion: p.members.MembershipVersion,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kSecondary,
			ReadyToPromote: true,
			StatisticsInfo: make(map[string]string),
		},
	}

	acts := &actions.PartitionActions{}
	acts.AddAction(&actions.AtomicSwitchPrimaryAction{FromNode: "node1", ToNode: "node2"})
	plans[0] = acts
	te.table.ApplyPlan(plans)
	p.facts["node1"].StatisticsInfo[pb.StdReplicaStat_name[int32(pb.StdReplicaStat_stream_load_paused)]] = "true"
	p.facts["node1"].StatisticsInfo[pb.StdReplicaStat_name[int32(pb.StdReplicaStat_stream_load_offset)]] = "1"
	p.facts["node2"].StatisticsInfo[pb.StdReplicaStat_name[int32(pb.StdReplicaStat_stream_load_paused)]] = "true"
	p.facts["node2"].StatisticsInfo[pb.StdReplicaStat_name[int32(pb.StdReplicaStat_stream_load_offset)]] = "1"

	for ans := te.table.scheduleAsPlan(); len(ans) > 0; ans = te.table.scheduleAsPlan() {
		logging.Info("still has plan to apply")
	}

	assert.Equal(t, len(te.table.currParts[0].members.Peers), 2)
	assert.Equal(t, te.table.currParts[0].members.Peers["node1"], pb.ReplicaRole_kSecondary)
	assert.Equal(t, te.table.currParts[0].members.Peers["node2"], pb.ReplicaRole_kPrimary)
}

func TestSchedulePlanAndPassiveTransformBothWork(t *testing.T) {
	te := setupTestTableEnv(
		t,
		2,
		2,
		1,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	plan := sched.SchedulePlan{}
	plan[0] = actions.MakeActions(&actions.AddLearnerAction{Node: "node1"})
	plan[1] = actions.MakeActions(&actions.AddLearnerAction{Node: "node2"})
	te.applyPlanAndUpdateFacts(plan)

	plan = sched.SchedulePlan{}
	plan[0] = actions.MakeActions(&actions.PromoteToSecondaryAction{Node: "node1"})
	te.table.currParts[0].facts["node1"].ReadyToPromote = false
	te.table.AddPlan(plan)

	ans := te.table.CheckPartitions()
	assert.Equal(t, ans.HasAction, true)
	assert.Equal(t, ans.Normal, false)
	assert.Equal(t, te.table.currParts[0].members.GetMember("node1"), pb.ReplicaRole_kLearner)
	assert.Equal(t, te.table.currParts[1].members.GetMember("node2"), pb.ReplicaRole_kSecondary)
	assert.Equal(t, len(te.table.plans), 1)

	te.table.currParts[0].facts["node1"].ReadyToPromote = true
	ans = te.table.CheckPartitions()
	assert.Equal(t, ans.HasAction, true)
	assert.Equal(t, ans.Normal, false)
	assert.Equal(t, te.table.currParts[0].members.GetMember("node1"), pb.ReplicaRole_kSecondary)

	ans = te.table.CheckPartitions()
	assert.Equal(t, ans.HasAction, false)
	assert.Equal(t, ans.Normal, true)
}

func TestPlannedLearnersNotPromotable(t *testing.T) {
	te := setupTestTableEnv(
		t,
		3,
		8,
		3,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	plans := map[int32]*actions.PartitionActions{}
	acts := &actions.PartitionActions{}
	acts.AddAction(&actions.AddLearnerAction{Node: "node1"})
	acts.AddAction(&actions.PromoteToSecondaryAction{Node: "node1"})
	plans[0] = acts

	acts = &actions.PartitionActions{}
	acts.AddAction(&actions.AddLearnerAction{Node: "node2"})
	acts.AddAction(&actions.PromoteToSecondaryAction{Node: "node2"})
	plans[1] = acts

	te.table.serviceLock.LockWrite()
	te.table.ApplyPlan(plans)
	te.table.serviceLock.UnlockWrite()

	logging.Info("node1 -> learner@p1, node2 -> learner@p2")
	assert.Equal(t, te.table.currParts[0].members.MembershipVersion, int64(1))
	assert.Equal(t, te.table.currParts[0].members.GetMember("node1"), pb.ReplicaRole_kLearner)
	assert.Assert(t, te.table.plans[0].HasAction())

	assert.Equal(t, te.table.currParts[1].members.MembershipVersion, int64(1))
	assert.Equal(t, te.table.currParts[1].members.GetMember("node2"), pb.ReplicaRole_kLearner)
	assert.Assert(t, te.table.plans[1].HasAction())

	logging.Info("node1 not promotable, node2 haven't collect info")
	te.table.currParts[0].facts["node1"] = &replicaFact{
		membershipVersion: 1,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kLearner,
			ReadyToPromote: false,
		},
	}

	te.table.serviceLock.LockWrite()
	checkAns := te.table.CheckPartitions()
	te.table.serviceLock.UnlockWrite()

	assert.Equal(t, checkAns.HasAction, true)
	assert.Equal(t, te.table.currParts[0].members.MembershipVersion, int64(1))
	assert.Equal(t, te.table.currParts[0].members.GetMember("node1"), pb.ReplicaRole_kLearner)
	assert.Assert(t, te.table.plans[0].HasAction())

	assert.Equal(t, te.table.currParts[1].members.MembershipVersion, int64(1))
	assert.Equal(t, te.table.currParts[1].members.GetMember("node2"), pb.ReplicaRole_kLearner)
	assert.Assert(t, te.table.plans[1].HasAction())

	logging.Info("node1 promotable, node2 can't promote")
	te.table.currParts[0].facts["node1"] = &replicaFact{
		membershipVersion: 1,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kLearner,
			ReadyToPromote: true,
		},
	}
	te.table.currParts[1].facts["node2"] = &replicaFact{
		membershipVersion: 1,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kLearner,
			ReadyToPromote: false,
		},
	}

	te.table.serviceLock.LockWrite()
	checkAns = te.table.CheckPartitions()
	te.table.serviceLock.UnlockWrite()

	assert.Equal(t, checkAns.Normal, false)
	assert.Equal(t, te.table.currParts[0].members.MembershipVersion, int64(2))
	assert.Equal(t, te.table.currParts[0].members.GetMember("node1"), pb.ReplicaRole_kSecondary)
	assert.Assert(t, !te.table.plans[0].HasAction())

	assert.Equal(t, te.table.currParts[1].members.MembershipVersion, int64(1))
	assert.Equal(t, te.table.currParts[1].members.GetMember("node2"), pb.ReplicaRole_kLearner)
	assert.Assert(t, te.table.plans[1].HasAction())

	logging.Info("node2 promotable too")
	te.table.currParts[1].facts["node2"] = &replicaFact{
		membershipVersion: 1,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kLearner,
			ReadyToPromote: true,
		},
	}

	te.table.serviceLock.LockWrite()
	checkAns = te.table.CheckPartitions()
	te.table.serviceLock.UnlockWrite()

	assert.Equal(t, checkAns.Normal, false)
	assert.Equal(t, te.table.currParts[0].members.MembershipVersion, int64(2))
	assert.Equal(t, te.table.currParts[0].members.GetMember("node1"), pb.ReplicaRole_kSecondary)
	assert.Assert(t, !te.table.plans[0].HasAction())

	assert.Equal(t, te.table.currParts[1].members.MembershipVersion, int64(2))
	assert.Equal(t, te.table.currParts[1].members.GetMember("node2"), pb.ReplicaRole_kSecondary)
	assert.Assert(t, !te.table.plans[0].HasAction())

	logging.Info("no plan to execute")
	te.table.serviceLock.LockWrite()
	ans2 := te.table.scheduleAsPlan()
	te.table.serviceLock.UnlockWrite()
	assert.Assert(t, len(ans2) == 0)
	assert.Assert(t, te.table.plans == nil)
}

func TestUpdateTableTasks(t *testing.T) {
	te := setupTestTableEnv(
		t,
		6,
		1,
		3,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	taskDesc := &pb.PeriodicTask{
		TaskName:                "invalid_task",
		FirstTriggerUnixSeconds: 1,
		PeriodSeconds:           0,
	}

	err := te.table.UpdateTask(taskDesc)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Assert(t, strings.Contains(err.Message, "can't find task"))

	taskDesc.TaskName = checkpoint.TASK_NAME
	err = te.table.UpdateTask(taskDesc)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Assert(t, !strings.Contains(err.Message, "can't find task"))
}

func TestManualRemoveReplicas(t *testing.T) {
	te := setupTestTableEnv(
		t,
		3,
		1,
		3,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	acts := &actions.PartitionActions{}
	acts.AddAction(&actions.AddLearnerAction{Node: "node1"})
	acts.AddAction(&actions.AddLearnerAction{Node: "node2"})
	acts.AddAction(&actions.AddLearnerAction{Node: "node3"})

	plans := map[int32]*actions.PartitionActions{}
	plans[0] = acts

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	te.table.ApplyPlan(plans)
	for ans := te.table.scheduleAsPlan(); len(ans) > 0; ans = te.table.scheduleAsPlan() {
		logging.Info("still has plan to apply")
	}

	manualRemoveReplicasReq := &pb.ManualRemoveReplicasRequest{
		ServiceName: "test",
		TableName:   "test",
		Replicas: []*pb.ManualRemoveReplicasRequest_ReplicaItem{
			{
				Node:        &pb.RpcNode{NodeName: "127.0.0.1", Port: 1001},
				PartitionId: 0,
			},
			{
				Node:        &pb.RpcNode{NodeName: "127.0.0.2", Port: 1001},
				PartitionId: 10,
			},
			{
				Node:        &pb.RpcNode{NodeName: "127.0.0.4", Port: 1001},
				PartitionId: 10,
			},
		},
	}

	resp := &pb.ManualRemoveReplicasResponse{}
	te.table.RemoveReplicas(manualRemoveReplicasReq, resp)
	assert.Equal(t, len(resp.ReplicasResult), 3)
	assert.Equal(t, resp.ReplicasResult[0].Code, int32(pb.AdminError_kOk))
	assert.Equal(t, resp.ReplicasResult[1].Code, int32(pb.AdminError_kInvalidParameter))
	assert.Equal(t, resp.ReplicasResult[2].Code, int32(pb.AdminError_kInvalidParameter))

	assert.Equal(t, te.table.currParts[0].members.HasMember("node1"), false)
	assert.Equal(t, te.table.currParts[0].members.HasMember("node2"), true)
	assert.Equal(t, te.table.currParts[0].members.HasMember("node3"), true)
}

func TestRemoveOfflinedUnlessLearnerPromote(t *testing.T) {
	te := setupTestTableEnv(
		t,
		6,
		1,
		3,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	plans := map[int32]*actions.PartitionActions{}

	acts := &actions.PartitionActions{}
	acts.AddAction(&actions.AddLearnerAction{Node: "node1"})
	acts.AddAction(&actions.TransformAction{Node: "node1", ToRole: pb.ReplicaRole_kSecondary})
	acts.AddAction(&actions.TransformAction{Node: "node1", ToRole: pb.ReplicaRole_kPrimary})

	acts.AddAction(&actions.AddLearnerAction{Node: "node2"})
	acts.AddAction(&actions.TransformAction{Node: "node2", ToRole: pb.ReplicaRole_kSecondary})

	acts.AddAction(&actions.AddLearnerAction{Node: "node3"})
	acts.AddAction(&actions.TransformAction{Node: "node3", ToRole: pb.ReplicaRole_kSecondary})

	plans[0] = acts

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	te.table.ApplyPlan(plans)
	for ans := te.table.scheduleAsPlan(); len(ans) > 0; ans = te.table.scheduleAsPlan() {
		logging.Info("still has plan to apply")
	}

	ans := te.table.nodes.AdminNodes(
		utils.FromHostPorts([]string{"127.0.0.1:1001"}),
		true,
		pb.AdminNodeOp_kOffline,
		nil,
	)
	assert.Equal(t, len(ans), 1)
	assert.Equal(t, ans[0].Code, int32(pb.AdminError_kOk))

	scheduler := sched.NewScheduler(sched.ADAPTIVE_SCHEDULER, "test-service")
	scheduleInput := &sched.ScheduleInput{
		Table: te.table,
		Nodes: te.table.nodes,
		Hubs:  te.table.hubs,
		Opts: sched.ScheduleCtrlOptions{
			Configs:          &pb.ScheduleOptions{EnablePrimaryScheduler: true, MaxSchedRatio: 100},
			PrepareRestoring: false,
		},
	}
	plans = scheduler.Schedule(scheduleInput)
	te.table.AddPlan(plans)

	p := &(te.table.currParts[0])

	// first will add learner
	version := p.members.MembershipVersion + 1
	normal := te.table.CheckPartitions()
	assert.Equal(t, normal.Normal, false)
	assert.Equal(t, len(te.table.currParts[0].members.Peers), 4)
	assert.Equal(t, p.members.MembershipVersion, version)

	// added learner not promotable
	normal = te.table.CheckPartitions()
	assert.Equal(t, normal.Normal, false)
	assert.Equal(t, len(te.table.currParts[0].members.Peers), 4)
	assert.Equal(t, p.members.MembershipVersion, version)

	p.facts["node4"] = &replicaFact{
		membershipVersion: p.members.MembershipVersion,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kLearner,
			ReadyToPromote: true,
		},
	}

	// node4 to secondary
	version++
	normal = te.table.CheckPartitions()
	assert.Equal(t, normal.Normal, false)
	assert.Equal(t, len(te.table.currParts[0].members.Peers), 4)
	assert.Equal(t, p.members.MembershipVersion, version)
	p.facts["node4"].Role = pb.ReplicaRole_kSecondary

	// node1 to secondary and node4 to primary
	p.facts["node1"] = &replicaFact{
		membershipVersion: p.members.MembershipVersion,
		PartitionReplica: &pb.PartitionReplica{
			Role: pb.ReplicaRole_kSecondary,
		},
	}
	version++
	version++
	normal = te.table.CheckPartitions()
	assert.Equal(t, normal.Normal, false)
	assert.Equal(t, len(te.table.currParts[0].members.Peers), 4)
	assert.Equal(t, p.members.MembershipVersion, version)
	p.facts["node4"].Role = pb.ReplicaRole_kPrimary

	// node1 to learner
	version++
	normal = te.table.CheckPartitions()
	assert.Equal(t, normal.Normal, false)
	assert.Equal(t, len(te.table.currParts[0].members.Peers), 4)
	assert.Equal(t, p.members.MembershipVersion, version)
	p.facts["node1"].Role = pb.ReplicaRole_kLearner

	// node 1 removed
	version++
	normal = te.table.CheckPartitions()
	assert.Equal(t, normal.Normal, false)
	assert.Equal(t, len(te.table.currParts[0].members.Peers), 3)
	assert.Equal(t, p.members.MembershipVersion, version)
}

func TestTableOccupiedResource(t *testing.T) {
	args := `
	{
		"btq_prefix": "mio_embedding",
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_num_keys": 10995116277760,
		"table_size_bytes": 512,
		"table_inflation_ratio": 1.5
	}`
	te := setupTestTableEnv(
		t,
		3,
		1,
		3,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_embedding_server),
		args,
		true,
		"zk",
	)
	defer te.teardown()

	plans := map[int32]*actions.PartitionActions{}

	acts := &actions.PartitionActions{}
	acts.AddAction(&actions.AddLearnerAction{Node: "node1"})
	acts.AddAction(&actions.TransformAction{Node: "node1", ToRole: pb.ReplicaRole_kSecondary})
	acts.AddAction(&actions.TransformAction{Node: "node1", ToRole: pb.ReplicaRole_kPrimary})

	acts.AddAction(&actions.AddLearnerAction{Node: "node2"})
	acts.AddAction(&actions.TransformAction{Node: "node2", ToRole: pb.ReplicaRole_kSecondary})

	acts.AddAction(&actions.AddLearnerAction{Node: "node3"})
	acts.AddAction(&actions.TransformAction{Node: "node3", ToRole: pb.ReplicaRole_kSecondary})

	plans[0] = acts

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	te.table.ApplyPlan(plans)
	for ans := te.table.scheduleAsPlan(); len(ans) > 0; ans = te.table.scheduleAsPlan() {
		logging.Info("still has plan to apply")
	}

	logging.Info("table resource will use create table args if no replicas fact collected")
	res := te.table.GetOccupiedResource()
	assert.DeepEqual(t, res, utils.HardwareUnit{utils.MEM_CAP: 512})

	p := &(te.table.currParts[0])

	logging.Info("only updated replica's info will be taken into account")
	p.facts["node1"] = &replicaFact{
		membershipVersion: p.members.MembershipVersion - 1,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kSecondary,
			ReadyToPromote: true,
			StatisticsInfo: map[string]string{
				"db_storage_limit_bytes": "800",
			},
		},
	}
	p.facts["node2"] = &replicaFact{
		membershipVersion: p.members.MembershipVersion,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kLearner,
			ReadyToPromote: true,
			StatisticsInfo: map[string]string{
				"db_storage_limit_bytes": "800",
			},
		},
	}
	p.facts["node3"] = &replicaFact{
		membershipVersion: p.members.MembershipVersion,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kSecondary,
			ReadyToPromote: true,
			StatisticsInfo: map[string]string{
				"db_storage_limit_bytes": "600",
			},
		},
	}
	res = te.table.GetOccupiedResource()
	assert.DeepEqual(t, res, utils.HardwareUnit{utils.MEM_CAP: 600})

	logging.Info("more than 1 replicas will be averaged")
	p.facts["node2"] = &replicaFact{
		membershipVersion: p.members.MembershipVersion,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kSecondary,
			ReadyToPromote: true,
			StatisticsInfo: map[string]string{
				"db_storage_limit_bytes": "800",
			},
		},
	}
	res = te.table.GetOccupiedResource()
	assert.DeepEqual(t, res, utils.HardwareUnit{utils.MEM_CAP: 700})
}

func TestDelayTableSchedule(t *testing.T) {
	*flagScheduleDelaySecs = 5
	defer func() {
		*flagScheduleDelaySecs = 0
	}()

	te := setupTestTableEnv(
		t,
		2,
		1,
		2,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	opts := &pb.ScheduleOptions{
		EnablePrimaryScheduler:          true,
		MaxSchedRatio:                   1000,
		Estimator:                       "default",
		ForceRescoreNodes:               false,
		EnableSplitBalancer:             true,
		HashArrangerAddReplicaFirst:     true,
		HashArrangerMaxOverflowReplicas: 1,
	}
	te.table.Schedule(true, opts)

	assert.Equal(t, len(te.table.currParts[0].members.Peers), 0)
	time.Sleep(time.Second * 5)
	te.table.Schedule(true, opts)
	assert.Assert(t, len(te.table.currParts[0].members.Peers) > 0)
}

func TestPartitionNormalWithLearner(t *testing.T) {
	te := setupTestTableEnv(
		t,
		6,
		1,
		3,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	plans := map[int32]*actions.PartitionActions{}

	acts := &actions.PartitionActions{}
	acts.AddAction(&actions.AddLearnerAction{Node: "node1"})
	acts.AddAction(&actions.TransformAction{Node: "node1", ToRole: pb.ReplicaRole_kSecondary})
	acts.AddAction(&actions.TransformAction{Node: "node1", ToRole: pb.ReplicaRole_kPrimary})

	acts.AddAction(&actions.AddLearnerAction{Node: "node2"})
	acts.AddAction(&actions.TransformAction{Node: "node2", ToRole: pb.ReplicaRole_kSecondary})

	acts.AddAction(&actions.AddLearnerAction{Node: "node3"})

	plans[0] = acts

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	te.table.ApplyPlan(plans)
	for ans := te.table.scheduleAsPlan(); len(ans) > 0; ans = te.table.scheduleAsPlan() {
		logging.Info("still has plan to apply")
	}

	assert.Assert(t, te.table.CheckPartitions().Normal)
	assert.Equal(t, te.table.currParts[0].members.GetMember("node3"), pb.ReplicaRole_kLearner)
}

type mockStrategyRemoveNode struct {
	base.DummyStrategy
}

func (m *mockStrategyRemoveNode) Transform(
	pid utils.TblPartID,
	self *pb.PartitionReplica,
	others []*pb.PartitionReplica,
	parents []*pb.PartitionReplica,
	opts *base.TableRunningOptions,
) pb.ReplicaRole {
	return pb.ReplicaRole_kInvalid
}

func TestWantPromoteButRemoved(t *testing.T) {
	mockStrategy := &mockStrategyRemoveNode{}
	te := setupTestTableEnv(t, 6, 1, 3, mockStrategy, "", true, "zk")
	defer te.teardown()

	tbl := te.table
	plans := map[int32]*actions.PartitionActions{}

	acts := &actions.PartitionActions{}
	acts.AddAction(&actions.AddLearnerAction{Node: "node1"})
	acts.AddAction(&actions.PromoteToSecondaryAction{Node: "node1"})
	acts.AddAction(&actions.TransformAction{Node: "node1", ToRole: pb.ReplicaRole_kPrimary})

	plans[0] = acts

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	logging.Info("first will add learner as plan")
	te.table.ApplyPlan(plans)
	assert.Equal(t, tbl.GetMembership(0).GetMember("node1"), pb.ReplicaRole_kLearner)

	logging.Info("don't have collected replica info for node1, so state will be kept")
	ans := te.table.CheckPartitions()
	assert.Equal(t, ans.Normal, false)
	assert.Equal(t, tbl.GetMembership(0).GetMember("node1"), pb.ReplicaRole_kLearner)
	assert.Equal(t, tbl.plans[0].ActionCount(), 2)

	logging.Info("collected info is staled, so state will be kept")
	tbl.currParts[0].facts["node1"] = &replicaFact{
		membershipVersion: tbl.currParts[0].members.MembershipVersion - 1,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kLearner,
			ReadyToPromote: false,
			StatisticsInfo: map[string]string{
				"db_storage_limit_bytes": "800",
			},
		},
	}
	ans = te.table.CheckPartitions()
	assert.Equal(t, ans.Normal, false)
	assert.Equal(t, tbl.GetMembership(0).GetMember("node1"), pb.ReplicaRole_kLearner)
	assert.Equal(t, tbl.plans[0].ActionCount(), 2)

	logging.Info("next node will be removed by passive transform")
	tbl.currParts[0].facts["node1"] = &replicaFact{
		membershipVersion: tbl.currParts[0].members.MembershipVersion,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kLearner,
			ReadyToPromote: false,
			StatisticsInfo: map[string]string{
				"db_storage_limit_bytes": "800",
			},
		},
	}
	ans = te.table.CheckPartitions()
	assert.Equal(t, ans.Normal, false)
	assert.Equal(t, len(tbl.GetMembership(0).Peers), 0)
	assert.Equal(t, len(tbl.currParts[0].facts), 0)
	assert.Equal(t, tbl.GetMembership(0).MembershipVersion, int64(2))
	assert.Equal(t, tbl.plans[0].HasAction(), false)

	logging.Info("plan will be cancelled")
	ans = te.table.CheckPartitions()
	assert.Assert(t, tbl.plans == nil)
	assert.Equal(t, ans.Normal, true)
	assert.Equal(t, len(tbl.GetMembership(0).Peers), 0)
	assert.Equal(t, tbl.GetMembership(0).MembershipVersion, int64(2))
}

func TestTableScheduleGrayscaleCompatibility(t *testing.T) {
	oldTable := `{
	"table_id":23,
	"table_name":"test",
	"hash_method":"crc32",
	"parts_count":1,
	"json_args":"{\"btq_prefix\":\"colossusdb_embd_test_v0\",\"is_logic_btq\": true,\"table_num_keys\": 20000000000,\"table_size_bytes\": 1717986918400,\"table_inflation_ratio\": 1.5}",
	"belong_to_service":"test",
	"kconf_path":"recoKrp.testPartitionManager.test",
	"recover_partitions_from_route":false
}`
	newTable := `{
	"table_id":23,
	"table_name":"test",
	"hash_method":"crc32",
	"parts_count":1,
	"json_args":"{\"btq_prefix\":\"colossusdb_embd_test_v0\",\"is_logic_btq\": true,\"table_num_keys\": 20000000000,\"table_size_bytes\": 1717986918400,\"table_inflation_ratio\": 1.5}",
	"belong_to_service":"test",
	"kconf_path":"recoKrp.testPartitionManager.test",
	"recover_partitions_from_route":false,
	"schedule_grayscale":50
}`

	te := setupTestTableEnv(
		t,
		6,
		1,
		3,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		false,
		"zk",
	)
	defer te.teardown()

	te.table.initializeZkPath("test")
	succ := te.table.zkConn.MultiCreate(
		context.Background(),
		[]string{te.table.zkPath, te.table.tasksPath, te.table.partsPath},
		[][]byte{[]byte(oldTable), nil, nil},
	)
	assert.Assert(t, succ)

	expectProto := &pb.Table{}
	err := json.Unmarshal([]byte(oldTable), expectProto)
	assert.NilError(t, err)
	expectProto.ScheduleGrayscale = kScheduleGrayscaleMax

	te.table.LoadFromZookeeper("test")
	assert.Assert(t, proto.Equal(te.table.Table, expectProto))
	te.table.Stop(false)

	succ = te.table.zkConn.Set(context.Background(), te.table.zkPath, []byte(newTable))
	assert.Assert(t, succ)

	expectProto.ScheduleGrayscale = 50
	te.table.LoadFromZookeeper("test")
	assert.Assert(t, proto.Equal(te.table.Table, expectProto))
}

func TestQueryTable(t *testing.T) {
	te := setupTestTableEnv(
		t,
		2,
		8,
		3,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	resp := &pb.QueryTableResponse{}
	te.table.state.Set(utils.StateDropped)
	te.table.QueryTable(true, true, resp)
	assert.Assert(t, resp.Status.Code == int32(pb.AdminError_kTableNotExists))
	te.table.state.Set(utils.StateNormal)

	plans := map[int32]*actions.PartitionActions{}
	for i := 0; i < 8; i++ {
		ans := &actions.PartitionActions{}
		ans.AddAction(&actions.AddLearnerAction{Node: fmt.Sprintf("node%d", (i%2)+1)})
		plans[int32(i)] = ans
	}

	te.table.serviceLock.LockWrite()
	te.table.ApplyPlan(plans)
	te.table.serviceLock.UnlockWrite()

	// check the local state is changed
	for i := range te.table.currParts {
		p := &(te.table.currParts[i])
		assert.Equal(t, p.hasZkPath, true)
		assert.Equal(t, p.membershipChanged, false)
		assert.Equal(t, p.members.MembershipVersion, int64(1))
	}

	resp = &pb.QueryTableResponse{}
	te.table.QueryTable(true, true, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(resp.Partitions), 8)
	assert.Equal(t, len(resp.Tasks), 1)
	for i := 0; i < 8; i++ {
		peers := resp.Partitions[i].Peers
		assert.Equal(t, len(peers), 1)
		assert.Equal(t, len(peers[0].StatisticsInfo), 0)
	}

	for i := 0; i < 8; i++ {
		p := &(te.table.currParts[i])
		p.facts["node1"] = &replicaFact{
			membershipVersion: p.members.MembershipVersion,
			PartitionReplica: &pb.PartitionReplica{
				Role:           pb.ReplicaRole_kLearner,
				ReadyToPromote: true,
				StatisticsInfo: map[string]string{
					"db_storage_limit_bytes": "800",
				},
			},
		}
		p.facts["node2"] = &replicaFact{
			membershipVersion: p.members.MembershipVersion,
			PartitionReplica: &pb.PartitionReplica{
				Role:           pb.ReplicaRole_kSecondary,
				ReadyToPromote: true,
				StatisticsInfo: map[string]string{
					"db_storage_limit_bytes": "600",
				},
			},
		}
	}

	resp = &pb.QueryTableResponse{}
	te.table.QueryTable(true, true, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(resp.Partitions), 8)
	assert.Equal(t, len(resp.Tasks), 1)

	for i := 0; i < 8; i++ {
		peers := resp.Partitions[i].Peers
		assert.Equal(t, len(peers), 1)
		assert.Equal(t, len(peers[0].StatisticsInfo), 1)
		if i%2 == 0 {
			assert.Equal(t, peers[0].StatisticsInfo["db_storage_limit_bytes"], "800")
		} else {
			assert.Equal(t, peers[0].StatisticsInfo["db_storage_limit_bytes"], "600")
		}
	}
}

func TestDeleteTask(t *testing.T) {
	te := setupTestTableEnv(
		t,
		2,
		8,
		3,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	assert.Equal(t, len(te.table.tasks), 1)
	resp := te.table.DeleteTask(checkpoint.TASK_NAME, false, 0)
	assert.Equal(t, resp.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(te.table.tasks), 0)
	_, exists, succ := te.table.zkConn.Get(context.Background(), te.table.tasksPath+"/checkpoint")
	assert.Assert(t, succ && !exists)
	_, exists, succ = te.table.zkConn.Get(
		context.Background(),
		te.testZkRoot+"/delayed_executor/1_checkpoint",
	)
	assert.Assert(t, succ && !exists)

	taskDesc := &pb.PeriodicTask{
		TaskName:                checkpoint.TASK_NAME,
		FirstTriggerUnixSeconds: time.Now().Unix(),
		PeriodSeconds:           60,
		MaxConcurrencyPerNode:   1,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
		KeepNums:                1,
		Args:                    map[string]string{},
	}
	resp = te.table.CreateTask(taskDesc)
	assert.Equal(t, resp.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(te.table.tasks), 1)
	_, exists, succ = te.table.zkConn.Get(context.Background(), te.table.tasksPath+"/checkpoint")
	assert.Assert(t, succ && exists)

	resp = te.table.DeleteTask(checkpoint.TASK_NAME, true, 1)
	assert.Equal(t, resp.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(te.table.tasks), 0)
	_, exists, succ = te.table.zkConn.Get(context.Background(), te.table.tasksPath+"/checkpoint")
	assert.Assert(t, succ && !exists)

	_, exists, succ = te.table.zkConn.Get(
		context.Background(),
		te.testZkRoot+"/delayed_executor/1_checkpoint",
	)
	assert.Assert(t, succ && exists)
	assert.Equal(t, len(te.table.tasks), 0)

	// Create a task with the same name for the task in the delayed_executor
	resp = te.table.CreateTask(taskDesc)
	assert.Equal(t, resp.Code, int32(pb.AdminError_kTaskExists))
}

func TestQueryPartition(t *testing.T) {
	te := setupTestTableEnv(
		t,
		2,
		8,
		3,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	resp := &pb.QueryPartitionResponse{}
	te.table.state.Set(utils.StateDropped)
	te.table.QueryPartition(0, 0, resp)
	assert.Assert(t, resp.Status.Code == int32(pb.AdminError_kTableNotExists))
	te.table.state.Set(utils.StateNormal)

	plans := map[int32]*actions.PartitionActions{}
	for i := 0; i < 8; i++ {
		ans := &actions.PartitionActions{}
		ans.AddAction(&actions.AddLearnerAction{Node: fmt.Sprintf("node%d", (i%2)+1)})
		plans[int32(i)] = ans
	}

	te.table.serviceLock.LockWrite()
	te.table.ApplyPlan(plans)
	te.table.serviceLock.UnlockWrite()

	// check the local state is changed
	for i := range te.table.currParts {
		p := &(te.table.currParts[i])
		assert.Equal(t, p.hasZkPath, true)
		assert.Equal(t, p.membershipChanged, false)
		assert.Equal(t, p.members.MembershipVersion, int64(1))
	}

	resp = &pb.QueryPartitionResponse{}
	te.table.QueryPartition(0, 0, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(resp.Partitions), 8)
	assert.Equal(t, resp.TablePartsCount, int32(8))
	for i := 0; i < 8; i++ {
		peers := resp.Partitions[i].Peers
		assert.Equal(t, len(peers), 1)
		assert.Equal(t, len(peers[0].StatisticsInfo), 0)
	}

	for i := 0; i < 8; i++ {
		p := &(te.table.currParts[i])
		p.facts["node1"] = &replicaFact{
			membershipVersion: p.members.MembershipVersion,
			PartitionReplica: &pb.PartitionReplica{
				Role:           pb.ReplicaRole_kLearner,
				ReadyToPromote: true,
				StatisticsInfo: map[string]string{
					"db_storage_limit_bytes": strconv.FormatInt(int64(i*100), 10),
				},
			},
		}
		p.facts["node2"] = &replicaFact{
			membershipVersion: p.members.MembershipVersion,
			PartitionReplica: &pb.PartitionReplica{
				Role:           pb.ReplicaRole_kSecondary,
				ReadyToPromote: true,
				StatisticsInfo: map[string]string{
					"db_storage_limit_bytes": strconv.FormatInt(int64(i*200), 10),
				},
			},
		}
	}

	resp = &pb.QueryPartitionResponse{}
	te.table.QueryPartition(0, 0, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(resp.Partitions), 8)
	assert.Equal(t, resp.TablePartsCount, int32(8))

	for i := 0; i < 8; i++ {
		peers := resp.Partitions[i].Peers
		assert.Equal(t, len(peers), 1)
		assert.Equal(t, len(peers[0].StatisticsInfo), 1)
		if i%2 == 0 {
			assert.Equal(
				t,
				peers[0].StatisticsInfo["db_storage_limit_bytes"],
				strconv.FormatInt(int64(i*100), 10),
			)
		} else {
			assert.Equal(t, peers[0].StatisticsInfo["db_storage_limit_bytes"], strconv.FormatInt(int64(i*200), 10))
		}
	}

	resp = &pb.QueryPartitionResponse{}
	te.table.QueryPartition(-1, 0, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, resp.TablePartsCount, int32(8))
	assert.Equal(t, len(resp.Partitions), 0)

	resp = &pb.QueryPartitionResponse{}
	te.table.QueryPartition(4, 1, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, resp.TablePartsCount, int32(8))
	assert.Equal(t, len(resp.Partitions), 0)

	resp = &pb.QueryPartitionResponse{}
	te.table.QueryPartition(4, 4, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, resp.TablePartsCount, int32(8))
	assert.Equal(t, len(resp.Partitions), 1)
	peers := resp.Partitions[0].Peers
	assert.Equal(
		t,
		peers[0].StatisticsInfo["db_storage_limit_bytes"],
		strconv.FormatInt(int64(4*100), 10),
	)

	resp = &pb.QueryPartitionResponse{}
	te.table.QueryPartition(0, 100, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, resp.TablePartsCount, int32(8))
	assert.Equal(t, len(resp.Partitions), 8)

	resp = &pb.QueryPartitionResponse{}
	te.table.QueryPartition(60, 100, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, resp.TablePartsCount, int32(8))
	assert.Equal(t, len(resp.Partitions), 0)

	resp = &pb.QueryPartitionResponse{}
	te.table.QueryPartition(0, 7, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, resp.TablePartsCount, int32(8))
	assert.Equal(t, len(resp.Partitions), 8)
	for i := 0; i < 8; i++ {
		peers := resp.Partitions[i].Peers
		assert.Equal(t, len(peers), 1)
		assert.Equal(t, len(peers[0].StatisticsInfo), 1)
		if i%2 == 0 {
			assert.Equal(
				t,
				peers[0].StatisticsInfo["db_storage_limit_bytes"],
				strconv.FormatInt(int64(i*100), 10),
			)
		} else {
			assert.Equal(t, peers[0].StatisticsInfo["db_storage_limit_bytes"], strconv.FormatInt(int64(i*200), 10))
		}
	}
}
