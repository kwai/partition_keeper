package server

import (
	"context"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
)

type testTableRestoreCtrlEnv struct {
	tableEnv *testTableEnv
	zkPath   string
}

func setupTestTableRestoreCtrlEnv(t *testing.T) *testTableRestoreCtrlEnv {
	output := &testTableRestoreCtrlEnv{
		tableEnv: setupTestTableEnv(
			t,
			4,
			1,
			2,
			strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
			"",
			true,
			"zk",
		),
		zkPath: utils.SpliceZkRootPath("/test/pk/restore_ctrl"),
	}
	ans := output.tableEnv.zkStore.RecursiveDelete(context.Background(), output.zkPath)
	assert.Assert(t, ans)
	return output
}

func (te *testTableRestoreCtrlEnv) teardown() {
	te.tableEnv.teardown()
}

func (te *testTableRestoreCtrlEnv) getNodesOnSameHubsWith(n string) []string {
	nodes := te.tableEnv.table.nodes
	refInfo := nodes.MustGetNodeInfo(n)
	return nodes.FilterNodes(func(info *node_mgr.NodeInfo) bool {
		return info.Hub == refInfo.Hub && info.Id != refInfo.Id
	})
}

func (te *testTableRestoreCtrlEnv) getNodesOnOtherHubs(n string) []string {
	nodes := te.tableEnv.table.nodes
	refInfo := nodes.MustGetNodeInfo(n)
	return nodes.FilterNodes(func(info *node_mgr.NodeInfo) bool {
		return info.Hub != refInfo.Hub
	})
}

func TestTableRestoreControllerUpdate(t *testing.T) {
	te := setupTestTableRestoreCtrlEnv(t)
	defer te.teardown()

	ctrl := NewTableRestoreController(te.tableEnv.table, te.zkPath)
	req := &pb.RestoreTableRequest{
		ServiceName: "test",
		TableName:   "test",
		RestorePath: "/tmp/colossusdb_test",
		Opts: &pb.RestoreOpts{
			MaxConcurrentNodesPerHub:  1,
			MaxConcurrentPartsPerNode: -1,
		},
	}
	ctrl.InitializeNew(req)
	assert.Equal(t, ctrl.stats.Stage, RestorePending)

	logging.Info("initialized ctrl is durable")
	ctrl2 := NewTableRestoreController(te.tableEnv.table, te.zkPath)
	assert.Assert(t, ctrl2.LoadFromZookeeper())

	assert.Equal(t, ctrl.stats.RestorePath, ctrl2.stats.RestorePath)
	assert.Equal(t, ctrl.stats.Stage, ctrl2.stats.Stage)
	assert.Assert(t, proto.Equal(ctrl.stats.RestoreOpts, ctrl2.stats.RestoreOpts))

	logging.Info("start running is durable")
	ctrl.StartRunning()
	assert.Equal(t, ctrl.stats.Stage, RestoreRunning)
	ctrl2.LoadFromZookeeper()
	assert.Equal(t, ctrl2.stats.Stage, RestoreRunning)

	logging.Info("update is durable")
	req2 := &pb.RestoreTableRequest{
		RestorePath: "/tmp/colossusdb_test",
		Opts: &pb.RestoreOpts{
			MaxConcurrentNodesPerHub:  10,
			MaxConcurrentPartsPerNode: 10,
		},
	}
	ctrl.Update(req2)
	assert.Equal(t, ctrl.stats.Stage, RestoreRunning)
	assert.Assert(t, proto.Equal(ctrl.stats.RestoreOpts, req2.Opts))
	ctrl2.LoadFromZookeeper()
	assert.Assert(t, proto.Equal(ctrl2.stats.RestoreOpts, req2.Opts))

	logging.Info("changing restore path will reset restore stage")
	req3 := &pb.RestoreTableRequest{
		RestorePath: "/tmp/colossusdb_test_2",
		Opts: &pb.RestoreOpts{
			MaxConcurrentNodesPerHub:  5,
			MaxConcurrentPartsPerNode: 5,
		},
	}
	ctrl.Update(req3)
	assert.Equal(t, ctrl.stats.Stage, RestorePending)
	assert.Assert(t, proto.Equal(ctrl.stats.RestoreOpts, req3.Opts))
	ctrl2.LoadFromZookeeper()
	assert.Assert(t, proto.Equal(ctrl2.stats.RestoreOpts, req3.Opts))
}

func TestTableRestoreControllerGeneratePlan(t *testing.T) {
	te := setupTestTableRestoreCtrlEnv(t)
	defer te.teardown()

	ctrl := NewTableRestoreController(te.tableEnv.table, te.zkPath)
	req := &pb.RestoreTableRequest{
		ServiceName: "test",
		TableName:   "test",
		RestorePath: "/tmp/colossusdb_test",
		Opts: &pb.RestoreOpts{
			MaxConcurrentNodesPerHub:  -1,
			MaxConcurrentPartsPerNode: 1,
		},
	}
	ctrl.InitializeNew(req)

	te.tableEnv.table.serviceLock.LockWrite()
	defer te.tableEnv.table.serviceLock.UnlockWrite()

	logging.Info("no action if state is not running")
	newPlans, finished := ctrl.GenerateRestorePlan()
	assert.Equal(t, len(newPlans), 0)
	assert.Equal(t, finished, false)

	logging.Info("no action if have primary")
	ctrl.StartRunning()
	plans := map[int32]*actions.PartitionActions{}
	acts := &actions.PartitionActions{}
	acts.AddAction(&actions.AddLearnerAction{Node: "node1"})
	acts.AddAction(&actions.TransformAction{Node: "node1", ToRole: pb.ReplicaRole_kSecondary})
	acts.AddAction(&actions.TransformAction{Node: "node1", ToRole: pb.ReplicaRole_kPrimary})
	plans[0] = acts
	te.tableEnv.table.ApplyPlan(plans)
	for ans := te.tableEnv.table.scheduleAsPlan(); len(ans) > 0; ans = te.tableEnv.table.scheduleAsPlan() {
		logging.Info("still has plan to apply")
	}
	newPlans, finished = ctrl.GenerateRestorePlan()
	assert.Equal(t, len(newPlans), 0)
	assert.Equal(t, finished, false)

	logging.Info("no action if some have don't have replicas")
	acts = &actions.PartitionActions{}
	acts.AddAction(&actions.TransformAction{Node: "node1", ToRole: pb.ReplicaRole_kSecondary})
	plans[0] = acts
	te.tableEnv.table.ApplyPlan(plans)
	for ans := te.tableEnv.table.scheduleAsPlan(); len(ans) > 0; ans = te.tableEnv.table.scheduleAsPlan() {
		logging.Info("still has plan to apply")
	}
	newPlans, finished = ctrl.GenerateRestorePlan()
	assert.Equal(t, len(newPlans), 0)
	assert.Equal(t, finished, false)

	logging.Info("no actions if some hub have more than 1 replicas")
	otherNodes := te.getNodesOnSameHubsWith("node1")
	assert.Assert(t, len(otherNodes) > 0)
	acts = &actions.PartitionActions{}
	acts.AddAction(&actions.AddLearnerAction{Node: otherNodes[0]})
	plans[0] = acts
	te.tableEnv.table.ApplyPlan(plans)
	newPlans, finished = ctrl.GenerateRestorePlan()
	assert.Equal(t, len(newPlans), 0)
	assert.Equal(t, finished, false)

	logging.Info("no actions if some nodes are located out of hub")
	te.tableEnv.table.UpdateHubs([]*pb.ReplicaHub{{Name: "invalid_az", Az: "YZ"}})
	newPlans, finished = ctrl.GenerateRestorePlan()
	assert.Equal(t, len(newPlans), 0)
	assert.Equal(t, finished, false)

	logging.Info("make table partitions normal")
	te.tableEnv.table.UpdateHubs([]*pb.ReplicaHub{
		{Name: "yz0", Az: "YZ"},
		{Name: "yz1", Az: "YZ"},
	})
	acts = &actions.PartitionActions{}
	acts.AddAction(&actions.RemoveNodeAction{Node: otherNodes[0]})
	anotherHubNodes := te.getNodesOnOtherHubs("node1")
	assert.Assert(t, len(anotherHubNodes) > 0)
	acts.AddAction(&actions.AddLearnerAction{Node: anotherHubNodes[0]})
	plans[0] = acts

	te.tableEnv.table.ApplyPlan(plans)
	for ans := te.tableEnv.table.scheduleAsPlan(); len(ans) > 0; ans = te.tableEnv.table.scheduleAsPlan() {
		logging.Info("still has actions in plan")
	}

	logging.Info("allocate new restore version for table")
	newVersion := time.Now().Unix()
	te.tableEnv.table.updateMeta(func(tablePb *pb.Table) {
		tablePb.RestoreVersion = newVersion
	})

	onYz0, onYz1 := "", ""
	if te.tableEnv.table.nodes.MustGetNodeInfo("node1").Hub == "yz0" {
		onYz0 = "node1"
		onYz1 = anotherHubNodes[0]
	} else {
		onYz0 = anotherHubNodes[0]
		onYz1 = "node1"
	}

	part := &(te.tableEnv.table.currParts[0])
	logging.Info("will generate plan for yz0")
	newPlans, finished = ctrl.GenerateRestorePlan()
	assert.Equal(t, finished, false)
	assert.Equal(t, len(newPlans), 1)
	assert.Equal(t, len(ctrl.progress["yz0"].runningNodes), 1)

	logging.Info("node on hub1 will be removed, then added")
	te.tableEnv.table.ApplyPlan(newPlans)
	te.tableEnv.table.scheduleAsPlan()
	assert.Equal(t, te.tableEnv.table.currParts[0].members.RestoreVersion[onYz0], newVersion)

	logging.Info("can't get fact of %s, treat restore running", onYz0)
	newPlans, finished = ctrl.GenerateRestorePlan()
	assert.Equal(t, len(newPlans), 0)
	assert.Equal(t, finished, false)

	logging.Info("fact of %s has smaller restore version, treat running", onYz0)
	part.facts[onYz0] = &replicaFact{
		membershipVersion: part.members.MembershipVersion,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kLearner,
			ReadyToPromote: false,
			RestoreVersion: 0,
		},
	}
	newPlans, finished = ctrl.GenerateRestorePlan()
	assert.Equal(t, len(newPlans), 0)
	assert.Equal(t, finished, false)

	logging.Info("fact of %s is still learner, treat running", onYz0)
	part.facts[onYz0].PartitionReplica.RestoreVersion = newVersion
	newPlans, finished = ctrl.GenerateRestorePlan()
	assert.Equal(t, len(newPlans), 0)
	assert.Equal(t, len(ctrl.progress["yz0"].runningNodes), 1)
	assert.Equal(t, finished, false)

	logging.Info("make %s secondary", onYz0)
	part.facts[onYz0].PartitionReplica.ReadyToPromote = true
	te.tableEnv.table.scheduleAsPlan()
	part.facts[onYz0].PartitionReplica.Role = pb.ReplicaRole_kSecondary
	part.facts[onYz0].membershipVersion = part.members.MembershipVersion

	logging.Info("yz0 will be finished, and plan will be generated for yz1")
	newPlans, finished = ctrl.GenerateRestorePlan()
	assert.Equal(t, len(newPlans), 1)
	assert.Equal(t, len(ctrl.progress["yz0"].runningNodes), 0)
	assert.Equal(t, len(ctrl.progress["yz1"].runningNodes), 1)
	assert.Equal(t, finished, false)
	assert.Equal(t, ctrl.nextHub, "yz1")
	assert.Equal(t, ctrl.getHubProgress("yz0").finishCount, 1)

	logging.Info("consume new restore plan")
	te.tableEnv.table.ApplyPlan(newPlans)
	te.tableEnv.table.scheduleAsPlan()
	part.facts[onYz1] = &replicaFact{
		membershipVersion: part.members.MembershipVersion,
		PartitionReplica: &pb.PartitionReplica{
			Role:           pb.ReplicaRole_kLearner,
			ReadyToPromote: true,
			RestoreVersion: newVersion,
		},
	}
	te.tableEnv.table.scheduleAsPlan()
	part.facts[onYz1].membershipVersion = part.members.MembershipVersion

	logging.Info("restore will finish")
	newPlans, finished = ctrl.GenerateRestorePlan()
	assert.Equal(t, len(newPlans), 0)
	assert.Equal(t, len(ctrl.progress["yz1"].runningNodes), 0)
	assert.Equal(t, finished, true)
	assert.Equal(t, ctrl.stats.Stage, RestoreFinished)

	logging.Info("finished restore will have no plan")
	newPlans, finished = ctrl.GenerateRestorePlan()
	assert.Equal(t, len(newPlans), 0)
	assert.Equal(t, finished, true)

	logging.Info("finish status is durable")
	ctrl2 := NewTableRestoreController(te.tableEnv.table, te.zkPath)
	assert.Equal(t, ctrl2.LoadFromZookeeper(), true)
	assert.Equal(t, ctrl2.stats.Stage, RestoreFinished)
}
