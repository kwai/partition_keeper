package sched

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"gotest.tools/assert"
)

func TestSortHubs(t *testing.T) {
	inspector := &adaptivePartitionInspector{}
	inspector.SetLogName("test")

	hubs := map[string][]string{
		"hub1": {"node1", "node2"},
		"hub2": {"node3"},
		"hub3": {"node4"},
		"hub4": {"node5", "node6"},
	}

	sortedHubs := inspector.sortHubs(hubs)
	assert.Equal(t, len(sortedHubs), 4)

	sortedName := []string{"hub2", "hub3", "hub1", "hub4"}
	for i, item := range sortedHubs {
		assert.Equal(t, item.name, sortedName[i])
		assert.DeepEqual(t, item.nodes, hubs[item.name])
	}
}

func TestNoNodeToAllocateReplica(t *testing.T) {
	var err error
	hubs := []*pb.ReplicaHub{
		{Name: "yz0", Az: "YZ"},
		{Name: "yz1", Az: "YZ"},
	}
	nodes, _ := createNodeStats(t, hubs, []int{2, 3})
	nodes.MustGetNodeInfo("node_yz0_0").Op = pb.AdminNodeOp_kOffline
	nodes.MustGetNodeInfo("node_yz0_1").Op = pb.AdminNodeOp_kOffline

	table := actions.NewTableModelMock(&pb.Table{
		TableId:    1,
		TableName:  "test",
		PartsCount: 1,
	})
	table.EstimatedReplicas, err = EstimateTableReplicas("test", nodes, table)
	assert.NilError(t, err)
	*table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz1_0": pb.ReplicaRole_kLearner,
		},
	}
	scheduleInput := &ScheduleInput{
		Table: table,
		Nodes: nodes,
		Hubs:  hubs,
		Opts: ScheduleCtrlOptions{
			Configs:          &pb.ScheduleOptions{EnablePrimaryScheduler: true, MaxSchedRatio: 100},
			PrepareRestoring: false,
		},
	}
	scheduler := NewScheduler(ADAPTIVE_SCHEDULER, "test-service")
	plan := scheduler.Schedule(scheduleInput)
	assert.Equal(t, len(plan), 0)
}

func applyPlan(table *actions.TableModelMock, plan SchedulePlan) bool {
	if len(plan) == 0 {
		return false
	}
	for pid, action := range plan {
		part := table.GetActionAcceptor(pid)
		for action.HasAction() {
			action.ConsumeAction(part)
		}
	}
	return true
}

func TestRemoveRedundantReplica(t *testing.T) {
	var err error
	hubs := []*pb.ReplicaHub{
		{Name: "yz0", Az: "YZ"},
	}
	nodes, _ := createNodeStats(t, hubs, []int{4})
	table := actions.NewTableModelMock(&pb.Table{
		TableId:    1,
		TableName:  "test",
		PartsCount: 1,
	})
	table.EstimatedReplicas, err = EstimateTableReplicas("test", nodes, table)
	assert.NilError(t, err)

	// first get the node which should contain the only replica
	replicaShouldLocated := ""
	for id := range nodes.AllNodes() {
		if replicaShouldLocated == "" {
			replicaShouldLocated = id
		} else {
			if table.GetEstimatedReplicasOnNode(id) > table.GetEstimatedReplicasOnNode(replicaShouldLocated) {
				replicaShouldLocated = id
			}
		}
	}

	nodesShouldEmpty := nodes.FilterNodes(func(info *node_mgr.NodeInfo) bool {
		return info.Id != replicaShouldLocated
	})

	logging.Info("%s removed as learner though it' loads is lighter", replicaShouldLocated)
	members := table.GetMembership(0)
	members.MembershipVersion = 10
	members.Peers = map[string]pb.ReplicaRole{
		replicaShouldLocated: pb.ReplicaRole_kLearner,
		nodesShouldEmpty[0]:  pb.ReplicaRole_kPrimary,
	}

	schedInput := &ScheduleInput{
		Table: table,
		Nodes: nodes,
		Hubs:  hubs,
		Opts: ScheduleCtrlOptions{
			Configs: &pb.ScheduleOptions{
				EnablePrimaryScheduler: true,
				MaxSchedRatio:          SCHEDULE_RATIO_MAX_VALUE,
			},
		},
	}
	sched := NewScheduler(ADAPTIVE_SCHEDULER, "test-service")
	plan := sched.Schedule(schedInput)
	assert.Equal(t, len(plan), 1)
	applyPlan(table, plan)
	assert.Assert(t, table.GetMembership(0).HasMember(nodesShouldEmpty[0]))
	assert.Assert(t, !table.GetMembership(0).HasMember(replicaShouldLocated))

	logging.Info("lighter primary will be removed")
	*table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			replicaShouldLocated: pb.ReplicaRole_kSecondary,
			nodesShouldEmpty[0]:  pb.ReplicaRole_kPrimary,
		},
	}

	plan = sched.Schedule(schedInput)
	assert.Equal(t, len(plan), 1)
	assert.Equal(t, plan[0].ActionCount(), 2)
	applyPlan(table, plan)
	assert.Equal(
		t,
		table.GetMembership(0).GetMember(nodesShouldEmpty[0]),
		pb.ReplicaRole_kInvalid,
	)
	assert.Equal(
		t,
		table.GetMembership(0).GetMember(replicaShouldLocated),
		pb.ReplicaRole_kSecondary,
	)

	logging.Info("prefer to remove from offlined node")
	*table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			nodesShouldEmpty[0]: pb.ReplicaRole_kLearner,
			nodesShouldEmpty[1]: pb.ReplicaRole_kLearner,
		},
	}
	nodes.MustGetNodeInfo(nodesShouldEmpty[0]).Op = pb.AdminNodeOp_kOffline
	nodes.MustGetNodeInfo(nodesShouldEmpty[1]).IsAlive = false

	plan = sched.Schedule(schedInput)
	assert.Equal(t, len(plan), 1)
	assert.Equal(t, plan[0].ActionCount(), 1)
	applyPlan(table, plan)
	assert.Assert(t, !table.GetMembership(0).HasMember(nodesShouldEmpty[0]))
	assert.Assert(t, table.GetMembership(0).HasMember(nodesShouldEmpty[1]))

	logging.Info("prefer to remove from dead")
	*table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			nodesShouldEmpty[0]: pb.ReplicaRole_kLearner,
			nodesShouldEmpty[1]: pb.ReplicaRole_kLearner,
		},
	}
	nodes.MustGetNodeInfo(nodesShouldEmpty[0]).Op = pb.AdminNodeOp_kNoop

	plan = sched.Schedule(schedInput)
	assert.Equal(t, len(plan), 1)
	assert.Equal(t, plan[0].ActionCount(), 1)
	applyPlan(table, plan)
	assert.Assert(t, table.GetMembership(0).HasMember(nodesShouldEmpty[0]))
	assert.Assert(t, !table.GetMembership(0).HasMember(nodesShouldEmpty[1]))
}

func TestRemoveExtraHubs(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 2, []int{2, 2}, 2)
	inspector := &adaptivePartitionInspector{}
	inspector.SetLogName("test-service")

	logging.Info("don't schedule if already has plan")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz0_1": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
		},
	}
	te.updateRecorder()
	input := te.getScheduleInput()
	input.Hubs = []*pb.ReplicaHub{
		{Name: "yz0", Az: "YZ"},
	}
	plan := SchedulePlan{
		0: actions.MakeActions(
			&actions.TransformAction{Node: "node_yz0_0", ToRole: pb.ReplicaRole_kPrimary},
		),
	}
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	switch plan[0].GetAction(0).(type) {
	case *actions.TransformAction:
		act := plan[0].GetAction(0).(*actions.TransformAction)
		assert.Equal(t, act.Node, "node_yz0_0")
		assert.Equal(t, act.ToRole, pb.ReplicaRole_kPrimary)
	default:
		assert.Assert(t, false)
	}

	logging.Info("remove extra hubs will be delayed if fill active hubs take effect")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz0_1": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
		},
	}
	te.updateRecorder()
	input = te.getScheduleInput()
	input.Hubs = []*pb.ReplicaHub{
		{Name: "yz0", Az: "YZ"},
	}
	plan = SchedulePlan{}
	inspector.Schedule(input, te.nr, plan)
	assert.Assert(t, plan[0] != nil)
	te.applyPlan(plan)
	assert.Equal(t, len(te.table.GetMembership(0).Peers), 2)
	assert.Equal(t, te.table.GetMembership(0).GetMember("node_yz1_0"), pb.ReplicaRole_kSecondary)

	plan = SchedulePlan{}
	inspector.Schedule(input, te.nr, plan)
	assert.Assert(t, plan[0] != nil)
	assert.Equal(t, plan[0].ActionCount(), 2)
	te.applyPlan(plan)
	assert.Equal(t, len(te.table.GetMembership(0).Peers), 1)
	assert.Equal(t, te.table.GetMembership(0).GetMember("node_yz1_0"), pb.ReplicaRole_kInvalid)

	logging.Info("remove learner from extra hubs")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_1": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kLearner,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
		},
	}
	te.updateRecorder()
	input = te.getScheduleInput()
	input.Hubs = []*pb.ReplicaHub{
		{Name: "yz0", Az: "YZ"},
	}

	plan = SchedulePlan{}
	inspector.Schedule(input, te.nr, plan)
	assert.Assert(t, plan[0] != nil)
	assert.Equal(t, plan[0].ActionCount(), 1)
	te.applyPlan(plan)
	assert.Equal(t, len(te.table.GetMembership(0).Peers), 1)
	assert.Equal(t, te.table.GetMembership(0).GetMember("node_yz1_0"), pb.ReplicaRole_kInvalid)
}

func TestAddLearnerPriority(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 1, []int{2}, 4)
	inspector := &adaptivePartitionInspector{}
	inspector.SetLogName("test-service")

	logging.Info("select node_yz0_1 which has less nodes")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_1": pb.ReplicaRole_kLearner,
		},
	}
	*te.table.GetMembership(2) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
		},
	}

	te.updateRecorder()
	input := te.getScheduleInput()
	plan := SchedulePlan{}
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	assert.Assert(t, plan[3] != nil)
	switch plan[3].GetAction(0).(type) {
	case *actions.AddLearnerAction:
		act := plan[3].GetAction(0).(*actions.AddLearnerAction)
		assert.Equal(t, act.Node, "node_yz0_1")
	default:
		assert.Assert(t, false)
	}

	logging.Info("select node_yz0_0 which has less learner although they have same node")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_1": pb.ReplicaRole_kLearner,
		},
	}
	*te.table.GetMembership(2) = table_model.PartitionMembership{}
	*te.table.GetMembership(3) = table_model.PartitionMembership{}
	te.updateRecorder()
	input = te.getScheduleInput()
	plan = SchedulePlan{
		3: actions.MakeActions(&actions.RemoveNodeAction{Node: "node_yz1_0"}),
	}
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 2)
	assert.Assert(t, plan[2] != nil)
	switch plan[2].GetAction(0).(type) {
	case *actions.AddLearnerAction:
		act := plan[2].GetAction(0).(*actions.AddLearnerAction)
		assert.Equal(t, act.Node, "node_yz0_0")
	default:
		assert.Assert(t, false)
	}
	assert.Assert(t, plan[3] != nil)
	switch plan[3].GetAction(0).(type) {
	case *actions.RemoveNodeAction:
		assert.Assert(t, true)
	default:
		assert.Assert(t, false)
	}
}

func TestMultiHubsActionInTurn(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 3, []int{1, 1, 1}, 1)
	inspector := &adaptivePartitionInspector{}
	inspector.SetLogName("test-service")

	input := te.getScheduleInput()
	plan := SchedulePlan{}

	te.updateRecorder()
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kLearner,
	})

	te.updateRecorder()
	plan = SchedulePlan{}
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kLearner,
		"node_yz1_0": pb.ReplicaRole_kLearner,
	})

	te.updateRecorder()
	plan = SchedulePlan{}
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kLearner,
		"node_yz1_0": pb.ReplicaRole_kLearner,
		"node_yz2_0": pb.ReplicaRole_kLearner,
	})
}
