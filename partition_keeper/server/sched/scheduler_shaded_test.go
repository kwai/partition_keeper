package sched

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"gotest.tools/assert"
)

func TestSchedulerShaded(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 2, []int{1, 1}, 1)
	baseTable := actions.NewTableModelMock(&pb.Table{
		TableId:    2,
		TableName:  "test_base_table",
		PartsCount: 1,
	})

	baseTable.GetMembership(0).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kPrimary,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	scheduler := NewShadedScheduler("test", baseTable)

	logging.Info("first add learner in hub1")
	plan := scheduler.Schedule(te.getScheduleInput())
	te.applyPlan(plan)
	assert.DeepEqual(
		t,
		te.table.GetMembership(0).Peers,
		map[string]pb.ReplicaRole{"node_yz0_0": pb.ReplicaRole_kLearner},
	)

	logging.Info("then add learner in hub2")
	plan = scheduler.Schedule(te.getScheduleInput())
	te.applyPlan(plan)
	assert.DeepEqual(
		t,
		te.table.GetMembership(0).Peers,
		map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
			"node_yz1_0": pb.ReplicaRole_kLearner,
		},
	)

	logging.Info("then manually promote node_yz1_0 to secondary")
	plan[0] = actions.MakeActions(
		&actions.TransformAction{Node: "node_yz1_0", ToRole: pb.ReplicaRole_kSecondary},
	)
	te.applyPlan(plan)

	logging.Info("then schedule will elect node_yz1_0 as primary")
	plan = scheduler.Schedule(te.getScheduleInput())
	te.applyPlan(plan)
	assert.DeepEqual(
		t,
		te.table.GetMembership(0).Peers,
		map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
			"node_yz1_0": pb.ReplicaRole_kPrimary,
		},
	)

	logging.Info("no action")
	plan = scheduler.Schedule(te.getScheduleInput())
	assert.Equal(t, len(plan), 0)

	logging.Info("then promote node_yz0_0 to secondary")
	te.promoteAllLearners()
	assert.DeepEqual(
		t,
		te.table.GetMembership(0).Peers,
		map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kPrimary,
		},
	)

	logging.Info("then scheduler will trasfer primary to node_yz0_0")
	plan = scheduler.Schedule(te.getScheduleInput())
	te.applyPlan(plan)
	assert.DeepEqual(
		t,
		te.table.GetMembership(0).Peers,
		map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
		},
	)

	logging.Info("then mark node_yz0_0 to restart, the restart_op_inspector will work")
	te.nodes.MustGetNodeInfo("node_yz0_0").Op = pb.AdminNodeOp_kRestart
	plan = scheduler.Schedule(te.getScheduleInput())
	te.applyPlan(plan)
	assert.DeepEqual(
		t,
		te.table.GetMembership(0).Peers,
		map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
			"node_yz1_0": pb.ReplicaRole_kPrimary,
		},
	)
}
