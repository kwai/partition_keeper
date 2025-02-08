package sched

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"gotest.tools/assert"
)

func TestRestartOpInspector(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 4, []int{1, 1, 1, 1}, 16)

	scheduler := NewScheduler(ADAPTIVE_SCHEDULER, "test-service")

	logging.Info("if primary & secondary restart, will only do primary for one round")
	te.nodes.MustGetNodeInfo("node_yz0_0").Op = pb.AdminNodeOp_kRestart
	te.nodes.MustGetNodeInfo("node_yz1_0").Op = pb.AdminNodeOp_kRestart
	for i := range te.table.Partitions {
		members := &(te.table.Partitions[i])
		members.MembershipVersion = 7
		members.Peers = map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
			"node_yz2_0": pb.ReplicaRole_kSecondary,
			"node_yz3_0": pb.ReplicaRole_kSecondary,
		}
	}

	scheduleInput := &ScheduleInput{
		Table: te.table,
		Nodes: te.nodes,
		Hubs:  te.hubs,
		Opts: ScheduleCtrlOptions{
			Configs:          &pb.ScheduleOptions{EnablePrimaryScheduler: true, MaxSchedRatio: 100},
			PrepareRestoring: false,
		},
	}
	plan := scheduler.Schedule(scheduleInput)
	te.applyPlan(plan)
	for i := int32(0); i < te.table.PartsCount; i++ {
		assert.Equal(t, te.table.GetMembership(i).GetMember("node_yz0_0"), pb.ReplicaRole_kLearner)
	}

	logging.Info("only restart a secondary each round")
	te.nodes.MustGetNodeInfo("node_yz0_0").Op = pb.AdminNodeOp_kNoop
	te.nodes.MustGetNodeInfo("node_yz1_0").Op = pb.AdminNodeOp_kRestart
	te.nodes.MustGetNodeInfo("node_yz2_0").Op = pb.AdminNodeOp_kRestart
	for i := range te.table.Partitions {
		members := &(te.table.Partitions[i])
		members.MembershipVersion = 7
		members.Peers = map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
			"node_yz2_0": pb.ReplicaRole_kSecondary,
			"node_yz3_0": pb.ReplicaRole_kSecondary,
		}
	}

	plan = scheduler.Schedule(scheduleInput)
	te.applyPlan(plan)
	for i := range te.table.Partitions {
		members := &(te.table.Partitions[i])
		pri, sec, learner := members.DivideRoles()
		assert.Equal(t, len(pri), 1)
		assert.Equal(t, len(sec), 2)
		assert.Equal(t, len(learner), 1)
		assert.Assert(
			t,
			learner[0] == "node_yz1_0" || learner[0] == "node_yz2_0",
			"learner is %s",
			learner[0],
		)
	}

	logging.Info("restart the only primary")
	te.nodes.MustGetNodeInfo("node_yz0_0").Op = pb.AdminNodeOp_kRestart
	te.nodes.MustGetNodeInfo("node_yz1_0").Op = pb.AdminNodeOp_kNoop
	te.nodes.MustGetNodeInfo("node_yz2_0").Op = pb.AdminNodeOp_kNoop
	for i := range te.table.Partitions {
		members := &(te.table.Partitions[i])
		members.MembershipVersion = 7
		members.Peers = map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz1_0": pb.ReplicaRole_kLearner,
			"node_yz2_0": pb.ReplicaRole_kLearner,
			"node_yz3_0": pb.ReplicaRole_kLearner,
		}
	}
	plan = scheduler.Schedule(scheduleInput)
	te.applyPlan(plan)
	for i := range te.table.Partitions {
		members := &(te.table.Partitions[i])
		pri, sec, learner := members.DivideRoles()
		assert.Equal(t, len(pri), 0)
		assert.Equal(t, len(sec), 0)
		assert.Equal(t, len(learner), 4)
	}
}
