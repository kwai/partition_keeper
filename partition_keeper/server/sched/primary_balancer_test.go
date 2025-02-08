package sched

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"gotest.tools/assert"
)

func TestPrimaryBalancerBuildNetwork(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 2, []int{1, 2}, 4)
	balancer := &primaryBalancer{}
	balancer.SetLogName("test-service")
	plan := SchedulePlan{}

	logging.Info("can't build network if no primary")
	te.table.GetMembership(int32(0)).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kSecondary,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	te.table.GetMembership(int32(1)).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kSecondary,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	te.table.GetMembership(int32(2)).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kSecondary,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	te.table.GetMembership(int32(3)).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kSecondary,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	te.updateRecorder()
	balancer.Initialize(te.getScheduleInput(), te.nr, plan)
	ans := balancer.buildNetwork(te.nr)
	assert.Assert(t, ans == nil)

	logging.Info("can't build network if node has invalid score")
	te.table.GetMembership(int32(0)).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kPrimary,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	te.table.GetMembership(int32(1)).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kPrimary,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	te.table.GetMembership(int32(2)).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kPrimary,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	te.table.GetMembership(int32(3)).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kPrimary,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	te.updateRecorder()
	te.nodes.MustGetNodeInfo("node_yz0_0").Score = 0
	balancer.Initialize(te.getScheduleInput(), te.nr, plan)
	ans = balancer.buildNetwork(te.nr)
	assert.Assert(t, ans == nil)

	logging.Info("can't build network if total scores are 0")
	te.nodes.MustGetNodeInfo("node_yz0_0").Score = 1
	te.nodes.MustGetNodeInfo("node_yz0_0").Op = pb.AdminNodeOp_kRestart
	te.nodes.MustGetNodeInfo("node_yz1_0").Op = pb.AdminNodeOp_kRestart
	te.nodes.MustGetNodeInfo("node_yz1_1").Op = pb.AdminNodeOp_kRestart
	balancer.Initialize(te.getScheduleInput(), te.nr, plan)
	ans = balancer.buildNetwork(te.nr)
	assert.Assert(t, ans == nil)

	logging.Info("build network ok")
	te.nodes.MustGetNodeInfo("node_yz0_0").Op = pb.AdminNodeOp_kNoop
	te.nodes.MustGetNodeInfo("node_yz1_0").Op = pb.AdminNodeOp_kNoop
	te.nodes.MustGetNodeInfo("node_yz1_1").Op = pb.AdminNodeOp_kNoop
	te.nodes.MustGetNodeInfo("node_yz0_0").Score = 2
	te.nodes.MustGetNodeInfo("node_yz1_0").Score = 1
	te.nodes.MustGetNodeInfo("node_yz1_1").Score = 1
	te.table.GetMembership(int32(0)).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kPrimary,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	te.table.GetMembership(int32(1)).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kPrimary,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	te.table.GetMembership(int32(2)).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kPrimary,
		"node_yz1_1": pb.ReplicaRole_kSecondary,
	}
	te.table.GetMembership(int32(3)).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kPrimary,
		"node_yz1_1": pb.ReplicaRole_kSecondary,
	}
	te.updateRecorder()
	balancer.Initialize(te.getScheduleInput(), te.nr, plan)
	ans = balancer.buildNetwork(te.nr)
	assert.Assert(t, ans != nil)
	assert.Equal(t, ans[kSourceNode]["node_yz0_0"], 2)
	assert.Equal(t, ans["node_yz1_0"][kSinkNode], 1)
	assert.Equal(t, ans["node_yz1_1"][kSinkNode], 1)
}
