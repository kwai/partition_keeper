package sched

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"gotest.tools/assert"
)

func TestEnableSplitBalancerFlag(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 1, []int{2}, 4)
	balancer := &adaptivePartitionBalancer{}

	te.table.Table.SplitVersion = 1
	balancer.SetLogName("test-service")
	logging.Info("don't schedule if split balancer is disabled")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
		},
		SplitVersion: 0,
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
		},
		SplitVersion: 0,
	}
	te.updateRecorder()
	input := te.getScheduleInput()
	plan := SchedulePlan{}
	balancer.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 0)

	logging.Info("do balance if split balancer is enabled")
	te.opts.Configs.EnableSplitBalancer = true
	input = te.getScheduleInput()
	balancer.Schedule(input, te.nr, plan)
	assert.Assert(t, len(plan) > 0)
}

func TestPartitionBalancerConditionCheck(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 2, []int{2, 2}, 2)
	balancer := &adaptivePartitionBalancer{}
	balancer.SetLogName("test-service")

	logging.Info("don't schedule if already has plan")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
		},
	}
	te.updateRecorder()
	input := te.getScheduleInput()
	input.Hubs = []*pb.ReplicaHub{
		{Name: "yz0", Az: "YZ"},
	}
	plan := SchedulePlan{
		0: actions.MakeActions(
			&actions.TransformAction{Node: "node_yz0_0", ToRole: pb.ReplicaRole_kSecondary},
		),
	}
	balancer.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	act := plan[0].GetAction(0).(*actions.TransformAction)
	assert.Equal(t, act.Node, "node_yz0_0")
	assert.Equal(t, act.ToRole, pb.ReplicaRole_kSecondary)

	logging.Info("don't schedule if has removed hub")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
			"node_yz1_0": pb.ReplicaRole_kLearner,
		},
	}
	te.updateRecorder()
	plan = SchedulePlan{}
	balancer.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 0)

	logging.Info("don't schedule if miss active hub nodes")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers:             map[string]pb.ReplicaRole{},
	}
	te.updateRecorder()
	plan = SchedulePlan{}
	balancer.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 0)

	logging.Info("don't schedule if has redudant nodes in active hubs")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
			"node_yz0_1": pb.ReplicaRole_kLearner,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
		},
	}
	te.updateRecorder()
	plan = SchedulePlan{}
	balancer.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 0)

	logging.Info("run schedule")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
		},
	}
	te.updateRecorder()
	plan = SchedulePlan{}
	balancer.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
}

func TestNoBalanceIfWeightedScoreNotReady(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 2, []int{2, 2}, 4)

	te.nodes.MustGetNodeInfo("node_yz0_0").Score = 0
	for i := 0; i < 4; i++ {
		te.table.GetMembership(int32(i)).Peers = map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
			"node_yz1_0": pb.ReplicaRole_kLearner,
		}
	}

	te.updateRecorder()
	balancer := &adaptivePartitionBalancer{}
	balancer.SetLogName("test-service")
	plan := SchedulePlan{}
	balancer.Schedule(te.getScheduleInput(), te.nr, plan)
	assert.Assert(t, te.applyPlan(plan))

	te.updateRecorder()
	plan = SchedulePlan{}
	balancer.Schedule(te.getScheduleInput(), te.nr, plan)
	assert.Equal(t, len(plan), 0)

	assert.Equal(t, te.nr.CountAll("node_yz0_0"), 4)
	assert.Equal(t, te.nr.CountAll("node_yz0_1"), 0)
	assert.Equal(t, te.nr.CountAll("node_yz1_0"), 2)
	assert.Equal(t, te.nr.CountAll("node_yz1_1"), 2)
}
