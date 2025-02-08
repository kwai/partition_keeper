package sched

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"gotest.tools/assert"
)

func TestAdaptivePrimaryBadCase(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 1, []int{2}, 2)
	inspector := &adaptivePrimaryInspector{}
	inspector.SetLogName("test-service")

	logging.Info("p0 skip schedule coz existing plan, p1 schedule to primary")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
		},
	}

	input := te.getScheduleInput()
	plan := SchedulePlan{}
	plan[0] = actions.FluentRemoveNode("node_yz0_0")
	te.updateRecorder()
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 2)
	te.applyPlan(plan)
	assert.Equal(t, len(te.table.GetMembership(0).Peers), 0)
	assert.Equal(t, te.table.GetMembership(1).GetMember("node_yz0_0"), pb.ReplicaRole_kPrimary)

	logging.Info("skip scheduler if hub empty")
	*te.table.GetMembership(0) = table_model.PartitionMembership{}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
		},
	}
	input.Hubs = []*pb.ReplicaHub{}
	plan = SchedulePlan{}
	te.updateRecorder()
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 0)

	logging.Info("p1 will downgrade primary if prepare restore")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
		},
	}
	input = te.getScheduleInput()
	input.Opts.PrepareRestoring = true
	plan = SchedulePlan{}
	te.updateRecorder()
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	assert.Assert(t, plan[0] != nil)
	te.applyPlan(plan)
	assert.Equal(t, te.table.GetMembership(0).GetMember("node_yz0_0"), pb.ReplicaRole_kSecondary)
	assert.Equal(t, te.table.GetMembership(1).GetMember("node_yz0_0"), pb.ReplicaRole_kSecondary)

	logging.Info("p0 can't promote secondary as all learners")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
		},
	}
	input = te.getScheduleInput()
	input.Opts.PrepareRestoring = false
	plan = SchedulePlan{}
	te.updateRecorder()
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	assert.Assert(t, plan[1] != nil)
	te.applyPlan(plan)
	assert.Equal(t, te.table.GetMembership(0).GetMember("node_yz0_0"), pb.ReplicaRole_kLearner)
	assert.Equal(t, te.table.GetMembership(1).GetMember("node_yz0_0"), pb.ReplicaRole_kPrimary)

	logging.Info("p0 can't promote as state not normal")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_1": pb.ReplicaRole_kSecondary,
		},
	}
	te.nodes.MustGetNodeInfo("node_yz0_0").IsAlive = false
	input = te.getScheduleInput()
	input.Opts.PrepareRestoring = false
	plan = SchedulePlan{}
	te.updateRecorder()
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	assert.Assert(t, plan[1] != nil)
	te.applyPlan(plan)
	assert.Equal(t, te.table.GetMembership(0).GetMember("node_yz0_0"), pb.ReplicaRole_kSecondary)
	assert.Equal(t, te.table.GetMembership(1).GetMember("node_yz0_1"), pb.ReplicaRole_kPrimary)
}

func TestPrimarySelectPriority(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 3, []int{1, 1, 1}, 2)
	inspector := &adaptivePrimaryInspector{}
	inspector.SetLogName("test-service")

	logging.Info("select node1 as it's normal")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 0,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
			"node_yz2_0": pb.ReplicaRole_kLearner,
		},
	}
	te.nodes.MustGetNodeInfo("node_yz1_0").IsAlive = false
	input := te.getScheduleInput()
	plan := SchedulePlan{}
	te.updateRecorder()
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.Equal(t, te.table.GetMembership(0).GetMember("node_yz0_0"), pb.ReplicaRole_kPrimary)
	// reset args
	te.nodes.MustGetNodeInfo("node_yz1_0").IsAlive = true

	logging.Info("select node3 as hub1 and hub2 disallow to have primary")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
			"node_yz2_0": pb.ReplicaRole_kSecondary,
		},
	}
	te.updateRecorder()
	input.Hubs[0].DisallowedRoles = []pb.ReplicaRole{pb.ReplicaRole_kPrimary}
	input.Hubs[1].DisallowedRoles = []pb.ReplicaRole{pb.ReplicaRole_kPrimary}
	plan = SchedulePlan{}
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.Equal(t, te.table.GetMembership(0).GetMember("node_yz2_0"), pb.ReplicaRole_kPrimary)
	// reset args
	input.Hubs[0].DisallowedRoles = nil
	input.Hubs[1].DisallowedRoles = nil

	logging.Info("select node1 as it has minimal primary")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 0,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
			"node_yz2_0": pb.ReplicaRole_kLearner,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 0,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kPrimary,
			"node_yz2_0": pb.ReplicaRole_kSecondary,
		},
	}
	te.updateRecorder()
	plan = SchedulePlan{}
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.Equal(t, te.table.GetMembership(0).GetMember("node_yz0_0"), pb.ReplicaRole_kPrimary)

	logging.Info("select node2 as it has less replicas")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 0,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
			"node_yz2_0": pb.ReplicaRole_kLearner,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 0,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz2_0": pb.ReplicaRole_kPrimary,
		},
	}
	te.updateRecorder()
	plan = SchedulePlan{}
	inspector.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.Equal(t, te.table.GetMembership(0).GetMember("node_yz1_0"), pb.ReplicaRole_kPrimary)
}
