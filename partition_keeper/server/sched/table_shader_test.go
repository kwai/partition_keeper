package sched

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"gotest.tools/assert"
)

func TestComparePrimary(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 2, []int{1, 1}, 4)
	baseTable := actions.NewTableModelMock(&pb.Table{
		TableId:    2,
		TableName:  "test_base_table",
		PartsCount: 4,
	})
	*baseTable.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 1,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
		},
	}
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 1,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kPrimary,
		},
	}

	*baseTable.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 1,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
		},
	}
	*te.table.GetMembership(1) = table_model.PartitionMembership{
		MembershipVersion: 1,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kPrimary,
		},
	}

	*baseTable.GetMembership(2) = table_model.PartitionMembership{
		MembershipVersion: 1,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
		},
	}
	*te.table.GetMembership(2) = table_model.PartitionMembership{
		MembershipVersion: 1,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
		},
	}

	*baseTable.GetMembership(3) = table_model.PartitionMembership{
		MembershipVersion: 1,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
		},
	}
	*te.table.GetMembership(3) = table_model.PartitionMembership{
		MembershipVersion: 1,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
		},
	}

	shader := newTableShader(baseTable, true)
	te.updateRecorder()
	plan := SchedulePlan{}
	plan[0] = actions.MakeActions(
		&actions.TransformAction{Node: "node_yz0_0", ToRole: pb.ReplicaRole_kLearner},
	)
	shader.Schedule(te.getScheduleInput(), te.nr, plan)
	assert.Equal(t, len(plan), 3)
	assert.Assert(t, plan[3] == nil)

	te.applyPlan(plan)
	assert.Equal(t, te.table.GetMembership(0).GetMember("node_yz1_0"), pb.ReplicaRole_kPrimary)
	assert.DeepEqual(
		t,
		te.table.GetMembership(1).Peers,
		map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
		},
	)
	assert.DeepEqual(
		t,
		te.table.GetMembership(2).Peers,
		map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
		},
	)
	assert.Equal(t, te.table.GetMembership(3).MembershipVersion, int64(1))
	assert.DeepEqual(
		t,
		te.table.GetMembership(3).Peers,
		map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
		},
	)
}

func TestCompareHubs(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 2, []int{2, 2}, 1)
	baseTable := actions.NewTableModelMock(&pb.Table{
		TableId:    2,
		TableName:  "test_base_table",
		PartsCount: 1,
	})
	baseTable.GetMembership(0).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kPrimary,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	te.table.GetMembership(0).Peers = map[string]pb.ReplicaRole{
		"node_yz0_1": pb.ReplicaRole_kPrimary,
		"node_yz1_1": pb.ReplicaRole_kSecondary,
	}
	shader := newTableShader(baseTable, false)
	te.updateRecorder()
	plan := SchedulePlan{}

	// first learner will be added
	shader.Schedule(te.getScheduleInput(), te.nr, plan)
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		"node_yz0_1": pb.ReplicaRole_kPrimary,
		"node_yz1_1": pb.ReplicaRole_kSecondary,
		"node_yz0_0": pb.ReplicaRole_kLearner,
	})

	// then no action will be taken
	te.updateRecorder()
	plan = SchedulePlan{}
	shader.Schedule(te.getScheduleInput(), te.nr, plan)
	assert.Equal(t, len(plan), 0)

	// then promote all learner, and try again
	te.promoteAllLearners()
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		"node_yz0_1": pb.ReplicaRole_kPrimary,
		"node_yz1_1": pb.ReplicaRole_kSecondary,
		"node_yz0_0": pb.ReplicaRole_kSecondary,
	})
	te.updateRecorder()
	shader.Schedule(te.getScheduleInput(), te.nr, plan)
	assert.Equal(t, plan[0].ActionCount(), 2)
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		"node_yz1_1": pb.ReplicaRole_kSecondary,
		"node_yz0_0": pb.ReplicaRole_kSecondary,
	})

	// then next hub
	te.updateRecorder()
	plan = SchedulePlan{}
	shader.Schedule(te.getScheduleInput(), te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		"node_yz1_1": pb.ReplicaRole_kSecondary,
		"node_yz0_0": pb.ReplicaRole_kSecondary,
		"node_yz1_0": pb.ReplicaRole_kLearner,
	})

	// test remove extra hubs
	te.updateRecorder()
	plan = SchedulePlan{}
	input := te.getScheduleInput()
	input.Hubs = input.Hubs[1:]
	shader.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		"node_yz1_1": pb.ReplicaRole_kSecondary,
		"node_yz1_0": pb.ReplicaRole_kLearner,
	})
}
