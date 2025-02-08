package server

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"gotest.tools/assert"
)

func TestPlanConductorAddLearnerCheck(t *testing.T) {
	table := actions.NewTableModelMock(&pb.Table{
		TableId:    1,
		TableName:  "test",
		HashMethod: "crc32",
		PartsCount: 4,
		BaseTable:  "",
	})
	table.Partitions[0] = table_model.PartitionMembership{
		MembershipVersion: 1,
		SplitVersion:      1,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz1_0": pb.ReplicaRole_kLearner,
		},
		RestoreVersion: map[string]int64{"node_yz0_0": 0, "node_yz1_0": 0},
	}
	table.Partitions[1] = table_model.PartitionMembership{
		MembershipVersion: 1,
		SplitVersion:      1,
		Peers:             map[string]pb.ReplicaRole{},
		RestoreVersion:    map[string]int64{},
	}
	table.Partitions[2] = table_model.PartitionMembership{
		MembershipVersion: 1,
		SplitVersion:      1,
		Peers:             map[string]pb.ReplicaRole{"node_yz0_0": pb.ReplicaRole_kPrimary},
		RestoreVersion:    map[string]int64{"node_yz0_0": 0},
	}
	table.Partitions[3] = table_model.PartitionMembership{
		MembershipVersion: 1,
		SplitVersion:      1,
		Peers:             map[string]pb.ReplicaRole{},
		RestoreVersion:    map[string]int64{},
	}

	logging.Info("test add learner disabled by per node limit")
	pc := NewPlanConductor(
		&pb.ScheduleOptions{MaxLearningPartsPerNode: -1, MaxSchedRatio: 1000},
		table,
	)
	ans := pc.TryAddLearner("test", 1, table.GetMembership(1), "node_yz0_0")
	assert.Equal(t, ans, false)

	logging.Info("test add learner disabled by global limit")
	*flagLimitAddLearnerGlobally = true
	pc = NewPlanConductor(
		&pb.ScheduleOptions{MaxLearningPartsPerNode: 1000, MaxSchedRatio: 1},
		table,
	)
	ans = pc.TryAddLearner("test", 1, table.GetMembership(1), "node_yz0_0")
	assert.Equal(t, ans, false)

	logging.Info("test add learner ok if no limit")
	*flagLimitAddLearnerGlobally = false
	pc = NewPlanConductor(
		&pb.ScheduleOptions{MaxLearningPartsPerNode: 0, MaxSchedRatio: 1000},
		table,
	)
	assert.DeepEqual(t, pc.learning, map[string]int{"node_yz1_0": 1})
	assert.DeepEqual(t, pc.learned, map[string]int{"node_yz0_0": 1})
	assert.DeepEqual(t, pc.learningParts, map[int32]bool{0: true})
	ans = pc.TryAddLearner("test", 1, table.GetMembership(1), "node_yz0_0")
	assert.Equal(t, ans, true)
	assert.DeepEqual(t, pc.learning, map[string]int{"node_yz1_0": 1, "node_yz0_0": 1})
	assert.DeepEqual(t, pc.learned, map[string]int{"node_yz0_0": 1})
	assert.DeepEqual(t, pc.learningParts, map[int32]bool{0: true, 1: true})

	logging.Info("node_yz0_0 already serve 1 leaner as primary, new leaner add will be reject")
	pc.schedOptions.MaxLearningPartsPerNode = 1
	ans = pc.TryAddLearner("test", 2, table.GetMembership(2), "node_yz2_0")
	assert.Equal(t, ans, false)

	logging.Info("node_yz1_0 already has 1 learner, new learner add will be reject")
	ans = pc.TryAddLearner("test", 3, table.GetMembership(3), "node_yz1_0")
	assert.Equal(t, ans, false)

	logging.Info("add learner pass limit")
	pc.schedOptions.MaxLearningPartsPerNode = 2
	ans = pc.TryAddLearner("test", 2, table.GetMembership(2), "node_yz2_0")
	assert.Equal(t, ans, true)
	assert.DeepEqual(
		t,
		pc.learning,
		map[string]int{"node_yz1_0": 1, "node_yz0_0": 1, "node_yz2_0": 1},
	)
	assert.DeepEqual(t, pc.learned, map[string]int{"node_yz0_0": 2})
	assert.DeepEqual(t, pc.learningParts, map[int32]bool{0: true, 1: true, 2: true})
}

func TestPlanConductorInit(t *testing.T) {
	table := actions.NewTableModelMock(&pb.Table{
		TableId:    1,
		TableName:  "test",
		HashMethod: "crc32",
		PartsCount: 4,
		BaseTable:  "",
	})
	table.Partitions[0] = table_model.PartitionMembership{
		MembershipVersion: 1,
		SplitVersion:      1,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
		},
		RestoreVersion: map[string]int64{"node_yz0_0": 0, "node_yz1_0": 0},
	}
	table.Partitions[1] = table_model.PartitionMembership{
		MembershipVersion: 1,
		SplitVersion:      1,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz1_0": pb.ReplicaRole_kLearner,
		},
		RestoreVersion: map[string]int64{"node_yz0_0": 0, "node_yz1_0": 0},
	}
	table.Partitions[2] = table_model.PartitionMembership{
		MembershipVersion: 1,
		SplitVersion:      1,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kSecondary,
			"node_yz1_0": pb.ReplicaRole_kPrimary,
		},
		RestoreVersion: map[string]int64{"node_yz0_0": 0, "node_yz1_0": 0},
	}
	table.Partitions[3] = table_model.PartitionMembership{
		MembershipVersion: 1,
		SplitVersion:      1,
		Peers: map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
			"node_yz1_0": pb.ReplicaRole_kLearner,
		},
		RestoreVersion: map[string]int64{"node_yz0_0": 0, "node_yz1_0": 0},
	}

	*flagLimitAddLearnerGlobally = true
	conductor := NewPlanConductor(&pb.ScheduleOptions{
		MaxSchedRatio:           500,
		MaxLearningPartsPerNode: 10,
	}, table)
	assert.Equal(t, conductor.maxAllowedLearningParts, int32(2))

	*flagLimitAddLearnerGlobally = false
	conductor = NewPlanConductor(
		&pb.ScheduleOptions{MaxSchedRatio: 500, MaxLearningPartsPerNode: 10},
		table,
	)
	assert.Equal(t, conductor.maxAllowedLearningParts, int32(-1))
	assert.DeepEqual(t, conductor.learning, map[string]int{"node_yz0_0": 1, "node_yz1_0": 2})
	assert.DeepEqual(t, conductor.learned, map[string]int{"node_yz0_0": 1, "node_yz1_0": 0})
	assert.DeepEqual(t, conductor.learningParts, map[int32]bool{1: true, 3: true})
}
