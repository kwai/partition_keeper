package sched

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"gotest.tools/assert"
)

func TestHashGroupScheduler(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 2, []int{4, 8}, 32)
	input := te.getScheduleInput()
	input.Opts.PartGroupCnt = 8

	scheduler := NewScheduler(HASH_GROUP_SCHEDULER, "test-service")

	logging.Info("first schedule will add nodes to yz0")
	plan := scheduler.Schedule(input)
	te.applyPlan(plan)
	for i := int32(0); i < te.table.GetPartsCount(); i++ {
		assert.Equal(t, len(te.table.GetMembership(i).Peers), 1)
		for node, role := range te.table.GetMembership(i).Peers {
			assert.Equal(t, role, pb.ReplicaRole_kLearner)
			assert.Equal(t, te.nodes.MustGetNodeInfo(node).Hub, "yz0")
		}
	}

	logging.Info("then promote nodes to secondary")
	plan = SchedulePlan{}
	for i := int32(0); i < te.table.GetPartsCount(); i++ {
		_, _, l := te.table.GetMembership(i).DivideRoles()
		plan[i] = actions.MakeActions(
			&actions.TransformAction{Node: l[0], ToRole: pb.ReplicaRole_kSecondary},
		)
	}
	te.applyPlan(plan)

	logging.Info("next schedule will do primary promotion")
	plan = scheduler.Schedule(input)
	te.applyPlan(plan)
	for i := int32(0); i < te.table.GetPartsCount(); i++ {
		p, s, l := te.table.GetMembership(i).DivideRoles()
		assert.Equal(t, len(p), 1)
		assert.Equal(t, len(s), 0)
		assert.Equal(t, len(l), 0)
	}

	logging.Info("next schedule will do yz1 add learner")
	plan = scheduler.Schedule(input)
	te.applyPlan(plan)
	for i := int32(0); i < te.table.GetPartsCount(); i++ {
		p, s, l := te.table.GetMembership(i).DivideRoles()
		assert.Equal(t, len(p), 1)
		assert.Equal(t, len(s), 0)
		assert.Equal(t, len(l), 1)
		assert.Equal(t, te.nodes.MustGetNodeInfo(p[0]).Hub, "yz0")
		assert.Equal(t, te.nodes.MustGetNodeInfo(l[0]).Hub, "yz1")
	}

	logging.Info("then promote some learners to sec")
	plan = SchedulePlan{}
	for i := int32(0); i < te.table.GetPartsCount()-2; i++ {
		_, _, l := te.table.GetMembership(i).DivideRoles()
		plan[i] = actions.MakeActions(
			&actions.TransformAction{Node: l[0], ToRole: pb.ReplicaRole_kSecondary},
		)
	}
	te.applyPlan(plan)

	logging.Info("next will do primary balancer")
	plan = scheduler.Schedule(input)
	te.applyPlan(plan)
	yz0_primary_count := 0
	for i := int32(0); i < te.table.GetPartsCount(); i++ {
		p, _, _ := te.table.GetMembership(i).DivideRoles()
		if te.nodes.MustGetNodeInfo(p[0]).Hub == "yz0" {
			yz0_primary_count++
		}
	}
	assert.Assert(t, yz0_primary_count < 32)
}
