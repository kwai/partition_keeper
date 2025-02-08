package server

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"gotest.tools/assert"
)

func TestPartitionSplitCleanup(t *testing.T) {
	te := setupTestTableEnv(t, 2, 1, 2, &mockDummyStrategyNoCheckpoint{}, "", true, "zk")
	defer te.teardown()

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	plan := sched.SchedulePlan{}
	plan[int32(0)] = actions.MakeActions(&actions.CreateMembersAction{
		Members: table_model.PartitionMembership{
			MembershipVersion: 1,
			Peers: map[string]pb.ReplicaRole{
				"node1": pb.ReplicaRole_kPrimary,
				"node2": pb.ReplicaRole_kSecondary,
			},
		},
	})
	te.applyPlanAndUpdateFacts(plan)

	splitReq := &pb.SplitTableRequest{
		NewSplitVersion: 1,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1},
	}
	ans := te.table.StartSplit(splitReq)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	logging.Info("prepare: first scheduler will create new partition")
	te.table.Schedule(true, &pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000})
	assert.Equal(t, te.table.PartsCount, int32(2))
	assert.Equal(t, len(te.table.currParts[1].members.Peers), 2)
	te.updateFactsFromMembers()
	part1, part2 := &(te.table.currParts[0]), &(te.table.currParts[1])

	logging.Info("prepare: next 2 schedules will make new partition promote to secondary")
	te.table.Schedule(true, &pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000})
	te.updateFactsFromMembers()
	te.table.Schedule(true, &pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000})
	te.updateFactsFromMembers()
	assert.Equal(t, part2.members.GetMember("node1"), pb.ReplicaRole_kSecondary)
	assert.Equal(t, part2.members.GetMember("node2"), pb.ReplicaRole_kSecondary)
	assert.Equal(t, part1.members.SplitVersion, int32(0))
	assert.Equal(t, part2.members.SplitVersion, int32(0))

	logging.Info("prepare: next schedule will make child elect leader, and split version increased")
	te.table.Schedule(true, &pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000})
	te.updateFactsFromMembers()
	pri, _, _ := part2.members.DivideRoles()
	assert.Equal(t, len(pri), 1)
	assert.Equal(t, part1.members.SplitVersion, int32(1))
	assert.Equal(t, part2.members.SplitVersion, int32(1))

	logging.Info("facts are updated, split cleanup will be finished")
	assert.Equal(t, part1.doSplitCleanup(), true)
	assert.Equal(t, part2.doSplitCleanup(), true)

	node1Client := te.lpb.GetOrNewClient("127.0.0.1:1001")
	node2Client := te.lpb.GetOrNewClient("127.0.0.2:1001")

	logging.Info("node2 mark no facts collected, so unfinished globally")
	node1Client.CleanMetrics()
	node2Client.CleanMetrics()
	part2.facts["node2"] = nil
	assert.Equal(t, part2.doSplitCleanup(), false)
	assert.Equal(t, node2Client.GetMetric("replica_split_cleanup"), 0)

	logging.Info("node2 mark split cleanup version smaller, so unfinished globally")
	te.updateFactsFromMembers()
	node1Client.CleanMetrics()
	node2Client.CleanMetrics()
	part2.facts["node2"].SplitCleanupVersion = 0
	assert.Equal(t, part2.doSplitCleanup(), false)
	assert.Equal(t, node2Client.GetMetric("replica_split_cleanup"), 1)

	logging.Info("mark node2 dead, split cleanup will finish globally")
	te.updateFactsFromMembers()
	te.nodeStats.MustGetNodeInfo("node2").IsAlive = false
	part1.members.Peers = map[string]pb.ReplicaRole{
		"node2": pb.ReplicaRole_kPrimary,
	}
	assert.Equal(t, part1.doSplitCleanup(), false)

	assert.Equal(t, part2.doSplitCleanup(), true)
	te.nodeStats.MustGetNodeInfo("node1").IsAlive = false
	assert.Equal(t, part2.doSplitCleanup(), false)
}

func TestIfOldParentChangePrimary(t *testing.T) {
	te := setupTestTableEnv(t, 2, 4, 2, &mockDummyStrategyNoCheckpoint{}, "", true, "zk")
	defer te.teardown()

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	plan := sched.SchedulePlan{}
	for i := 0; i < 4; i++ {
		plan[int32(i)] = actions.MakeActions(&actions.CreateMembersAction{
			Members: table_model.PartitionMembership{
				MembershipVersion: 1,
				Peers: map[string]pb.ReplicaRole{
					"node1": pb.ReplicaRole_kPrimary,
					"node2": pb.ReplicaRole_kSecondary,
				},
			},
		})
	}
	te.applyPlanAndUpdateFacts(plan)

	splitReq := &pb.SplitTableRequest{
		NewSplitVersion: 1,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 1},
	}
	ans := te.table.StartSplit(splitReq)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	te.table.Schedule(true, &pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000})

	part1 := &(te.table.currParts[0])
	part2 := &(te.table.currParts[4])
	assert.Equal(t, part1.oldParentChangePrimary(), false)
	assert.Equal(t, part2.oldParentChangePrimary(), false)

	part1.membershipChanged = true
	part1.newMembers = table_model.PartitionMembership{
		MembershipVersion: 2,
		Peers: map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kSecondary,
			"node2": pb.ReplicaRole_kSecondary,
		},
	}
	assert.Equal(t, part1.oldParentChangePrimary(), true)

	part1.members = table_model.PartitionMembership{
		MembershipVersion: 2,
		Peers: map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kSecondary,
			"node2": pb.ReplicaRole_kSecondary,
		},
	}
	assert.Equal(t, part1.oldParentChangePrimary(), false)

	part1.members = table_model.PartitionMembership{
		MembershipVersion: 2,
		Peers: map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kPrimary,
			"node2": pb.ReplicaRole_kSecondary,
		},
	}
	part1.newMembers = table_model.PartitionMembership{
		MembershipVersion: 2,
		Peers: map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kSecondary,
			"node2": pb.ReplicaRole_kPrimary,
		},
	}
	assert.Equal(t, part1.oldParentChangePrimary(), true)
}
