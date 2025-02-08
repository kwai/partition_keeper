package server

import (
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"gotest.tools/assert"
)

func TestTableSplitController(t *testing.T) {
	te := setupTestTableEnv(t, 1, 2, 1, &mockDummyStrategyNoCheckpoint{}, "", true, "zk")
	defer te.teardown()

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	plan := sched.SchedulePlan{}
	for i := int32(0); i < te.table.PartsCount; i++ {
		plan[i] = actions.MakeActions(&actions.CreateMembersAction{
			Members: table_model.PartitionMembership{
				MembershipVersion: 1,
				Peers: map[string]pb.ReplicaRole{
					"node1": pb.ReplicaRole_kPrimary,
				},
			},
		})
	}
	te.applyPlanAndUpdateFacts(plan)

	logging.Info("start split, last split second will be initialized to a past ts value")
	splitReq := &pb.SplitTableRequest{
		NewSplitVersion: 1,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 3},
	}
	ans := te.table.StartSplit(splitReq)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk), ans.Message)
	splitCtrl := te.table.splitCtrl
	assert.Assert(t, splitCtrl.lastSplitSecond+2 < time.Now().Unix())

	logging.Info("first schedule will double partitions, make first partition split")
	te.table.Schedule(true, &pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000})
	assert.Equal(t, te.table.PartsCount, int32(4))
	assert.Equal(t, len(te.table.currParts[2].members.Peers), 1)
	te.updateFactsFromMembers()
	assert.Assert(t, splitCtrl.lastSplitSecond+3 > time.Now().Unix())

	logging.Info("next 2 schedule will make part3 elect leader, so first split finished")
	te.table.Schedule(true, &pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000})
	te.updateFactsFromMembers()
	te.table.Schedule(true, &pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000})
	te.updateFactsFromMembers()
	assert.Equal(t, te.table.currParts[0].members.SplitVersion, int32(1))
	assert.Equal(t, te.table.currParts[2].members.SplitVersion, int32(1))

	logging.Info("next schedule won't have plan as split will be delayed")
	te.table.Schedule(true, &pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000})
	te.updateFactsFromMembers()
	assert.Equal(t, len(te.table.currParts[3].members.Peers), 0)

	logging.Info("wait a while until split can continue")
	time.Sleep(time.Second * 3)
	te.table.Schedule(true, &pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000})
	te.updateFactsFromMembers()
	assert.Equal(t, len(te.table.currParts[3].members.Peers), 1)
}
