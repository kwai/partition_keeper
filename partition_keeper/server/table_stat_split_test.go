package server

import (
	"context"
	"strings"
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/checkpoint"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

type mockDummyStrategyNoCheckpoint struct {
	base.DummyStrategy
}

func (m *mockDummyStrategyNoCheckpoint) CreateCheckpointByDefault() bool {
	return false
}

func TestSomeSchedulerDisallowSplit(t *testing.T) {
	args := `{
		"msg_queue_shards": 2
}`
	te := setupTestTableEnv(
		t,
		1,
		4,
		1,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		args,
		true,
		"zk",
	)
	defer te.teardown()

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	req := &pb.SplitTableRequest{
		NewSplitVersion: 1,
		Options: &pb.SplitTableOptions{
			MaxConcurrentParts: 1,
			DelaySeconds:       1,
		},
	}
	ans := te.table.StartSplit(req)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Assert(t, strings.Contains(ans.Message, "doesn't support"))
}

func TestStartSplit(t *testing.T) {
	te := setupTestTableEnv(
		t,
		1,
		1,
		1,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	logging.Info("invalid split options")
	ans := te.table.StartSplit(&pb.SplitTableRequest{
		NewSplitVersion: 1,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 0, DelaySeconds: 0},
	})
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Assert(t, strings.Contains(ans.Message, "concurrent parts"))

	logging.Info("restore exists")
	te.table.restoreCtrl = NewTableRestoreController(te.table, te.table.getRestoreCtrlPath())
	ans = te.table.StartSplit(&pb.SplitTableRequest{
		NewSplitVersion: 1,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 0},
	})
	assert.Equal(t, ans.Code, int32(pb.AdminError_kWorkingInprogress))
	assert.Assert(t, strings.Contains(ans.Message, "running restore"))

	logging.Info("task not paused")
	te.table.restoreCtrl = nil
	ans = te.table.StartSplit(&pb.SplitTableRequest{
		NewSplitVersion: 1,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 0},
	})
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Assert(t, strings.Contains(ans.Message, "pause it"))

	logging.Info("table not fully initialized")
	checkpointTask := te.table.tasks[checkpoint.TASK_NAME]
	queryTaskResp := &pb.QueryTaskResponse{}
	checkpointTask.QueryTask(queryTaskResp)
	assert.Equal(t, queryTaskResp.Task.Paused, false)
	queryTaskResp.Task.Paused = true
	ans = checkpointTask.UpdateTask(queryTaskResp.Task)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	ans = te.table.StartSplit(&pb.SplitTableRequest{
		NewSplitVersion: 2,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 0},
	})
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Assert(t, strings.Contains(ans.Message, "fully initialized"))

	logging.Info("invalid split version")
	plan := sched.SchedulePlan{}
	plan[0] = actions.MakeActions(&actions.AddLearnerAction{Node: "node1"})
	te.applyPlanAndUpdateFacts(plan)

	ans = te.table.StartSplit(&pb.SplitTableRequest{
		NewSplitVersion: 2,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 0},
	})
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Assert(t, strings.Contains(ans.Message, "split version"))

	logging.Info("create split controller succeed")
	ans = te.table.StartSplit(&pb.SplitTableRequest{
		NewSplitVersion: 1,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 0},
	})
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, te.table.splitCtrl != nil)

	logging.Info("update split controller")
	ans = te.table.StartSplit(&pb.SplitTableRequest{
		NewSplitVersion: 1,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 2},
	})
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, te.table.splitCtrl.stableInfo.DelaySeconds, int32(2))
}

func TestReloadSplitCtrl(t *testing.T) {
	st := &mockDummyStrategyNoCheckpoint{}
	te := setupTestTableEnv(
		t, 1, 2, 1, st, "", true, "zk",
	)
	defer te.teardown()
	te.lock.LockWrite()
	defer te.lock.UnlockWrite()

	te.table.Stop(true)
	te.table = NewTableStats(
		"test",
		"test",
		false,
		te.lock,
		te.nodeStats,
		te.hubs,
		st,
		te.lpb.Build(),
		te.zkStore,
		te.delayExecutor,
		te.testZkRoot,
		nil,
	)
	te.table.LoadFromZookeeper("test")
	assert.Assert(t, te.table.splitCtrl == nil)

	plan := sched.SchedulePlan{}
	for i := int32(0); i < te.table.PartsCount; i++ {
		targetNode := "node1"
		action := actions.CreateMembersAction{
			Members: table_model.PartitionMembership{
				MembershipVersion: 1,
				Peers: map[string]pb.ReplicaRole{
					targetNode: pb.ReplicaRole_kPrimary,
				},
			},
		}
		plan[i] = actions.MakeActions(&action)
	}
	te.applyPlanAndUpdateFacts(plan)

	ans := te.table.StartSplit(&pb.SplitTableRequest{
		NewSplitVersion: 1,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 1},
	})
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	logging.Info("first run a full round of split, so split controller will be cleaned")
	for te.table.splitCtrl != nil {
		te.table.Schedule(
			true,
			&pb.ScheduleOptions{EnablePrimaryScheduler: true, MaxSchedRatio: 1000},
		)
		te.updateFactsFromMembers()
	}

	logging.Info("reload table")
	te.table.Stop(true)
	te.table = NewTableStats(
		"test",
		"test",
		false,
		te.lock,
		te.nodeStats,
		te.hubs,
		st,
		te.lpb.Build(),
		te.zkStore,
		te.delayExecutor,
		te.testZkRoot,
		nil,
	)
	te.table.LoadFromZookeeper("test")
	assert.Assert(t, te.table.splitCtrl == nil)
	assert.Equal(t, te.table.SplitVersion, int32(1))
	for i := int32(0); i < te.table.PartsCount; i++ {
		assert.Equal(t, te.table.currParts[i].members.SplitVersion, int32(1))
	}

	logging.Info("start a new round of split")
	ans = te.table.StartSplit(&pb.SplitTableRequest{
		NewSplitVersion: 2,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 1},
	})
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	logging.Info("reload table again")
	te.table.Stop(true)
	te.table = NewTableStats(
		"test",
		"test",
		false,
		te.lock,
		te.nodeStats,
		te.hubs,
		st,
		te.lpb.Build(),
		te.zkStore,
		te.delayExecutor,
		te.testZkRoot,
		nil,
	)
	te.table.LoadFromZookeeper("test")
	assert.Equal(t, te.table.SplitVersion, int32(1))
	assert.Assert(t, te.table.splitCtrl != nil)
	assert.Equal(t, te.table.splitCtrl.stableInfo.NewSplitVersion, int32(2))

	logging.Info("split 1 partition")
	for te.table.currParts[0].members.SplitVersion < 2 {
		te.table.Schedule(
			true,
			&pb.ScheduleOptions{EnablePrimaryScheduler: true, MaxSchedRatio: 1000},
		)
		te.updateFactsFromMembers()
	}

	logging.Info("reload table")
	te.table.Stop(true)
	te.table = NewTableStats(
		"test",
		"test",
		false,
		te.lock,
		te.nodeStats,
		te.hubs,
		st,
		te.lpb.Build(),
		te.zkStore,
		te.delayExecutor,
		te.testZkRoot,
		nil,
	)
	te.table.LoadFromZookeeper("test")
	assert.Equal(t, te.table.SplitVersion, int32(2))
	assert.Assert(t, te.table.splitCtrl != nil)
	assert.Assert(t, te.table.currParts[4].hasZkPath)
	assert.Equal(t, te.table.currParts[4].members.SplitVersion, int32(2))
	for i := 5; i < 8; i++ {
		assert.Equal(t, te.table.currParts[i].hasZkPath, false)
		assert.Equal(t, te.table.currParts[i].members.SplitVersion, int32(1))
	}

	logging.Info("finish split on reloaded table")
	for te.table.splitCtrl != nil {
		te.table.Schedule(
			true,
			&pb.ScheduleOptions{EnablePrimaryScheduler: true, MaxSchedRatio: 1000},
		)
		te.updateFactsFromMembers()
	}
}

func TestSplitSchedule(t *testing.T) {
	// if split:
	//  1. partition count and split version will increased
	//  2. new partitions will be created, new partitions starts with all learners
	//  3. split request will be sent, parents are properly set
	//  4. if child primary elected, then parent's &
	//     child's split version will increase, and write to remote in a batch
	//  5. split cleanup will be sent, request fields are as expected
	te := setupTestTableEnv(
		t,
		1,
		4,
		1,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		"",
		true,
		"zk",
	)
	defer te.teardown()

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	plan := sched.SchedulePlan{}
	for i := int32(0); i < te.table.PartsCount; i++ {
		targetNode := "node1"
		action := actions.CreateMembersAction{
			Members: table_model.PartitionMembership{
				MembershipVersion: 1,
				Peers: map[string]pb.ReplicaRole{
					targetNode: pb.ReplicaRole_kPrimary,
				},
			},
		}
		plan[i] = actions.MakeActions(&action)
	}
	te.applyPlanAndUpdateFacts(plan)

	for i := int32(0); i < te.table.PartsCount; i++ {
		assert.Equal(t, te.table.PartSchedulable(i), true)
		assert.Equal(t, te.table.GetPartWeight(i), 1)
	}

	logging.Info("pause tasks")
	checkpointTask := te.table.tasks[checkpoint.TASK_NAME]
	queryTaskResp := &pb.QueryTaskResponse{}
	checkpointTask.QueryTask(queryTaskResp)
	assert.Equal(t, queryTaskResp.Task.Paused, false)
	queryTaskResp.Task.Paused = true
	ans := checkpointTask.UpdateTask(queryTaskResp.Task)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	logging.Info("create split controller")
	ans = te.table.StartSplit(&pb.SplitTableRequest{
		NewSplitVersion: 1,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 0},
	})
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, te.table.Table.SplitVersion, int32(0))
	assert.Equal(t, te.table.Table.PartsCount, int32(4))
	client1 := te.lpb.GetOrNewClient("127.0.0.1:1001")

	logging.Info("first round of schedule: table meta updated, first partition split")
	te.table.Schedule(true, &pb.ScheduleOptions{
		EnablePrimaryScheduler: true,
		MaxSchedRatio:          1000,
	})

	assert.Equal(t, te.table.PartsCount, int32(8))
	assert.Equal(t, len(te.table.currParts), 8)
	assert.Equal(t, te.table.SplitVersion, int32(1))
	for pid := 4; pid < 8; pid++ {
		assert.Equal(t, te.table.currParts[pid].members.SplitVersion, int32(0))
	}

	data, exists, succ := te.table.zkConn.Get(context.Background(), te.table.zkPath)
	assert.Assert(t, succ && exists)
	tablePb := &pb.Table{}
	utils.UnmarshalJsonOrDie(data, tablePb)
	assert.Equal(t, tablePb.SplitVersion, int32(1))
	assert.Equal(t, tablePb.PartsCount, int32(8))

	for pid := 4; pid < 8; pid++ {
		curChild := &(te.table.currParts[pid])
		curParent := &(te.table.currParts[pid-4])

		if pid > 4 {
			te.table.Schedule(true, &pb.ScheduleOptions{
				EnablePrimaryScheduler: true,
				MaxSchedRatio:          1000,
			})
		}

		logging.Info("test partition split schedule property")
		for i := 4; i < pid; i++ {
			assert.Equal(t, te.table.GetPartWeight(int32(i-4)), 1)
			assert.Equal(t, te.table.PartSchedulable(int32(i-4)), true)
			assert.Equal(t, te.table.GetPartWeight(int32(i)), 1)
			assert.Equal(t, te.table.PartSchedulable(int32(i)), true)
		}
		for i := pid + 1; i < 8; i++ {
			assert.Equal(t, te.table.GetPartWeight(int32(i-4)), 2)
			assert.Equal(t, te.table.PartSchedulable(int32(i-4)), true)
			assert.Equal(t, te.table.GetPartWeight(int32(i)), 0)
			assert.Equal(t, te.table.PartSchedulable(int32(i)), false)
		}

		logging.Info("first schedule will make child partition to create new members")
		te.updateFactsFromMembers()
		assert.Equal(t, te.table.GetPartWeight(int32(pid-4)), 2)
		assert.Equal(t, te.table.PartSchedulable(int32(pid-4)), false)
		assert.Equal(t, te.table.GetPartWeight(int32(pid)), 0)
		assert.Equal(t, te.table.PartSchedulable(int32(pid)), false)
		_, _, l := curChild.members.DivideRoles()
		assert.Equal(t, len(curChild.members.Peers), 1)
		assert.Equal(t, len(l), 1)
		assert.Equal(t, curChild.members.SplitVersion, int32(0))
		assert.Equal(t, curParent.members.GetMember(l[0]), pb.ReplicaRole_kPrimary)
		assert.Equal(t, curParent.members.SplitVersion, int32(0))
		if pid < 7 {
			assert.Equal(t, len(te.table.currParts[pid+1].members.Peers), 0)
		}

		assert.Equal(t, client1.GetMetric("replica_split"), 1)
		assert.Equal(t, client1.GetMetric("replica_split_cleanup"), 0)
		splitReq := client1.ReceivedSplitReqs()[utils.MakeTblPartID(te.table.TableId, int32(pid-4))]
		assert.Equal(t, splitReq.NewTableInfo.PartitionNum, int32(8))
		assert.Equal(t, len(splitReq.ChildPeers.Peers), 1)

		logging.Info("then schedule will promote child learner to secondary")
		te.table.Schedule(
			true,
			&pb.ScheduleOptions{EnablePrimaryScheduler: true, MaxSchedRatio: 1000},
		)
		te.updateFactsFromMembers()
		assert.Equal(t, te.table.GetPartWeight(int32(pid-4)), 2)
		assert.Equal(t, te.table.PartSchedulable(int32(pid-4)), false)
		assert.Equal(t, te.table.GetPartWeight(int32(pid)), 0)
		assert.Equal(t, te.table.PartSchedulable(int32(pid)), false)
		assert.Equal(t, curChild.members.SplitVersion, int32(0))
		assert.Equal(t, curChild.members.GetMember(l[0]), pb.ReplicaRole_kSecondary)
		assert.Equal(t, curParent.members.SplitVersion, int32(0))

		logging.Info(
			"then schedule will elect leader for new partition, and split version will be increased",
		)
		te.table.Schedule(
			true,
			&pb.ScheduleOptions{EnablePrimaryScheduler: true, MaxSchedRatio: 1000},
		)
		te.updateFactsFromMembers()
		assert.Equal(t, te.table.GetPartWeight(int32(pid-4)), 1)
		assert.Equal(t, te.table.PartSchedulable(int32(pid-4)), true)
		assert.Equal(t, te.table.GetPartWeight(int32(pid)), 1)
		assert.Equal(t, te.table.PartSchedulable(int32(pid)), true)
		assert.Equal(t, curChild.members.GetMember(l[0]), pb.ReplicaRole_kPrimary)
		assert.Equal(t, curChild.members.SplitVersion, int32(1))
		assert.Equal(t, curParent.members.SplitVersion, int32(1))

		logging.Info("then schedule will trigger parent cleanup")
		curChild.facts["node1"].SplitCleanupVersion = 0
		curParent.facts["node1"].SplitCleanupVersion = 0
		te.table.Schedule(
			true,
			&pb.ScheduleOptions{EnablePrimaryScheduler: true, MaxSchedRatio: 1000},
		)
		assert.Equal(t, client1.GetMetric("replica_split"), 1)
		assert.Equal(t, client1.GetMetric("replica_split_cleanup"), 1)
		splitCleanupReq := client1.ReceivedSplitCleanupReqs()[utils.MakeTblPartID(te.table.TableId, int32(pid-4))]
		assert.Equal(t, splitCleanupReq.PartitionSplitVersion, int32(1))
		curParent.facts["node1"].SplitCleanupVersion = 1

		logging.Info("then schedule will trigger child cleanup")
		te.table.Schedule(
			true,
			&pb.ScheduleOptions{EnablePrimaryScheduler: true, MaxSchedRatio: 1000},
		)
		assert.Equal(t, client1.GetMetric("replica_split"), 1)
		assert.Equal(t, client1.GetMetric("replica_split_cleanup"), 2)
		splitCleanupReq = client1.ReceivedSplitCleanupReqs()[utils.MakeTblPartID(te.table.TableId, int32(pid))]
		assert.Equal(t, splitCleanupReq.PartitionSplitVersion, int32(1))

		te.updateFactsFromMembers()
		client1.CleanMetrics()
	}
}

func TestSplitRelatedBatch(t *testing.T) {
	te := setupTestTableEnv(t, 2, 4, 2, &mockDummyStrategyNoCheckpoint{}, "", true, "mock")
	defer te.teardown()

	mockStore := te.zkStore.(*metastore.MetaStoreMock)
	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()

	logging.Info("first create partitions")
	plan := sched.SchedulePlan{}
	for i := int32(0); i < te.table.PartsCount; i++ {
		action := actions.CreateMembersAction{
			Members: table_model.PartitionMembership{
				MembershipVersion: 1,
				Peers: map[string]pb.ReplicaRole{
					"node1": pb.ReplicaRole_kPrimary,
					"node2": pb.ReplicaRole_kSecondary,
				},
			},
		}
		plan[i] = actions.MakeActions(&action)
	}
	te.applyPlanAndUpdateFacts(plan)

	logging.Info("then trigger split")
	ans := te.table.StartSplit(&pb.SplitTableRequest{
		NewSplitVersion: 1,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 0},
	})
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	logging.Info("first schedule will part 4 to create partition, and all roles are learner")
	te.table.Schedule(true, &pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000})
	te.updateFactsFromMembers()

	logging.Info(
		"make parent to switch primary, splitting child affect, pending child not affected",
	)
	mockStore.CleanWriteBachRecord()
	plan = sched.SchedulePlan{}
	plan[0] = actions.SwitchPrimary("node1", "node2")
	plan[1] = actions.SwitchPrimary("node1", "node2")
	te.applyPlanAndUpdateFacts(plan)

	p, _, _ := te.table.currParts[0].members.DivideRoles()
	assert.Equal(t, p[0], "node2")

	p2, _, _ := te.table.currParts[1].members.DivideRoles()
	assert.Equal(t, p2[0], "node2")

	lastOps := mockStore.GetAllWriteBatches()
	assert.Equal(t, len(lastOps), 2)
	singleOpItems, doubleOpItems := 0, 0
	for _, lastOp := range lastOps {
		if len(lastOp.Ops) == 2 {
			doubleOpItems++
			assert.Equal(t, lastOp.Method, "WriteBatch")
			assert.Equal(t, lastOp.Ops[0].OpPath(), te.table.getPartitionPath(0))
			assert.Equal(t, lastOp.Ops[1].OpPath(), te.table.getPartitionPath(4))
			assert.Equal(t, te.table.currParts[4].members.MembershipVersion, int64(2))
		} else {
			singleOpItems++
			assert.Equal(t, len(lastOp.Ops), 1)
			assert.Equal(t, lastOp.Ops[0].OpPath(), te.table.getPartitionPath(1))
		}
	}
	assert.Equal(t, singleOpItems, 1)
	assert.Equal(t, doubleOpItems, 1)

	logging.Info("next node1 will promote to secondary, and no batch will be generate")
	mockStore.CleanWriteBachRecord()
	te.table.Schedule(true, &pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000})
	te.updateFactsFromMembers()
	p, _, _ = te.table.currParts[4].members.DivideRoles()
	assert.Equal(t, len(p), 0)
	assert.Equal(t, te.table.currParts[4].members.MembershipVersion, int64(3))
	assert.Equal(t, te.table.currParts[4].members.SplitVersion, int32(0))
	assert.Equal(t, te.table.currParts[0].members.SplitVersion, int32(0))
	lastOp := mockStore.GetLastWriteBatchRecord()
	assert.Assert(t, lastOp != nil)
	assert.Equal(t, len(lastOp.Ops), 1)
	assert.Equal(t, lastOp.Ops[0].OpPath(), te.table.getPartitionPath(4))

	logging.Info("next node2 will promote to secondary, and no split batch will be generated")
	mockStore.CleanWriteBachRecord()
	te.table.Schedule(true, &pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000})
	te.updateFactsFromMembers()
	p, _, _ = te.table.currParts[4].members.DivideRoles()
	assert.Equal(t, len(p), 0)
	assert.Equal(t, te.table.currParts[4].members.MembershipVersion, int64(4))
	assert.Equal(t, te.table.currParts[4].members.SplitVersion, int32(0))
	assert.Equal(t, te.table.currParts[0].members.SplitVersion, int32(0))
	lastOp = mockStore.GetLastWriteBatchRecord()
	assert.Assert(t, lastOp != nil)
	assert.Equal(t, len(lastOp.Ops), 1)
	assert.Equal(t, lastOp.Ops[0].OpPath(), te.table.getPartitionPath(4))

	logging.Info("next child will promote to primary, and batch will be generate")
	mockStore.CleanWriteBachRecord()
	te.table.Schedule(true, &pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000})
	te.updateFactsFromMembers()

	p, _, _ = te.table.currParts[4].members.DivideRoles()
	assert.Equal(t, len(p), 1)
	assert.Equal(t, te.table.currParts[4].members.MembershipVersion, int64(5))
	assert.Equal(t, te.table.currParts[4].members.SplitVersion, int32(1))
	assert.Equal(t, te.table.currParts[0].members.SplitVersion, int32(1))

	lastOp = mockStore.GetLastWriteBatchRecord()
	assert.Assert(t, lastOp != nil)
	assert.Equal(t, len(lastOp.Ops), 2)
	assert.Equal(t, lastOp.Ops[0].OpPath(), te.table.getPartitionPath(0))
	assert.Equal(t, lastOp.Ops[1].OpPath(), te.table.getPartitionPath(4))

	logging.Info("part0 change primary won't affect part4 if split finished")
	mockStore.CleanWriteBachRecord()
	plan = sched.SchedulePlan{}
	plan[0] = actions.SwitchPrimary("node2", "node1")
	te.applyPlanAndUpdateFacts(plan)
	p, _, _ = te.table.currParts[0].members.DivideRoles()
	assert.Equal(t, p[0], "node1")
	assert.Equal(t, te.table.currParts[4].members.MembershipVersion, int64(5))

	lastOp = mockStore.GetLastWriteBatchRecord()
	assert.Equal(t, len(lastOp.Ops), 1)
	assert.Equal(t, lastOp.Ops[0].OpPath(), te.table.getPartitionPath(0))
}
