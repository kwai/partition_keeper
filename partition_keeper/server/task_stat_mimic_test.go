package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	cmd_base "github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/checkpoint"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
)

func TestMimickedTaskFinish(t *testing.T) {
	te := setupTaskTestEnv(t, [3]string{"STAGING", "STAGING", "STAGING"})
	defer te.teardown()

	task := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))
	taskDesc := &pb.PeriodicTask{
		TaskName:                checkpoint.TASK_NAME,
		FirstTriggerUnixSeconds: time.Now().Unix() - 100,
		PeriodSeconds:           1,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
		Args:                    nil,
		KeepNums:                5,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()
	oldSessionId1 := task.execInfo.SessionId

	logging.Info("trigger mimic")
	ts := task.FreezeCancelAndMimicExecution(
		map[string]string{"restore_path": "/tmp/colossusdb_test/table_2002/123_123"},
	)
	assert.Equal(t, task.Freezed, true)
	te.checkZkSynced(t, task)

	assert.Equal(t, ts, int64(cmd_base.INVALID_SESSION_ID))
	assert.Assert(t, task.execInfo.SessionId > oldSessionId1)
	assert.Equal(t, task.execInfo.FinishSecond, int64(0))
	assert.DeepEqual(
		t,
		task.execInfo.Args,
		map[string]string{"restore_path": "/tmp/colossusdb_test/table_2002/123_123"},
	)
	oldSessionId2 := task.execInfo.SessionId

	logging.Info("retrigger mimic with same args will have no effect")
	ts = task.FreezeCancelAndMimicExecution(
		map[string]string{"restore_path": "/tmp/colossusdb_test/table_2002/123_123"},
	)
	assert.Equal(t, ts, int64(cmd_base.INVALID_SESSION_ID))
	assert.Equal(t, task.execInfo.SessionId, oldSessionId2)

	logging.Info("issue command will finish mimicked execution")
	task.tryIssueTaskCommand()
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	assert.Equal(t, task.execInfo.FinishStatus, pb.TaskExecInfo_kFinished)

	data, exists, succ := te.zkStore.Get(
		context.Background(),
		task.getTimeStampTaskExecInfoPath(oldSessionId2),
	)
	assert.Assert(t, exists && succ)

	execInfo := &pb.TaskExecInfo{}
	utils.UnmarshalJsonOrDie(data, execInfo)
	assert.Assert(t, proto.Equal(execInfo, task.execInfo))

	logging.Info("retrigger mimic with same args will get new ts if it's finished")
	ts = task.FreezeCancelAndMimicExecution(
		map[string]string{"restore_path": "/tmp/colossusdb_test/table_2002/123_123"},
	)
	assert.Equal(t, ts, oldSessionId2)
	assert.Equal(t, task.execInfo.SessionId, oldSessionId2)

	logging.Info("sleep a while for next task period to come")
	time.Sleep(time.Second * 2)

	logging.Info("issue command again will have no effect, as task is freezed")
	task.tryIssueTaskCommand()
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	assert.Equal(t, task.execInfo.SessionId, oldSessionId2)

	ans := task.issueCommandNow()
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("unfreeze task")
	task.UnfreezeExecution()
	assert.Equal(t, task.Freezed, false)
	te.checkZkSynced(t, task)
	task.tryIssueTaskCommand()
	assert.Assert(t, task.execInfo.SessionId > oldSessionId2)
}

func TestTwoMimicHaveSameArgs(t *testing.T) {
	te := setupTaskTestEnv(t, [3]string{"STAGING", "STAGING", "STAGING"})
	defer te.teardown()

	task := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))
	taskDesc := &pb.PeriodicTask{
		TaskName:                checkpoint.TASK_NAME,
		FirstTriggerUnixSeconds: time.Now().Unix() - 100,
		PeriodSeconds:           1,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
		Args:                    nil,
		KeepNums:                5,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()
	oldSessionId1 := task.execInfo.SessionId

	logging.Info("trigger mimic")
	ts := task.FreezeCancelAndMimicExecution(
		map[string]string{"restore_path": "/tmp/colossusdb_test/table_2002/123_123"},
	)
	assert.Equal(t, task.Freezed, true)
	te.checkZkSynced(t, task)

	assert.Equal(t, ts, int64(cmd_base.INVALID_SESSION_ID))
	assert.Assert(t, task.execInfo.SessionId > oldSessionId1)
	assert.Equal(t, task.execInfo.FinishSecond, int64(0))
	assert.DeepEqual(
		t,
		task.execInfo.Args,
		map[string]string{"restore_path": "/tmp/colossusdb_test/table_2002/123_123"},
	)
	oldSessionId2 := task.execInfo.SessionId

	logging.Info("issue command will finish mimicked execution")
	task.tryIssueTaskCommand()
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	assert.Equal(t, task.execInfo.FinishStatus, pb.TaskExecInfo_kFinished)

	data, exists, succ := te.zkStore.Get(
		context.Background(),
		task.getTimeStampTaskExecInfoPath(oldSessionId2),
	)
	assert.Assert(t, exists && succ)

	execInfo := &pb.TaskExecInfo{}
	utils.UnmarshalJsonOrDie(data, execInfo)
	assert.Assert(t, proto.Equal(execInfo, task.execInfo))

	logging.Info("resume task")
	task.UnfreezeExecution()

	logging.Info("retrigger mimic with same args will start a new session")
	ts = task.FreezeCancelAndMimicExecution(
		map[string]string{"restore_path": "/tmp/colossusdb_test/table_2002/123_123"},
	)
	assert.Equal(t, task.Freezed, true)
	te.checkZkSynced(t, task)
	assert.Equal(t, ts, int64(cmd_base.INVALID_SESSION_ID))
	assert.Assert(t, task.execInfo.SessionId > oldSessionId2)
	assert.Equal(t, task.execInfo.FinishSecond, int64(0))
	assert.DeepEqual(
		t,
		task.execInfo.Args,
		map[string]string{"restore_path": "/tmp/colossusdb_test/table_2002/123_123"},
	)
}

func TestMimicTaskCancelCurrentRunningOne(t *testing.T) {
	te := setupTaskTestEnv(t, [3]string{"STAGING", "STAGING", "STAGING"})
	defer te.teardown()

	for i := range te.table.currParts {
		part := &(te.table.currParts[i])
		part.members.MembershipVersion = 3
		part.members.Peers["node1"] = pb.ReplicaRole_kPrimary
		part.members.Peers["node2"] = pb.ReplicaRole_kSecondary
		part.members.Peers["node3"] = pb.ReplicaRole_kSecondary
	}

	task := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))

	taskDesc := &pb.PeriodicTask{
		TaskName:                checkpoint.TASK_NAME,
		FirstTriggerUnixSeconds: time.Now().Unix(),
		PeriodSeconds:           3600,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
		Args:                    map[string]string{"a": "b"},
		KeepNums:                5,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	logging.Info("first start a new execution, but can't finished until next issue")
	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()
	session1 := proto.Clone(task.execInfo).(*pb.TaskExecInfo)
	concurrencyContext := concurrencyContext{
		maxConcurrentHubs:    0,
		maxConcurrentNodes:   0,
		maxConcurrentPerNode: 0,
	}

	logging.Info(
		"then start mimic in a goroutine and schedule issueing in another goroutine. session 1 will be cancelled",
	)

	finish := make(chan bool)
	go func() {
		ts := task.FreezeCancelAndMimicExecution(
			map[string]string{"restore_path": "/tmp/colossusdb_test/table_2002/123_123"},
		)
		assert.Equal(t, ts, int64(cmd_base.INVALID_SESSION_ID))
		finish <- true
	}()
	go func() {
		time.Sleep(time.Millisecond * 100)
		task.issueTaskCommand(session1, concurrencyContext)
		finish <- true
	}()

	<-finish
	<-finish

	assert.Equal(t, task.execHistory.Len(), 3)
	// first one is the fake one
	execution1 := task.execHistory.Front().Next().Value.(*pb.TaskExecInfo)
	assert.Equal(t, execution1.SessionId, session1.SessionId)
	assert.Assert(t, execution1.FinishSecond > 0)
	assert.Equal(t, execution1.FinishStatus, pb.TaskExecInfo_kCancelled)

	taskInfo := &pb.TaskExecInfo{}
	data, exists, succ := te.zkStore.Get(
		context.Background(),
		task.getTimeStampTaskExecInfoPath(session1.SessionId),
	)
	assert.Assert(t, succ && exists)
	utils.UnmarshalJsonOrDie(data, taskInfo)
	assert.Assert(t, proto.Equal(execution1, taskInfo))
}

func TestMimicTaskRestorePath(t *testing.T) {
	te := setupTaskTestEnv(t, [3]string{"STAGING", "STAGING", "STAGING"})
	defer te.teardown()

	for i := range te.table.currParts {
		part := &(te.table.currParts[i])
		part.members.MembershipVersion = 3
		part.members.Peers["node1"] = pb.ReplicaRole_kPrimary
		part.members.Peers["node2"] = pb.ReplicaRole_kSecondary
		part.members.Peers["node3"] = pb.ReplicaRole_kSecondary
	}

	task := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))

	taskDesc := &pb.PeriodicTask{
		TaskName:                checkpoint.TASK_NAME,
		FirstTriggerUnixSeconds: time.Now().Unix(),
		PeriodSeconds:           1,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
		Args:                    map[string]string{"a": "b"},
		KeepNums:                5,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	logging.Info("first create a new complete execution")
	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	firstSession := proto.Clone(task.execInfo).(*pb.TaskExecInfo)

	logging.Info("then mimic with path from other table")
	task.FreezeCancelAndMimicExecution(
		map[string]string{"restore_path": "/tmp/colossusdb/table_2002/12345_12345"},
	)
	task.tryIssueTaskCommand()
	assert.Equal(t, task.execHistory.Len(), 3)
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	secondSession := proto.Clone(task.execInfo).(*pb.TaskExecInfo)
	task.UnfreezeExecution()

	time.Sleep(time.Second)
	logging.Info("then create a new complete execution again")
	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()
	assert.Equal(t, task.execHistory.Len(), 4)
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	thirdSession := proto.Clone(task.execInfo).(*pb.TaskExecInfo)

	logging.Info("then mimic with path from my old checkpoint")
	path := fmt.Sprintf(
		"/tmp/colossusdb_test/table_%d/%d_%d",
		te.table.TableId,
		firstSession.SessionId,
		firstSession.StartSecond,
	)
	task.FreezeCancelAndMimicExecution(map[string]string{"restore_path": path})
	task.tryIssueTaskCommand()
	assert.Equal(t, task.execHistory.Len(), 4)
	assert.Assert(t, task.execInfo.FinishSecond > 0)

	historyHead := task.execHistory.Front()
	assert.Equal(t, historyHead.Value.(*pb.TaskExecInfo).SessionId, int64(1))

	historyHead = historyHead.Next()
	assert.Assert(t, proto.Equal(historyHead.Value.(*pb.TaskExecInfo), secondSession))

	historyHead = historyHead.Next()
	assert.Assert(t, proto.Equal(historyHead.Value.(*pb.TaskExecInfo), thirdSession))

	historyHead = historyHead.Next()
	forthSession := historyHead.Value.(*pb.TaskExecInfo)
	assert.Assert(t, forthSession.SessionId > thirdSession.SessionId)
	assert.DeepEqual(t, forthSession.Args, map[string]string{"restore_path": path})
}
