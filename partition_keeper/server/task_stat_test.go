package server

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/delay_execute"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/fs"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd"
	cmd_base "github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/checkpoint"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/schema_change"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
)

type mockedTaskEffectHandler struct {
	params *cmd_base.HandlerParams
}

func (m *mockedTaskEffectHandler) CheckMimicArgs(args map[string]string, tablePb *pb.Table) error {
	return nil
}

func (m *mockedTaskEffectHandler) BeforeExecutionDurable(history *list.List) bool {
	return true
}

func (m *mockedTaskEffectHandler) BorrowFromHistory(
	history *list.List,
	args map[string]string,
) int64 {
	return cmd_base.INVALID_SESSION_ID
}

func (m *mockedTaskEffectHandler) CleanOneExecution(execInfo *pb.TaskExecInfo) bool {
	return true
}

func (m *mockedTaskEffectHandler) CleanAllExecutions() bool {
	return true
}

var (
	mockedBeforeExecutionDurableCallCount int
)

type mockedBeforeExecutionDurableFail struct {
	mockedTaskEffectHandler
}

func (m *mockedBeforeExecutionDurableFail) BeforeExecutionDurable(history *list.List) bool {
	mockedBeforeExecutionDurableCallCount++
	return mockedBeforeExecutionDurableCallCount > 1
}

type taskTestEnv struct {
	zkPath  string
	zkStore metastore.MetaStore
	lp      *rpc.LocalPSClientPoolBuilder
	table   *TableStats
}

func setupTaskTestEnv(t *testing.T, azs [3]string) *taskTestEnv {
	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	out := &taskTestEnv{}
	out.zkPath = utils.SpliceZkRootPath("/test/pk/test_tasks")
	out.zkStore = metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"},
		time.Second*10,
		acl,
		scheme,
		auth,
	)

	succ := out.zkStore.RecursiveDelete(context.Background(), out.zkPath)
	assert.Assert(t, succ)

	out.lp = rpc.NewLocalPSClientPoolBuilder()

	lock := utils.NewLooseLock()
	hubs := []*pb.ReplicaHub{
		{Name: azs[0] + "_1", Az: azs[0]},
		{Name: azs[1] + "_2", Az: azs[1]},
		{Name: azs[2] + "_3", Az: azs[2]},
	}

	succ = out.zkStore.RecursiveCreate(context.Background(), out.zkPath+"/nodes")
	assert.Assert(t, succ)
	succ = out.zkStore.RecursiveCreate(context.Background(), out.zkPath+"/hints")
	assert.Assert(t, succ)

	nodes := node_mgr.NewNodeStats(
		"test",
		lock,
		out.zkPath+"/nodes",
		out.zkPath+"/hints",
		out.zkStore,
	)
	nodes.LoadFromZookeeper(utils.MapHubs(hubs), true)

	lock.LockWrite()
	nodes.UpdateStats(map[string]*node_mgr.NodePing{
		"node1": {
			IsAlive:   true,
			Az:        azs[0],
			Address:   utils.FromHostPort("127.0.0.1:1001"),
			ProcessId: "12341",
			BizPort:   2001,
			NodeIndex: "1.0",
		},
		"node2": {
			IsAlive:   true,
			Az:        azs[1],
			Address:   utils.FromHostPort("127.0.0.1:1002"),
			ProcessId: "12342",
			BizPort:   2002,
			NodeIndex: "2.0",
		},
		"node3": {
			IsAlive:   true,
			Az:        azs[2],
			Address:   utils.FromHostPort("127.0.0.1:1003"),
			ProcessId: "12343",
			BizPort:   2003,
			NodeIndex: "3.0",
		},
	})
	lock.UnlockWrite()

	succ = out.zkStore.RecursiveCreate(context.Background(), out.zkPath+"/tables")
	assert.Assert(t, succ)

	delayedExecutorManager := delay_execute.NewDelayedExecutorManager(
		out.zkStore,
		out.zkPath+"/"+kDelayedExecutorNode,
	)
	delayedExecutorManager.InitFromZookeeper()

	out.table = NewTableStats(
		"test",
		"test",
		false,
		lock,
		nodes,
		hubs,
		strategy.MustNewServiceStrategy(pb.ServiceType_colossusdb_dummy),
		out.lp.Build(),
		out.zkStore,
		delayedExecutorManager,
		out.zkPath+"/tables",
		nil,
	)

	tableDescription := &pb.Table{
		TableId:           1,
		TableName:         "test",
		HashMethod:        "crc32",
		PartsCount:        8,
		JsonArgs:          `{"a": "b"}`,
		KconfPath:         "reco.rodisFea.partitionKeeperHDFSTest",
		ScheduleGrayscale: kScheduleGrayscaleMax + 1,
	}

	out.table.InitializeNew(tableDescription, "")
	return out
}

func (te *taskTestEnv) teardown() {
	te.table.Stop(true)
	te.table.delayedExecutorManager.Stop()
	te.zkStore.RecursiveDelete(context.Background(), te.zkPath)
	te.zkStore.Close()
}

func (te *taskTestEnv) checkZkSynced(t *testing.T, task *TaskStat) {
	data, exists, succ := te.zkStore.Get(context.Background(), task.zkPath)
	assert.Assert(t, exists && succ)

	taskDesc := &pb.PeriodicTask{}
	utils.UnmarshalJsonOrDie(data, taskDesc)
	assert.Assert(t, proto.Equal(taskDesc, task.PeriodicTask))
}

func (te *taskTestEnv) generateFullExecutionRound(t *testing.T, task *TaskStat) {
	oldSessionId := task.execInfo.SessionId
	task.tryIssueTaskCommand()
	newSessionId := task.execInfo.SessionId
	assert.Assert(t, newSessionId > oldSessionId)
	assert.Equal(t, task.execInfo.FinishSecond, int64(0))
	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()
	assert.Equal(t, task.execInfo.SessionId, newSessionId)
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	assert.Assert(t, proto.Equal(task.execInfo, task.execHistory.Back().Value.(*pb.TaskExecInfo)))
}

func (te *taskTestEnv) checkExecHistoryZkSynced(t *testing.T, task *TaskStat) {
	children, exists, succ := te.zkStore.Children(context.Background(), task.zkPath)
	assert.Assert(t, exists && succ)
	filterLast := []string{}
	for _, item := range children {
		if item != kTaskExecZNode {
			filterLast = append(filterLast, item)
		}
	}
	sort.Strings(filterLast)
	assert.Equal(t, len(filterLast), task.execHistory.Len())
	index := 0
	for iter := task.execHistory.Front(); iter != nil; iter = iter.Next() {
		item := iter.Value.(*pb.TaskExecInfo)
		assert.Equal(t, fmt.Sprintf("%d", item.SessionId), filterLast[index])
		index++
	}
}

func TestStartNewRoundPermission(t *testing.T) {
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
		Paused:                  false,
		Freezed:                 false,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	permissionFlags := [][]bool{
		{false, false},
		{true, false},
		{false, true},
		{true, true},
	}
	expectValues := []bool{
		false,
		true,
		true,
		true,
	}
	for i, f := range permissionFlags {
		task.Paused = f[0]
		task.Freezed = f[1]
		assert.Equal(t, task.disallowToStartNewRound(), expectValues[i])
	}
}

func TestIssueCreatedTask(t *testing.T) {
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
		Paused:                  true,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	for i := range task.progress.partExecutions {
		assert.Assert(t, task.progress.partExecutions[i].receivers == nil)
	}
	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()
	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond > 0)

	taskDesc.Paused = false
	err := task.UpdateTask(taskDesc)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))

	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()
	assert.Assert(t, task.execInfo.SessionId > 1)
	assert.Equal(t, task.execInfo.FinishSecond, int64(0))
	for i := range task.progress.partExecutions {
		part := &(task.progress.partExecutions[i])
		assert.Assert(t, len(part.receivers) > 0)
		for _, recv := range part.receivers {
			assert.Equal(t, recv.progress, int32(kProgressFinished))
			assert.Assert(t, recv.nodeId != "")
		}
		assert.Assert(t, part.hasFinished())
	}
	client := te.lp.GetClient("127.0.0.1:1001")
	assert.Assert(t, client.GetMetric("command") > 0)

	task.tryIssueTaskCommand()
	assert.Assert(t, task.execInfo.FinishSecond > 0)

	oldFinishTime := task.execInfo.FinishSecond
	task.tryIssueTaskCommand()
	assert.Equal(t, task.execInfo.FinishSecond, oldFinishTime)

	task2 := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))
	task2.LoadFromZookeeper(te.zkPath, checkpoint.TASK_NAME)
	defer task2.Stop()

	assert.Assert(t, proto.Equal(taskDesc, task2.PeriodicTask))
	assert.Equal(t, task.execInfo.SessionId, task2.execInfo.SessionId)
	assert.Equal(t, task.execInfo.FinishSecond, task2.execInfo.FinishSecond)
}

func TestFinishOneRoundRetry(t *testing.T) {
	mockedBeforeExecutionDurableCallCount = 0
	mockedHandlerName := "before_exec_durable_fail"
	cmd.RegisterCmdHandler(
		mockedHandlerName,
		func(params *cmd_base.HandlerParams) cmd_base.CmdHandler {
			return &mockedBeforeExecutionDurableFail{
				mockedTaskEffectHandler: mockedTaskEffectHandler{
					params: params,
				},
			}
		},
	)
	defer func() {
		cmd.UnregisterCmdHandler(mockedHandlerName)
	}()

	te := setupTaskTestEnv(t, [3]string{"YZ", "YZ", "YZ"})
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
		TaskName:                mockedHandlerName,
		FirstTriggerUnixSeconds: time.Now().Unix(),
		PeriodSeconds:           3600,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
		Args:                    map[string]string{"a": "b"},
		KeepNums:                5,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()
	assert.Assert(t, task.execInfo.SessionId > 0)
	assert.Equal(t, task.execInfo.FinishSecond, int64(0))
	for i := range task.progress.partExecutions {
		part := &(task.progress.partExecutions[i])
		assert.Assert(t, len(part.receivers) > 0)
		for _, recv := range part.receivers {
			assert.Equal(t, recv.progress, int32(kProgressFinished))
			assert.Assert(t, recv.nodeId != "")
		}
		assert.Assert(t, part.hasFinished())
	}
	task.tryIssueTaskCommand()
	assert.Equal(t, task.execInfo.FinishSecond, int64(0))

	data, exists, succ := te.zkStore.Get(
		context.Background(),
		task.getTimeStampTaskExecInfoPath(task.execInfo.SessionId),
	)
	assert.Assert(t, exists && succ)
	execInfo := &pb.TaskExecInfo{}
	utils.UnmarshalJsonOrDie(data, execInfo)
	assert.Equal(t, execInfo.FinishSecond, int64(0))

	task.tryIssueTaskCommand()
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	assert.Equal(t, task.execInfo.FinishStatus, pb.TaskExecInfo_kFinished)

	data, exists, succ = te.zkStore.Get(
		context.Background(),
		task.getTimeStampTaskExecInfoPath(task.execInfo.SessionId),
	)
	assert.Assert(t, exists && succ)
	utils.UnmarshalJsonOrDie(data, execInfo)
	assert.Equal(t, execInfo.FinishSecond, task.execInfo.FinishSecond)
	assert.Equal(t, execInfo.FinishStatus, pb.TaskExecInfo_kFinished)
}

func TestOldReceiverDead(t *testing.T) {
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

	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	for i := range task.progress.partExecutions {
		assert.Assert(t, task.progress.partExecutions[i].receivers == nil)
	}
	task.tryIssueTaskCommand()

	client1 := te.lp.GetClient("127.0.0.1:1001")
	assert.Assert(t, client1.GetMetric("command") > 0)

	client1.CleanMetrics()

	te.table.nodes.MustGetNodeInfo("node1").IsAlive = false
	for i := range te.table.currParts {
		part := &(te.table.currParts[i])
		part.members.MembershipVersion = 3
		part.members.Peers["node1"] = pb.ReplicaRole_kLearner
		part.members.Peers["node2"] = pb.ReplicaRole_kPrimary
		part.members.Peers["node3"] = pb.ReplicaRole_kSecondary
	}

	task.tryIssueTaskCommand()
	assert.Equal(t, client1.GetMetric("command"), 0)

	client2 := te.lp.GetClient("127.0.0.1:1002")
	assert.Assert(t, client2.GetMetric("command") > 0)

	client2.CleanMetrics()

	te.table.nodes.MustGetNodeInfo("node2").IsAlive = false
	for i := range te.table.currParts {
		part := &(te.table.currParts[i])
		part.members.MembershipVersion = 3
		part.members.Peers["node1"] = pb.ReplicaRole_kLearner
		part.members.Peers["node2"] = pb.ReplicaRole_kLearner
		part.members.Peers["node3"] = pb.ReplicaRole_kSecondary
	}

	task.tryIssueTaskCommand()
	assert.Equal(t, client1.GetMetric("command"), 0)
	assert.Equal(t, client2.GetMetric("command"), 0)

	client3 := te.lp.GetClient("127.0.0.1:1003")
	assert.Assert(t, client3.GetMetric("command") > 0)
}

func TestUpdateTask(t *testing.T) {
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
		FirstTriggerUnixSeconds: time.Now().Unix() + 3600,
		PeriodSeconds:           3600,
		MaxConcurrentNodes:      1,
		KeepNums:                1,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_ANY,
		Args:                    map[string]string{"a": "b"},
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	desc2 := &pb.PeriodicTask{
		TaskName:                checkpoint.TASK_NAME,
		FirstTriggerUnixSeconds: time.Now().Unix() + 1200,
		PeriodSeconds:           0,
		KeepNums:                -1,
	}
	oldFirstTriggerUnixSeconds := desc2.FirstTriggerUnixSeconds
	// error PeriodSeconds
	err := task.UpdateTask(desc2)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))
	desc2.PeriodSeconds = 3600 * 24 * 400
	err = task.UpdateTask(desc2)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))

	// error FirstTriggerUnixSeconds
	desc2.PeriodSeconds = 1200
	desc2.FirstTriggerUnixSeconds = 1200
	err = task.UpdateTask(desc2)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))

	desc2.FirstTriggerUnixSeconds = -10
	err = task.UpdateTask(desc2)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))

	// error keep nums
	desc2.FirstTriggerUnixSeconds = time.Now().Unix() + 3600
	desc2.KeepNums = -10
	err = task.UpdateTask(desc2)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))

	desc2.KeepNums = 2
	desc2.NotifyMode = pb.TaskNotifyMode_NOTIFY_EVERY_REGION

	// error MaxConcurrentNodes
	desc2.MaxConcurrentNodes = -10
	err = task.UpdateTask(desc2)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))
	desc2.MaxConcurrentNodes = 5

	err = task.UpdateTask(desc2)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, task.PeriodSeconds, desc2.PeriodSeconds)
	assert.Equal(t, task.FirstTriggerUnixSeconds, desc2.FirstTriggerUnixSeconds)
	assert.Equal(t, task.NotifyMode, desc2.NotifyMode)
	assert.Equal(t, task.KeepNums, desc2.KeepNums)
	assert.Equal(t, task.MaxConcurrentNodes, desc2.MaxConcurrentNodes)
	assert.DeepEqual(t, task.Args, map[string]string{"a": "b"})
	te.checkZkSynced(t, task)

	desc2.Args = map[string]string{"hello": "world"}
	err = task.UpdateTask(desc2)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	assert.DeepEqual(t, task.Args, map[string]string{"hello": "world"})
	te.checkZkSynced(t, task)

	task.tryIssueTaskCommand()
	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond != 0)

	logging.Info("first pause the task, freeze flag won't be affected")
	desc3 := proto.Clone(task.PeriodicTask).(*pb.PeriodicTask)
	desc3.Freezed = true
	desc3.Paused = true
	err = task.UpdateTask(desc3)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, task.Paused, true)
	assert.Equal(t, task.Freezed, false)
	te.checkZkSynced(t, task)

	logging.Info("test trigger command now, don't work as task is paused")
	desc2.FirstTriggerUnixSeconds = 0
	err = task.UpdateTask(desc2)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond != 0)

	logging.Info("resume task and then trigger again")
	desc3.Paused = false
	err = task.UpdateTask(desc3)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, task.Paused, false)
	assert.Equal(t, task.Freezed, false)
	te.checkZkSynced(t, task)

	err = task.UpdateTask(desc2)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, task.execInfo.FinishSecond, int64(0))
	assert.Assert(t, task.execInfo.SessionId > 0)

	logging.Info("retrigger will fall as already have running one")
	err = task.UpdateTask(desc2)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))
	desc2.FirstTriggerUnixSeconds = oldFirstTriggerUnixSeconds

	// first will send command to all servers
	task.tryIssueTaskCommand()
	assert.Equal(t, task.execInfo.FinishSecond, int64(0))
	desc2.MaxConcurrentNodes = 1
	err = task.UpdateTask(desc2)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))

	// second will got response from servers
	task.tryIssueTaskCommand()
	assert.Equal(t, task.execInfo.FinishSecond, int64(0))

	// third will check succeed
	task.tryIssueTaskCommand()
	assert.Assert(t, task.execInfo.FinishSecond != 0)
}

func TestMultiRegions(t *testing.T) {
	mockedHandlerName := "mocked_task_effect_handler"
	cmd.RegisterCmdHandler(
		mockedHandlerName,
		func(params *cmd_base.HandlerParams) cmd_base.CmdHandler {
			return &mockedTaskEffectHandler{
				params: params,
			}
		},
	)
	defer func() {
		cmd.UnregisterCmdHandler(mockedHandlerName)
	}()

	te := setupTaskTestEnv(t, [3]string{"YZ", "GZ1", "TXSGP1"})
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
		TaskName:                mockedHandlerName,
		FirstTriggerUnixSeconds: time.Now().Unix(),
		PeriodSeconds:           3600,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
		Args:                    map[string]string{"a": "b"},
		KeepNums:                5,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	for i := range task.progress.partExecutions {
		assert.Assert(t, task.progress.partExecutions[i].receivers == nil)
	}
	te.table.nodes.MustGetNodeInfo("node1").Op = pb.AdminNodeOp_kOffline

	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()

	for i := range task.progress.partExecutions {
		pe := &(task.progress.partExecutions[i])
		assert.Assert(t, len(pe.receivers) == 3)

		node2Count := 0
		node3Count := 0
		emptyCount := 0
		for _, recv := range pe.receivers {
			if recv.nodeId == "node2" {
				assert.Equal(t, recv.progress, int32(kProgressFinished))
				node2Count++
			} else if recv.nodeId == "node3" {
				assert.Equal(t, recv.progress, int32(kProgressFinished))
				node3Count++
			} else {
				assert.Equal(t, recv.nodeId, "")
				emptyCount++
			}
		}
		assert.Equal(t, node2Count, 1)
		assert.Equal(t, node3Count, 1)
		assert.Equal(t, emptyCount, 1)
		assert.Assert(t, !pe.hasFinished())
	}

	te.table.nodes.MustGetNodeInfo("node1").Op = pb.AdminNodeOp_kNoop
	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()
	assert.Assert(t, task.execInfo.FinishSecond > 0)
}

func TestCleanTaskSideEffect(t *testing.T) {
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
		PeriodSeconds:           2,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
		Args:                    map[string]string{"a": "b"},
		KeepNums:                1,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	for i := range task.progress.partExecutions {
		assert.Assert(t, task.progress.partExecutions[i].receivers == nil)
	}
	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()
	// mock checkpoint on hdfs
	var hdfsPath, checkpointPath string
	{
		data, err := third_party.GetStringConfig(te.table.KconfPath)
		assert.Assert(t, err == nil)

		value := &checkpoint.KConfValue{}
		err = json.Unmarshal([]byte(data), value)
		assert.Assert(t, err == nil)
		checkpointPath = value.CheckpointPath
		hdfsPath = (value.CheckpointPath + "/" + "table_" +
			strconv.FormatInt(int64(te.table.TableId), 10) + "/" +
			strconv.FormatInt(task.execInfo.StartSecond, 10) + "_" +
			strconv.FormatInt(task.execInfo.StartSecond, 10))
	}

	fileSystem, _, _ := fs.GetFsContextByKconfPath(checkpointPath)
	assert.Equal(t, fileSystem.RecursiveDelete(hdfsPath, "STAGING", ""), true)
	assert.Equal(t, fileSystem.RecursiveMkdir(hdfsPath, "STAGING", ""), true)
	list, succ := fileSystem.ListDirectory(hdfsPath, "STAGING", "")
	assert.Equal(t, succ, true)
	assert.Equal(t, len(list.FileStatuses.FileStatus), 0)

	firstCheckpointZkPath := te.zkPath + "/checkpoint/" + strconv.FormatInt(
		task.execInfo.StartSecond,
		10,
	)

	// first succ to do checkpoint
	assert.Assert(t, task.execInfo.SessionId > 0)
	assert.Equal(t, task.execInfo.FinishSecond, int64(0))
	for i := range task.progress.partExecutions {
		part := &(task.progress.partExecutions[i])
		assert.Assert(t, len(part.receivers) > 0)
		for _, recv := range part.receivers {
			assert.Equal(t, recv.progress, int32(kProgressFinished))
			assert.Assert(t, recv.nodeId != "")
		}
		assert.Assert(t, part.hasFinished())
	}
	assert.Equal(t, task.execHistory.Len(), 2)
	client := te.lp.GetClient("127.0.0.1:1001")
	assert.Assert(t, client.GetMetric("command") > 0)
	task.tryIssueTaskCommand()
	_, exist, succ := te.zkStore.Get(context.Background(), firstCheckpointZkPath)
	assert.Equal(t, succ, true)
	assert.Equal(t, exist, true)

	// second succ to do checkpoint
	time.Sleep(time.Second * 3)
	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()
	secondCheckpointZkPath := te.zkPath + "/checkpoint/" + strconv.FormatInt(
		task.execInfo.StartSecond,
		10,
	)

	assert.Equal(t, task.execHistory.Len(), 3)
	for i := range task.progress.partExecutions {
		part := &(task.progress.partExecutions[i])
		assert.Assert(t, len(part.receivers) > 0)
		for _, recv := range part.receivers {
			assert.Equal(t, recv.progress, int32(kProgressFinished))
			assert.Assert(t, recv.nodeId != "")
		}
		assert.Assert(t, part.hasFinished())
	}
	task.tryIssueTaskCommand()
	assert.Equal(t, task.execHistory.Len(), 1)

	// check zk
	_, exist, succ = te.zkStore.Get(context.Background(), firstCheckpointZkPath)
	assert.Equal(t, succ, true)
	assert.Equal(t, exist, false)

	_, exist, succ = te.zkStore.Get(context.Background(), secondCheckpointZkPath)
	assert.Equal(t, succ, true)
	assert.Equal(t, exist, true)

	// check hdfs
	list, succ = fileSystem.ListDirectory(hdfsPath, "STAGING", "")
	assert.Equal(t, succ, false)
	assert.Assert(t, list.RemoteException.Exception == fs.HdfsError_kFileNotExist)
}

func TestLoadOldStyleTask(t *testing.T) {
	mockedHandlerName := "mocked_task_effect_handler"
	cmd.RegisterCmdHandler(
		mockedHandlerName,
		func(params *cmd_base.HandlerParams) cmd_base.CmdHandler {
			return &mockedTaskEffectHandler{
				params: params,
			}
		},
	)
	defer func() {
		cmd.UnregisterCmdHandler(mockedHandlerName)
	}()

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
		TaskName:                mockedHandlerName,
		FirstTriggerUnixSeconds: time.Now().Unix(),
		PeriodSeconds:           1,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
		Args:                    map[string]string{"a": "b"},
		KeepNums:                3,
	}
	logging.Info("first create task")
	task.InitializeNew(te.zkPath, taskDesc)
	task.Stop()
	assert.Equal(t, task.execInfo.FinishStatus, pb.TaskExecInfo_kCancelled)

	logging.Info("then remove exec info with session id and create one with \"last\"")
	data := utils.MarshalJsonOrDie(task.execInfo)
	delOp := &metastore.DeleteOp{
		Path: task.getTimeStampTaskExecInfoPath(task.execInfo.SessionId),
	}
	createOp := &metastore.CreateOp{
		Path: task.subpath(kTaskExecZNode),
		Data: data,
	}
	succ := te.zkStore.WriteBatch(context.Background(), delOp, createOp)
	assert.Equal(t, succ, true)

	logging.Info("create new task and load")
	task2 := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))
	task2.LoadFromZookeeper(te.zkPath, mockedHandlerName)
	task2.Stop()

	assert.Equal(t, task2.execHistory.Len(), 1)
	assert.Assert(t, proto.Equal(task2.execInfo, task.execInfo))

	createPath := task2.getTimeStampTaskExecInfoPath(task2.execInfo.SessionId)
	data, succ, exists := te.zkStore.Get(context.Background(), createPath)
	assert.Equal(t, succ && exists, true)

	execInfo := &pb.TaskExecInfo{}
	utils.UnmarshalJsonOrDie(data, execInfo)
	assert.Assert(t, proto.Equal(task2.execInfo, execInfo))

	logging.Info("load again")
	task3 := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))
	task3.LoadFromZookeeper(te.zkPath, mockedHandlerName)
	assert.Equal(t, task2.execHistory.Len(), 1)
	assert.Assert(t, proto.Equal(task3.execInfo, task.execInfo))

	logging.Info("then create 3 round of execution")
	te.generateFullExecutionRound(t, task3)
	time.Sleep(time.Second)
	te.generateFullExecutionRound(t, task3)
	time.Sleep(time.Second)
	te.generateFullExecutionRound(t, task3)
	assert.Equal(t, task3.execHistory.Len(), 4)
	task3.Stop()

	logging.Info("then load again")
	task4 := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))
	task4.LoadFromZookeeper(te.zkPath, mockedHandlerName)
	defer task4.Stop()
	assert.Equal(t, task4.execHistory.Len(), 4)

	t3Head := task3.execHistory.Front()
	t4Head := task4.execHistory.Front()
	var prevT4Head *list.Element = nil

	for t3Head != nil {
		assert.Assert(
			t,
			proto.Equal(t3Head.Value.(*pb.TaskExecInfo), t4Head.Value.(*pb.TaskExecInfo)),
		)
		if prevT4Head != nil {
			assert.Assert(
				t,
				prevT4Head.Value.(*pb.TaskExecInfo).SessionId != t4Head.Value.(*pb.TaskExecInfo).SessionId,
			)
		}
		prevT4Head = t4Head
		t4Head = t4Head.Next()
		t3Head = t3Head.Next()
	}
}

func TestLoadTask(t *testing.T) {
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
		KeepNums:                1,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	{
		task2 := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))
		task2.LoadFromZookeeper(te.zkPath, checkpoint.TASK_NAME)
		defer task2.Stop()
		assert.Equal(t, task2.execHistory.Len(), 1)
		data, exist, succ := te.zkStore.Get(context.Background(), te.zkPath+"/checkpoint/1")
		assert.Equal(t, succ, true)
		assert.Equal(t, exist, true)
		taskInfo := &pb.TaskExecInfo{}
		utils.UnmarshalJsonOrDie(data, &taskInfo)
		assert.Assert(t, proto.Equal(taskInfo, task2.execInfo))
	}

	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	assert.Equal(t, task.execInfo.FinishStatus, pb.TaskExecInfo_kCancelled)
	for i := range task.progress.partExecutions {
		assert.Assert(t, task.progress.partExecutions[i].receivers == nil)
	}
	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()

	timestamp := task.execInfo.StartSecond
	// succ to do checkpoint
	assert.Assert(t, task.execInfo.SessionId > 0)
	assert.Equal(t, task.execInfo.FinishSecond, int64(0))
	for i := range task.progress.partExecutions {
		part := &(task.progress.partExecutions[i])
		assert.Assert(t, len(part.receivers) > 0)
		for _, recv := range part.receivers {
			assert.Equal(t, recv.progress, int32(kProgressFinished))
			assert.Assert(t, recv.nodeId != "")
		}
		assert.Assert(t, part.hasFinished())
	}
	assert.Equal(t, task.execHistory.Len(), 2)
	client := te.lp.GetClient("127.0.0.1:1001")
	assert.Assert(t, client.GetMetric("command") > 0)
	task.tryIssueTaskCommand()
	assert.Equal(t, task.execHistory.Len(), 2)

	{
		// load /test /timestamp
		task3 := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))
		task3.LoadFromZookeeper(te.zkPath, checkpoint.TASK_NAME)
		defer task3.Stop()
		assert.Equal(t, task3.execHistory.Len(), 2)
		data, exist, succ := te.zkStore.Get(
			context.Background(),
			te.zkPath+"/checkpoint/"+strconv.FormatInt(timestamp, 10),
		)
		assert.Equal(t, succ, true)
		assert.Equal(t, exist, true)
		taskInfo := &pb.TaskExecInfo{}
		utils.UnmarshalJsonOrDie(data, &taskInfo)
		assert.Assert(t, proto.Equal(task3.execInfo, taskInfo))
	}

}

func TestUpdateKeepNums(t *testing.T) {
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
		KeepNums:                -1,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	for i := range task.progress.partExecutions {
		assert.Assert(t, task.progress.partExecutions[i].receivers == nil)
	}

	for i := 0; i < 3; i++ {
		task.tryIssueTaskCommand()
		task.tryIssueTaskCommand()

		// first succ to do checkpoint
		assert.Assert(t, task.execInfo.SessionId > 0)
		assert.Equal(t, task.execInfo.FinishSecond, int64(0))
		for i := range task.progress.partExecutions {
			part := &(task.progress.partExecutions[i])
			assert.Assert(t, len(part.receivers) > 0)
			for _, recv := range part.receivers {
				assert.Equal(t, recv.progress, int32(kProgressFinished))
				assert.Assert(t, recv.nodeId != "")
			}
			assert.Assert(t, part.hasFinished())
		}
		assert.Equal(t, task.execHistory.Len(), i+2)
		task.tryIssueTaskCommand()
		time.Sleep(time.Second * 2)
	}

	// check zk
	children, exist, succ := te.zkStore.Children(context.Background(), te.zkPath+"/checkpoint")
	assert.Equal(t, succ, true)
	assert.Equal(t, exist, true)
	assert.Equal(t, len(children), 4)
	var resp pb.QueryTaskResponse
	task.QueryTask(&resp)
	assert.Equal(t, len(resp.Infos), 4)

	last := task.execHistory.Back()
	lastStartSecond := last.Value.(*pb.TaskExecInfo).StartSecond
	lastFinishSecond := last.Value.(*pb.TaskExecInfo).FinishSecond

	taskDesc.FirstTriggerUnixSeconds = task.FirstTriggerUnixSeconds
	taskDesc.PeriodSeconds = task.PeriodSeconds
	taskDesc.NotifyMode = task.NotifyMode
	taskDesc.KeepNums = 1
	err := task.UpdateTask(taskDesc)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	task.TriggerDeleteTaskSideEffect()
	children, exist, succ = te.zkStore.Children(context.Background(), te.zkPath+"/checkpoint")
	assert.Equal(t, succ, true)
	assert.Equal(t, exist, true)
	assert.Equal(t, len(children), 1)

	assert.Equal(t, task.execHistory.Len(), 1)
	front := task.execHistory.Front()
	assert.Assert(t, front.Value.(*pb.TaskExecInfo).StartSecond == lastStartSecond)
	assert.Assert(t, front.Value.(*pb.TaskExecInfo).FinishSecond == lastFinishSecond)

	resp.Reset()
	task.QueryTask(&resp)
	assert.Equal(t, len(resp.Infos), 1)
}

func TestTaskCleanHistory(t *testing.T) {
	mockedHandlerName := "mocked_task_effect_handler"
	cmd.RegisterCmdHandler(
		mockedHandlerName,
		func(params *cmd_base.HandlerParams) cmd_base.CmdHandler {
			return &mockedTaskEffectHandler{
				params: params,
			}
		},
	)
	defer func() {
		cmd.UnregisterCmdHandler(mockedHandlerName)
	}()

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
		TaskName:                mockedHandlerName,
		FirstTriggerUnixSeconds: time.Now().Unix(),
		PeriodSeconds:           1,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
		Args:                    map[string]string{"a": "b"},
		KeepNums:                2,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	te.generateFullExecutionRound(t, task)
	assert.Equal(t, task.execHistory.Len(), 2)
	te.checkExecHistoryZkSynced(t, task)

	time.Sleep(time.Second)
	te.generateFullExecutionRound(t, task)
	// first fake one is cancelled
	assert.Equal(t, task.execHistory.Len(), 3)
	te.checkExecHistoryZkSynced(t, task)

	time.Sleep(time.Second)
	te.generateFullExecutionRound(t, task)
	assert.Equal(t, task.execHistory.Len(), 2)
	te.checkExecHistoryZkSynced(t, task)

	time.Sleep(time.Second)
	logging.Info("start a new round then start mimic")
	task.tryIssueTaskCommand()
	assert.Equal(t, task.execHistory.Len(), 3)
	te.checkExecHistoryZkSynced(t, task)

	task.FreezeCancelAndMimicExecution(
		map[string]string{"restore_path": "/tmp/colossusdb_test_checkpoint"},
	)
	assert.Equal(t, task.execHistory.Len(), 4)
	te.checkExecHistoryZkSynced(t, task)

	logging.Info("finish the mimic")
	task.tryIssueTaskCommand()
	assert.Equal(t, task.execHistory.Len(), 3)
	te.checkExecHistoryZkSynced(t, task)

	task.UnfreezeExecution()

	time.Sleep(time.Second)
	te.generateFullExecutionRound(t, task)
	assert.Equal(t, task.execHistory.Len(), 3)
	te.checkExecHistoryZkSynced(t, task)

	time.Sleep(time.Second)
	te.generateFullExecutionRound(t, task)
	assert.Equal(t, task.execHistory.Len(), 2)
	te.checkExecHistoryZkSynced(t, task)
}

func TestConcurrencyNodes(t *testing.T) {
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
		MaxConcurrencyPerNode:   5,
		MaxConcurrentNodes:      1,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
		Args:                    map[string]string{"a": "b"},
		KeepNums:                5,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	for i := range task.progress.partExecutions {
		assert.Assert(t, task.progress.partExecutions[i].receivers == nil)
	}

	check := func() {
		for i := 0; i < 6; i++ {
			// Wait for all nodes to complete their tasks
			task.tryIssueTaskCommand()
		}

		assert.Assert(t, task.execInfo.SessionId > 1)
		for i := range task.progress.partExecutions {
			part := &(task.progress.partExecutions[i])
			assert.Assert(t, len(part.receivers) > 0)
			for _, recv := range part.receivers {
				assert.Equal(t, recv.progress, int32(kProgressFinished))
				assert.Assert(t, recv.nodeId != "")
			}
			assert.Assert(t, part.hasFinished())
		}
		client := te.lp.GetClient("127.0.0.1:1001")
		assert.Assert(t, client.GetMetric("command") > 0)

		task.tryIssueTaskCommand()
		assert.Assert(t, task.execInfo.FinishSecond > 0)

		oldFinishTime := task.execInfo.FinishSecond
		task.tryIssueTaskCommand()
		assert.Equal(t, task.execInfo.FinishSecond, oldFinishTime)

	}
	check()

	// update MaxConcurrentNodes
	oldFirstTriggerUnixSeconds := taskDesc.FirstTriggerUnixSeconds

	perTasks := []int{2, 10, 1}
	for _, i := range perTasks {
		if i == 1 {
			te.table.nodes.MustGetNodeInfo("node1").IsAlive = false
			for i := range te.table.currParts {
				part := &(te.table.currParts[i])
				part.members.MembershipVersion = 3
				part.members.Peers["node1"] = pb.ReplicaRole_kLearner
				part.members.Peers["node2"] = pb.ReplicaRole_kPrimary
				part.members.Peers["node3"] = pb.ReplicaRole_kSecondary
			}
		}
		taskDesc.MaxConcurrentNodes = int32(i)
		err := task.UpdateTask(taskDesc)
		assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
		taskDesc.FirstTriggerUnixSeconds = 0
		err = task.UpdateTask(taskDesc)
		assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
		check()
		taskDesc.FirstTriggerUnixSeconds = oldFirstTriggerUnixSeconds
	}

	task2 := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))
	task2.LoadFromZookeeper(te.zkPath, checkpoint.TASK_NAME)
	defer task2.Stop()

	assert.Assert(t, proto.Equal(taskDesc, task2.PeriodicTask))
	assert.Equal(t, task.execInfo.SessionId, task2.execInfo.SessionId)
	assert.Equal(t, task.execInfo.FinishSecond, task2.execInfo.FinishSecond)
}

func TestConcurrencyReceiverDead(t *testing.T) {
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
		MaxConcurrencyPerNode:   5,
		MaxConcurrentNodes:      2,
		MaxConcurrentHubs:       2,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
		Args:                    map[string]string{"a": "b"},
		KeepNums:                5,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	for i := range task.progress.partExecutions {
		assert.Assert(t, task.progress.partExecutions[i].receivers == nil)
	}

	for i := 0; i < 6; i++ {
		if i == 1 {
			te.table.nodes.MustGetNodeInfo("node1").IsAlive = false
			for i := range te.table.currParts {
				part := &(te.table.currParts[i])
				part.members.MembershipVersion = 3
				part.members.Peers["node1"] = pb.ReplicaRole_kLearner
				part.members.Peers["node2"] = pb.ReplicaRole_kPrimary
				part.members.Peers["node3"] = pb.ReplicaRole_kSecondary
			}
		}
		// Wait for all nodes to complete their tasks
		task.tryIssueTaskCommand()
	}

	assert.Assert(t, task.execInfo.SessionId > 1)
	for i := range task.progress.partExecutions {
		part := &(task.progress.partExecutions[i])
		assert.Assert(t, len(part.receivers) > 0)
		for _, recv := range part.receivers {
			assert.Equal(t, recv.progress, int32(kProgressFinished))
			assert.Assert(t, recv.nodeId != "")
		}
		assert.Assert(t, part.hasFinished())
	}
	client := te.lp.GetClient("127.0.0.1:1002")
	assert.Assert(t, client.GetMetric("command") > 0)

	task.tryIssueTaskCommand()
	assert.Assert(t, task.execInfo.FinishSecond > 0)

	oldFinishTime := task.execInfo.FinishSecond
	task.tryIssueTaskCommand()
	assert.Equal(t, task.execInfo.FinishSecond, oldFinishTime)
}

func TestQueryTaskCurrentExecution(t *testing.T) {
	te := setupTaskTestEnv(t, [3]string{"STAGING", "STAGING", "STAGING"})
	defer te.teardown()

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

	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	for i := range task.progress.partExecutions {
		assert.Assert(t, task.progress.partExecutions[i].receivers == nil)
	}
	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()

	resp := pb.QueryTaskCurrentExecutionResponse{}
	task.QueryTaskCurrentExecution(&resp)
	assert.Equal(t, len(resp.Executions), 8)
	for _, execution := range resp.Executions {
		assert.Assert(t, execution != nil)
		assert.Equal(t, len(execution.ReplicaExecutions), 1)
		assert.Assert(t, execution.ReplicaExecutions[0].Node == "")
	}

	for i := range te.table.currParts {
		part := &(te.table.currParts[i])
		part.members.MembershipVersion = 3
		part.members.Peers["node1"] = pb.ReplicaRole_kPrimary
		part.members.Peers["node2"] = pb.ReplicaRole_kSecondary
		part.members.Peers["node3"] = pb.ReplicaRole_kSecondary
	}
	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()

	resp = pb.QueryTaskCurrentExecutionResponse{}
	task.QueryTaskCurrentExecution(&resp)
	assert.Equal(t, len(resp.Executions), 8)
	for _, execution := range resp.Executions {
		assert.Assert(t, execution != nil)
		assert.Equal(t, len(execution.ReplicaExecutions), 1)
		assert.Assert(t, execution.ReplicaExecutions[0].Node != "")
	}

}

func TestNotifyLeader(t *testing.T) {
	te := setupTaskTestEnv(t, [3]string{"STAGING", "STAGING", "STAGING"})
	defer te.teardown()

	for i := range te.table.currParts {
		part := &(te.table.currParts[i])
		part.members.MembershipVersion = 3
		part.members.Peers["node1"] = pb.ReplicaRole_kSecondary
		part.members.Peers["node2"] = pb.ReplicaRole_kSecondary
		part.members.Peers["node3"] = pb.ReplicaRole_kSecondary
	}

	task := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))

	taskDesc := &pb.PeriodicTask{
		TaskName:                checkpoint.TASK_NAME,
		FirstTriggerUnixSeconds: time.Now().Unix(),
		PeriodSeconds:           1,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_LEADER,
		Args:                    map[string]string{"a": "b"},
		KeepNums:                -1,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	for i := range task.progress.partExecutions {
		assert.Assert(t, task.progress.partExecutions[i].receivers == nil)
	}

	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()

	assert.Assert(t, task.execInfo.SessionId > 0)
	assert.Equal(t, task.execInfo.FinishSecond, int64(0))
	for i := range task.progress.partExecutions {
		part := &(task.progress.partExecutions[i])
		assert.Equal(t, len(part.receivers), 1)
		assert.Assert(t, part.receivers[0].nodeId == "")
	}

	for i := range te.table.currParts {
		part := &(te.table.currParts[i])
		part.members.MembershipVersion = 3
		part.members.Peers["node1"] = pb.ReplicaRole_kPrimary
	}

	task.tryIssueTaskCommand()
	task.tryIssueTaskCommand()

	for i := range task.progress.partExecutions {
		part := &(task.progress.partExecutions[i])
		assert.Assert(t, len(part.receivers) > 0)
		for _, recv := range part.receivers {
			assert.Equal(t, recv.progress, int32(kProgressFinished))
			assert.Assert(t, recv.nodeId == "node1")
		}
		assert.Assert(t, part.hasFinished())
	}
}

func TestNotifyAll(t *testing.T) {
	te := setupTaskTestEnv(t, [3]string{"STAGING", "STAGING", "STAGING"})
	defer te.teardown()

	for i := range te.table.currParts {
		part := &(te.table.currParts[i])
		part.members.MembershipVersion = 3
		part.members.Peers["node1"] = pb.ReplicaRole_kPrimary
		part.members.Peers["node2"] = pb.ReplicaRole_kSecondary
		part.members.Peers["node3"] = pb.ReplicaRole_kLearner
	}

	for i := 0; i < 2; i++ {
		task := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))
		taskDesc := &pb.PeriodicTask{
			TaskName:                checkpoint.TASK_NAME,
			FirstTriggerUnixSeconds: time.Now().Unix(),
			PeriodSeconds:           3600,
			NotifyMode:              pb.TaskNotifyMode_NOTIFY_ALL,
			Args:                    map[string]string{"a": "b"},
			KeepNums:                -1,
		}
		if i == 1 {
			taskDesc.TaskName = schema_change.TASK_NAME
		}
		task.InitializeNew(te.zkPath, taskDesc)
		defer task.Stop()

		assert.Equal(t, task.execInfo.SessionId, int64(1))
		assert.Assert(t, task.execInfo.FinishSecond > 0)
		for i := range task.progress.partExecutions {
			assert.Assert(t, task.progress.partExecutions[i].receivers == nil)
		}

		task.tryIssueTaskCommand()
		task.tryIssueTaskCommand()

		assert.Assert(t, task.execInfo.SessionId > 0)
		assert.Equal(t, task.execInfo.FinishSecond, int64(0))
		for i := range task.progress.partExecutions {
			part := &(task.progress.partExecutions[i])
			assert.Equal(t, len(part.receivers), 3)
		}

		task.tryIssueTaskCommand()
		task.tryIssueTaskCommand()
		time.Sleep(time.Second * 1) //wait task succ
		task.tryIssueTaskCommand()
		task.tryIssueTaskCommand()

		for i := range task.progress.partExecutions {
			part := &(task.progress.partExecutions[i])
			assert.Equal(t, len(part.receivers), 3)
			for _, recv := range part.receivers {
				assert.Equal(t, recv.progress, int32(kProgressFinished))
				assert.Assert(
					t,
					recv.nodeId == "node1" || recv.nodeId == "node2" || recv.nodeId == "node3",
				)
			}
			assert.Assert(t, part.hasFinished())
		}
	}
}

func TestConcurrencyhubs(t *testing.T) {
	te := setupTaskTestEnv(t, [3]string{"yz", "yz", "yz"})
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
		TaskName:                schema_change.TASK_NAME,
		FirstTriggerUnixSeconds: time.Now().Unix(),
		PeriodSeconds:           3600,
		MaxConcurrencyPerNode:   5,
		MaxConcurrentNodes:      0,
		MaxConcurrentHubs:       1,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
		Args:                    map[string]string{"a": "b"},
		KeepNums:                5,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	for i := range task.progress.partExecutions {
		assert.Assert(t, task.progress.partExecutions[i].receivers == nil)
	}

	check := func() {
		for i := 0; i < 6; i++ {
			// Wait for all nodes to complete their tasks
			task.tryIssueTaskCommand()
		}

		assert.Assert(t, task.execInfo.SessionId > 1)
		for i := range task.progress.partExecutions {
			part := &(task.progress.partExecutions[i])
			assert.Assert(t, len(part.receivers) == 1)
			for _, recv := range part.receivers {
				assert.Equal(t, recv.progress, int32(kProgressFinished))
				assert.Assert(t, recv.nodeId != "")
			}
			assert.Assert(t, part.hasFinished())
		}
		client := te.lp.GetClient("127.0.0.1:1001")
		assert.Assert(t, client.GetMetric("command") > 0)

		task.tryIssueTaskCommand()
		assert.Assert(t, task.execInfo.FinishSecond > 0)

		oldFinishTime := task.execInfo.FinishSecond
		task.tryIssueTaskCommand()
		assert.Equal(t, task.execInfo.FinishSecond, oldFinishTime)

	}
	check()

	// update MaxConcurrentNodes
	oldFirstTriggerUnixSeconds := taskDesc.FirstTriggerUnixSeconds

	perTasks := []int{0, 2, 8}
	for _, i := range perTasks {
		if i == 2 {
			te.table.nodes.MustGetNodeInfo("node1").IsAlive = false
			for i := range te.table.currParts {
				part := &(te.table.currParts[i])
				part.members.MembershipVersion = 3
				part.members.Peers["node1"] = pb.ReplicaRole_kLearner
				part.members.Peers["node2"] = pb.ReplicaRole_kPrimary
				part.members.Peers["node3"] = pb.ReplicaRole_kSecondary
			}
		}
		taskDesc.MaxConcurrentHubs = int32(i)
		taskDesc.MaxConcurrentNodes = int32(i) + 1
		err := task.UpdateTask(taskDesc)
		assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
		taskDesc.FirstTriggerUnixSeconds = 0
		err = task.UpdateTask(taskDesc)
		assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
		check()
		taskDesc.FirstTriggerUnixSeconds = oldFirstTriggerUnixSeconds
	}

	task2 := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))
	task2.LoadFromZookeeper(te.zkPath, schema_change.TASK_NAME)
	defer task2.Stop()

	assert.Assert(t, proto.Equal(taskDesc, task2.PeriodicTask))
	assert.Equal(t, task.execInfo.SessionId, task2.execInfo.SessionId)
	assert.Equal(t, task.execInfo.FinishSecond, task2.execInfo.FinishSecond)
}

func TestConcurrencyPartitions(t *testing.T) {
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
		MaxConcurrencyPerNode:   2,
		MaxConcurrentNodes:      1,
		NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
		Args:                    map[string]string{"a": "b"},
		KeepNums:                5,
	}
	task.InitializeNew(te.zkPath, taskDesc)
	defer task.Stop()

	assert.Equal(t, task.execInfo.SessionId, int64(1))
	assert.Assert(t, task.execInfo.FinishSecond > 0)
	for i := range task.progress.partExecutions {
		assert.Assert(t, task.progress.partExecutions[i].receivers == nil)
	}

	check := func() {
		for i := 0; i < 10; i++ {
			// Wait for all nodes to complete their tasks
			task.tryIssueTaskCommand()
		}

		assert.Assert(t, task.execInfo.SessionId > 1)
		for i := range task.progress.partExecutions {
			part := &(task.progress.partExecutions[i])
			assert.Assert(t, len(part.receivers) > 0)
			for _, recv := range part.receivers {
				assert.Equal(t, recv.progress, int32(kProgressFinished))
				assert.Assert(t, recv.nodeId != "")
			}
			assert.Assert(t, part.hasFinished())
		}
		client := te.lp.GetClient("127.0.0.1:1001")
		assert.Assert(t, client.GetMetric("command") > 0)

		task.tryIssueTaskCommand()
		assert.Assert(t, task.execInfo.FinishSecond > 0)

		oldFinishTime := task.execInfo.FinishSecond
		task.tryIssueTaskCommand()
		assert.Equal(t, task.execInfo.FinishSecond, oldFinishTime)

	}
	check()

	// update MaxConcurrentNodes
	oldFirstTriggerUnixSeconds := taskDesc.FirstTriggerUnixSeconds

	perTasks := []int{3, 10, 2}
	for _, i := range perTasks {
		if i == 2 {
			te.table.nodes.MustGetNodeInfo("node1").IsAlive = false
			for i := range te.table.currParts {
				part := &(te.table.currParts[i])
				part.members.MembershipVersion = 3
				part.members.Peers["node1"] = pb.ReplicaRole_kLearner
				part.members.Peers["node2"] = pb.ReplicaRole_kPrimary
				part.members.Peers["node3"] = pb.ReplicaRole_kSecondary
			}
		}
		taskDesc.MaxConcurrencyPerNode = int32(i)
		err := task.UpdateTask(taskDesc)
		assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
		taskDesc.FirstTriggerUnixSeconds = 0
		err = task.UpdateTask(taskDesc)
		assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
		check()
		taskDesc.FirstTriggerUnixSeconds = oldFirstTriggerUnixSeconds
	}

	task2 := NewTaskStat(te.table, WithScheduleCmdIntervalSecs(36000))
	task2.LoadFromZookeeper(te.zkPath, checkpoint.TASK_NAME)
	defer task2.Stop()

	assert.Assert(t, proto.Equal(taskDesc, task2.PeriodicTask))
	assert.Equal(t, task.execInfo.SessionId, task2.execInfo.SessionId)
	assert.Equal(t, task.execInfo.FinishSecond, task2.execInfo.FinishSecond)
}

func TestConcurrencyControl1(t *testing.T) {
	partNum := 8
	nodes := []string{"nodeId1", "nodeId2", "nodeId3"}
	replicas := []replicaExecution{
		{
			nodeId:      "nodeId1",
			nodeAddress: &utils.RpcNode{NodeName: "node1", Port: 2001},
			progress:    0,
			hub:         "hub1",
		},
		{
			nodeId:      "nodeId2",
			nodeAddress: &utils.RpcNode{NodeName: "node2", Port: 2002},
			progress:    0,
			hub:         "hub1",
		},
		{
			nodeId:      "nodeId3",
			nodeAddress: &utils.RpcNode{NodeName: "node3", Port: 2003},
			progress:    0,
			hub:         "hub2",
		},
	}

	progress := executionProgress{
		partExecutions:   make([]partitionExecution, partNum),
		currentSessionId: cmd_base.INVALID_SESSION_ID,
		concurrency:      concurrencyControl{},
	}
	control := &progress.concurrency
	control.reset()

	for i := 0; i < partNum; i++ {
		partExec := partitionExecution{
			finished: false,
		}
		partExec.receivers = append(partExec.receivers, &replicas[i%len(replicas)])
		progress.partExecutions[i] = partExec
	}
	control.adjustNodes(&progress)
	assert.Equal(t, len(control.hubNodes), 2)
	assert.Equal(t, len(control.allNodes), len(nodes))
	assert.Equal(t, len(control.runningHub), 0)
	assert.Equal(t, len(control.runningNodes), 0)

	// adjust error node id
	replicas[0].nodeId = ""
	control.adjustNodes(&progress)
	assert.Equal(t, len(control.hubNodes), 2)
	assert.Equal(t, len(control.allNodes), len(nodes)-1)
	assert.Equal(t, len(control.runningHub), 0)
	assert.Equal(t, len(control.runningNodes), 0)
	replicas[0].nodeId = "nodeId1"

	//adjust running node
	control.runningHub["hub3"] = 0
	control.runningNodes["nodeId4"] = make(map[int32]*replicaExecution)
	control.adjustNodes(&progress)
	assert.Equal(t, len(control.hubNodes), 2)
	assert.Equal(t, len(control.allNodes), len(nodes))
	assert.Equal(t, len(control.runningHub), 0)
	assert.Equal(t, len(control.runningNodes), 0)
}

func TestConcurrencyControl2(t *testing.T) {

	partNum := 8
	nodes := []string{"nodeId1", "nodeId2", "nodeId3"}
	replicas := []replicaExecution{
		{
			nodeId:      "nodeId1",
			nodeAddress: &utils.RpcNode{NodeName: "node1", Port: 2001},
			progress:    0,
			hub:         "hub1",
		},
		{
			nodeId:      "nodeId2",
			nodeAddress: &utils.RpcNode{NodeName: "node2", Port: 2002},
			progress:    0,
			hub:         "hub1",
		},
		{
			nodeId:      "nodeId3",
			nodeAddress: &utils.RpcNode{NodeName: "node3", Port: 2003},
			progress:    0,
			hub:         "hub2",
		},
	}

	progress := executionProgress{
		partExecutions:   make([]partitionExecution, partNum),
		currentSessionId: cmd_base.INVALID_SESSION_ID,
		concurrency:      concurrencyControl{},
	}
	control := &progress.concurrency
	control.reset()

	for i := 0; i < partNum; i++ {
		partExec := partitionExecution{
			finished: false,
		}
		partExec.receivers = append(partExec.receivers, &replicas[i%len(replicas)])
		progress.partExecutions[i] = partExec
	}
	control.adjustNodes(&progress)
	assert.Equal(t, len(control.hubNodes), 2)
	assert.Equal(t, len(control.allNodes), len(nodes))
	assert.Equal(t, len(control.runningHub), 0)
	assert.Equal(t, len(control.runningNodes), 0)

	context := concurrencyContext{
		maxConcurrentHubs:    0,
		maxConcurrentNodes:   0,
		maxConcurrentPerNode: 0,
	}
	waitingExecuted := control.concurrent(context)
	assert.Equal(t, len(waitingExecuted), 0)

	//adjust hub
	context.maxConcurrentHubs = 1
	waitingExecuted = control.concurrent(context)
	assert.Equal(t, len(control.runningHub), 1)
	if _, ok := control.runningHub["hub1"]; ok {
		assert.Equal(t, len(waitingExecuted), 2)
	} else {
		assert.Equal(t, len(waitingExecuted), 1)
	}

	//add hub
	context.maxConcurrentHubs = 2
	waitingExecuted = control.concurrent(context)
	assert.Equal(t, len(control.runningHub), 2)
	assert.Equal(t, len(waitingExecuted), 3)

	//adjust node
	control.runningNodes = make(map[string]map[int32]*replicaExecution)
	control.runningHub = make(map[string]int)
	context.maxConcurrentNodes = 1
	waitingExecuted = control.concurrent(context)
	assert.Equal(t, len(waitingExecuted), 1)
	assert.Equal(t, len(control.runningHub), 2)
	assert.Equal(t, len(control.runningNodes), 1)

	context.maxConcurrentNodes = 3
	waitingExecuted = control.concurrent(context)
	assert.Equal(t, len(waitingExecuted), 3)
	assert.Equal(t, len(control.runningHub), 2)
	assert.Equal(t, len(control.runningNodes), 3)

	//adjust partition
	control.runningNodes = make(map[string]map[int32]*replicaExecution)
	control.runningHub = make(map[string]int)
	context.maxConcurrentPerNode = 1
	waitingExecuted = control.concurrent(context)
	assert.Equal(t, len(waitingExecuted), 3)
	for _, replicas := range waitingExecuted {
		assert.Equal(t, len(replicas), 1)
	}
	assert.Equal(t, len(control.runningHub), 2)
	assert.Equal(t, len(control.runningNodes), 3)

	context.maxConcurrentPerNode = 2
	waitingExecuted = control.concurrent(context)
	assert.Equal(t, len(waitingExecuted), 3)
	for _, replicas := range waitingExecuted {
		assert.Equal(t, len(replicas), 2)
	}
	assert.Equal(t, len(control.runningHub), 2)
	assert.Equal(t, len(control.runningNodes), 3)
}

func TestConcurrencyControl3(t *testing.T) {
	partNum := 4
	nodes := []string{"nodeId1", "nodeId2", "nodeId3"}
	replicas := []replicaExecution{
		{
			nodeId:      "nodeId1",
			nodeAddress: &utils.RpcNode{NodeName: "node1", Port: 2001},
			progress:    0,
			hub:         "hub1",
		},
		{
			nodeId:      "nodeId2",
			nodeAddress: &utils.RpcNode{NodeName: "node2", Port: 2002},
			progress:    0,
			hub:         "hub1",
		},
		{
			nodeId:      "nodeId3",
			nodeAddress: &utils.RpcNode{NodeName: "node3", Port: 2003},
			progress:    0,
			hub:         "hub2",
		},
		{
			nodeId:      "nodeId1",
			nodeAddress: &utils.RpcNode{NodeName: "node1", Port: 2004},
			progress:    0,
			hub:         "hub1",
		},
	}

	progress := executionProgress{
		partExecutions:   make([]partitionExecution, partNum),
		currentSessionId: cmd_base.INVALID_SESSION_ID,
		concurrency:      concurrencyControl{},
	}
	control := &progress.concurrency
	control.reset()

	for i := 0; i < partNum; i++ {
		partExec := partitionExecution{
			finished: false,
		}
		partExec.receivers = append(partExec.receivers, &replicas[i])
		progress.partExecutions[i] = partExec
	}

	control.adjustNodes(&progress)
	assert.Equal(t, len(control.hubNodes), 2)
	assert.Equal(t, len(control.allNodes), len(nodes))
	assert.Equal(t, len(control.runningHub), 0)
	assert.Equal(t, len(control.runningNodes), 0)

	context := concurrencyContext{
		maxConcurrentHubs:    0,
		maxConcurrentNodes:   0,
		maxConcurrentPerNode: 0,
	}
	context.maxConcurrentHubs = 2
	waitingExecuted := control.concurrent(context)
	assert.Equal(t, len(waitingExecuted), 3)
	for nodeId, replicas := range waitingExecuted {
		if nodeId == "nodeId1" {
			assert.Equal(t, len(replicas), 2)
		} else {
			assert.Equal(t, len(replicas), 1)
		}
	}
	assert.Equal(t, len(control.runningHub), 2)
	assert.Equal(t, len(control.runningNodes), 3)

	progress.partExecutions[0].receivers[0].progress = kProgressFinished
	changeNode := progress.partExecutions[0].receivers[0].nodeId
	control.markIfAllFinished(progress.partExecutions)

	control.adjustNodes(&progress)
	waitingExecuted = control.concurrent(context)
	for _, replicas := range waitingExecuted {
		assert.Equal(t, len(replicas), 1)
	}
	changeReplicas := waitingExecuted[changeNode]
	assert.Equal(t, len(changeReplicas), 1)
	assert.Equal(t, len(control.runningNodes), 3)

	// node1 finished
	progress.partExecutions[partNum-1].receivers[0].progress = kProgressFinished
	changeNode = progress.partExecutions[0].receivers[0].nodeId
	control.markIfAllFinished(progress.partExecutions)
	control.adjustNodes(&progress)
	waitingExecuted = control.concurrent(context)
	for _, replicas := range waitingExecuted {
		assert.Equal(t, len(replicas), 1)
	}
	changeReplicas = waitingExecuted[changeNode]
	assert.Equal(t, len(changeReplicas), 0)
	assert.Equal(t, len(control.runningNodes), 2)

	// adjust all partition succeed
	for i := 0; i < partNum; i++ {
		progress.partExecutions[i].receivers[0].progress = kProgressFinished
	}
	control.markIfAllFinished(progress.partExecutions)
	control.adjustNodes(&progress)
	waitingExecuted = control.concurrent(context)
	assert.Equal(t, len(waitingExecuted), 0)
	assert.Equal(t, len(control.runningNodes), 0)
}
