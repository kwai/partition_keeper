package server

import (
	"container/list"
	"context"
	"flag"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd"
	cmd_base "github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
)

const (
	kTaskExecZNode    = "last"
	kProgressFinished = 100
	kMaxQueryTaskInfo = 10
)

var (
	flagScheduleCmdIntervalSecs = flag.Int64(
		"schedule_cmd_interval_secs",
		60,
		"schedule cmd interval secs",
	)
)

type replicaExecution struct {
	nodeId      string
	nodeAddress *utils.RpcNode
	progress    int32
	hub         string
}

type partitionExecution struct {
	receivers []*replicaExecution
	finished  bool
}

func (p *partitionExecution) matchSome(nodes []*node_mgr.NodeInfo) *replicaExecution {
	for _, recv := range p.receivers {
		for _, node := range nodes {
			if recv.nodeId == node.Id {
				return recv
			}
		}
	}
	return nil
}

func (p *partitionExecution) matchNode(node string) *replicaExecution {
	for _, recv := range p.receivers {
		if recv.nodeId == node {
			return recv
		}
	}
	return nil
}

func (p *partitionExecution) findReceiverEveryRegion(
	logName string,
	pid int32,
	belongTo *TableStats,
	recvCommands map[string]int,
) {
	regionNodes := map[string][]*node_mgr.NodeInfo{}
	for region := range utils.RegionsOfAzs(belongTo.getHubAzs()) {
		regionNodes[region] = nil
	}

	for node, role := range belongTo.currParts[pid].members.Peers {
		if role == pb.ReplicaRole_kLearner {
			continue
		}
		info := belongTo.nodes.MustGetNodeInfo(node)
		if !info.Servable() {
			continue
		}
		region := utils.RegionOfAz(info.Az)
		regionNodes[region] = append(regionNodes[region], info)
	}

	newReceivers := []*replicaExecution{}
	for rg, nodes := range regionNodes {
		if oldRecv := p.matchSome(nodes); oldRecv != nil {
			newReceivers = append(newReceivers, oldRecv)
			recvCommands[oldRecv.nodeId]++
		} else if len(nodes) > 0 {
			index := utils.FindMinIndex(len(nodes), func(i, j int) bool {
				leftId, rightId := nodes[i].Id, nodes[j].Id
				return recvCommands[leftId] < recvCommands[rightId]
			})
			selectNode := nodes[index]
			newReceivers = append(newReceivers, &replicaExecution{
				nodeId:      selectNode.Id,
				nodeAddress: selectNode.Address.Clone(),
				progress:    0,
				hub:         selectNode.Hub,
			})
			recvCommands[selectNode.Id]++
			logging.Info("%s.p%d: select %s as command receiver for region %s", logName, pid, selectNode.Id, rg)
		} else {
			newReceivers = append(newReceivers, &replicaExecution{
				nodeId:      "",
				nodeAddress: nil,
				progress:    0,
				hub:         "",
			})
			logging.Info("%s.p%d: can't find command receiver for region %s", logName, pid, rg)
		}
	}

	p.receivers = newReceivers
}

func (p *partitionExecution) findReceiverAll(
	logName string,
	pid int32,
	belongTo *TableStats,
	recvCommands map[string]int,
) {
	newReceivers := []*replicaExecution{}
	for node := range belongTo.currParts[pid].members.Peers {
		info := belongTo.nodes.MustGetNodeInfo(node)
		if !info.Servable() {
			continue
		}

		if oldRecv := p.matchNode(node); oldRecv != nil {
			newReceivers = append(newReceivers, oldRecv)
		} else {
			newReceivers = append(newReceivers, &replicaExecution{
				nodeId:      info.Id,
				nodeAddress: info.Address.Clone(),
				progress:    0,
				hub:         info.Hub,
			})
			logging.Info("%s.p%d: select %s as command receiver", logName, pid, info.Id)
		}
	}

	if len(newReceivers) == 0 {
		newReceivers = append(newReceivers, &replicaExecution{
			nodeId:      "",
			nodeAddress: nil,
			progress:    0,
			hub:         "",
		})
		logging.Info("%s.p%d: can't find command receiver", logName, pid)
	}
	p.receivers = newReceivers
}

func (p *partitionExecution) findReceiverLeader(
	logName string,
	pid int32,
	belongTo *TableStats,
	recvCommands map[string]int,
) {
	newReceivers := []*replicaExecution{}
	for node, role := range belongTo.currParts[pid].members.Peers {
		if role != pb.ReplicaRole_kPrimary {
			continue
		}
		info := belongTo.nodes.MustGetNodeInfo(node)
		if !info.Servable() {
			continue
		}
		if oldRecv := p.matchNode(node); oldRecv != nil {
			newReceivers = append(newReceivers, oldRecv)
		} else {
			newReceivers = append(newReceivers, &replicaExecution{
				nodeId:      info.Id,
				nodeAddress: info.Address.Clone(),
				progress:    0,
				hub:         info.Hub,
			})
			logging.Info("%s.p%d: select %s as command receiver", logName, pid, info.Id)
		}
	}
	if len(newReceivers) == 0 {
		newReceivers = append(newReceivers, &replicaExecution{
			nodeId:      "",
			nodeAddress: nil,
			progress:    0,
			hub:         "",
		})
		logging.Info("%s.p%d: can't find command receiver", logName, pid)
	}
	p.receivers = newReceivers
}

func (p *partitionExecution) adjustReceivers(
	logName string,
	notifyMode pb.TaskNotifyMode,
	pid int32,
	belongTo *TableStats,
	recvCommands map[string]int,
) {
	switch notifyMode {
	case pb.TaskNotifyMode_NOTIFY_EVERY_REGION:
		p.findReceiverEveryRegion(logName, pid, belongTo, recvCommands)
	case pb.TaskNotifyMode_NOTIFY_ALL:
		p.findReceiverAll(logName, pid, belongTo, recvCommands)
	case pb.TaskNotifyMode_NOTIFY_LEADER:
		p.findReceiverLeader(logName, pid, belongTo, recvCommands)
	default:
		logging.Error("%s.p%d: don't support notify mode: %s", logName, pid, notifyMode.String())
	}
}

func (p *partitionExecution) markIfAllFinished(logName string, pid int32) {
	if len(p.receivers) == 0 {
		return
	}
	for _, recv := range p.receivers {
		if recv.progress != kProgressFinished {
			return
		}
	}
	logging.Info("%s.p%d has finished as all replicas are finished", logName, pid)
	p.finished = true
}

func (p *partitionExecution) hasFinished() bool {
	return p.finished
}

func (p *partitionExecution) reset() {
	p.receivers = nil
	p.finished = false
}

type concurrencyContext struct {
	maxConcurrentHubs    int
	maxConcurrentNodes   int
	maxConcurrentPerNode int
}

func (c *concurrencyContext) needLimit() bool {
	if c.maxConcurrentHubs > 0 || c.maxConcurrentNodes > 0 || c.maxConcurrentPerNode > 0 {
		return true
	}
	return false
}

type concurrencyControl struct {
	hubNodes     map[string]map[string]int // hub -> nodes
	allNodes     map[string]map[int32]*replicaExecution
	runningNodes map[string]map[int32]*replicaExecution // nodeId -> (partId -> replicaExecution)
	runningHub   map[string]int

	logName string
}

func (c *concurrencyControl) reset() {
	c.hubNodes = make(map[string]map[string]int)
	c.allNodes = make(map[string]map[int32]*replicaExecution)
	c.runningNodes = make(map[string]map[int32]*replicaExecution)
	c.runningHub = make(map[string]int)
}

func (c *concurrencyControl) markIfAllFinished(execution []partitionExecution) {
	if len(c.runningNodes) == 0 {
		// When a node is dead during task execution, need to iterate through all the partitions to
		// check if the task is completed
		for i := range execution {
			execution[i].markIfAllFinished(c.logName, int32(i))
		}
		return
	}
	for nodeId, replicas := range c.runningNodes {
		for partId, replica := range replicas {
			execution[partId].markIfAllFinished(c.logName, partId)
			if replica.progress == kProgressFinished {
				delete(replicas, partId)
			}
		}
		if len(replicas) == 0 {
			logging.Info("%s: All tasks on the node:%s have been completed", c.logName, nodeId)
			delete(c.runningNodes, nodeId)
		}
	}
}

func (c *concurrencyControl) adjustNodes(progress *executionProgress) {
	c.hubNodes = make(map[string]map[string]int)
	c.allNodes = make(map[string]map[int32]*replicaExecution)
	for i := range progress.partExecutions {
		partitions := &(progress.partExecutions[i])
		for _, replica := range partitions.receivers {
			if replica.nodeId == "" || replica.progress == kProgressFinished {
				continue
			}

			if nodes, ok := c.hubNodes[replica.hub]; ok {
				nodes[replica.nodeId] = 0
			} else {
				tmpNodes := make(map[string]int)
				tmpNodes[replica.nodeId] = 0
				c.hubNodes[replica.hub] = tmpNodes
			}

			if replicas, ok := c.allNodes[replica.nodeId]; ok {
				replicas[int32(i)] = replica
			} else {
				tmpReplicas := make(map[int32]*replicaExecution)
				tmpReplicas[int32(i)] = replica
				c.allNodes[replica.nodeId] = tmpReplicas
			}
			logging.Info(
				"%s: concurrency adjust nodes p%d: select %s as command receiver",
				c.logName,
				i,
				replica.nodeId,
			)
		}
	}
	for k := range c.runningNodes {
		// Check the new round to see if the executing node still has tasks
		if _, ok := c.allNodes[k]; !ok {
			logging.Info(
				"%s: This node:%s is not selected to perform the task. Delete it",
				c.logName,
				k,
			)
			delete(c.runningNodes, k)
		}
	}
	for hub := range c.runningHub {
		if _, ok := c.hubNodes[hub]; !ok {
			delete(c.runningHub, hub)
			logging.Info("%s: All nodes have completed tasks on hub:%s", c.logName, hub)
		}
	}
}

func (c *concurrencyControl) concurrent(
	context concurrencyContext,
) map[string]map[int32]*replicaExecution {
	if !context.needLimit() {
		return make(map[string]map[int32]*replicaExecution)
	}

	// adjust running hubs
	if context.maxConcurrentHubs <= 0 {
		for hub := range c.hubNodes {
			c.runningHub[hub] = 0
			logging.Info("%s: add running hub:%s", c.logName, hub)
		}
	} else {
		for hub := range c.hubNodes {
			if len(c.runningHub) < context.maxConcurrentHubs {
				if _, ok := c.runningHub[hub]; !ok {
					c.runningHub[hub] = 0
					logging.Info("%s: add running hub:%s", c.logName, hub)
				}
			} else {
				break
			}
		}
	}

	// adjust running nodes
	for hub, nodes := range c.hubNodes {
		if _, ok := c.runningHub[hub]; !ok {
			continue
		}
		for nodeId := range nodes {
			if context.maxConcurrentNodes <= 0 {
				if _, ok := c.runningNodes[nodeId]; !ok {
					c.runningNodes[nodeId] = make(map[int32]*replicaExecution)
					logging.Info("%s: hub:%s add running node:%s", c.logName, hub, nodeId)
				}
				continue
			}

			if len(c.runningNodes) >= context.maxConcurrentNodes {
				break
			}

			if _, ok := c.runningNodes[nodeId]; !ok {
				c.runningNodes[nodeId] = make(map[int32]*replicaExecution)
				logging.Info("%s: hub:%s add running node:%s", c.logName, hub, nodeId)
			}
		}
	}

	// adjust running partitions
	for nodeId, runningPartInfos := range c.runningNodes {
		allPartInfos, ok := c.allNodes[nodeId]
		if !ok {
			logging.Error(
				"%s: This node:%s is not selected to perform the task. Delete it",
				c.logName,
				nodeId,
			)
			delete(c.runningNodes, nodeId)
			continue
		}

		if context.maxConcurrentPerNode <= 0 {
			c.runningNodes[nodeId] = allPartInfos
			continue
		}

		// Update the partition information in the running node
		for partId := range runningPartInfos {
			if info, ok := allPartInfos[partId]; !ok {
				logging.Error(
					"%s: This node:%s partition:%d partId not selected to perform the task. Delete it",
					c.logName,
					nodeId,
					partId,
				)
				delete(runningPartInfos, partId)
			} else {
				runningPartInfos[partId] = info
			}
		}

		// add partition in the running node
		for partId, info := range allPartInfos {
			if len(runningPartInfos) >= context.maxConcurrentPerNode {
				break
			}
			if _, ok := runningPartInfos[partId]; !ok {
				runningPartInfos[partId] = info
			}
		}
	}

	for nodeId, runningPartInfos := range c.runningNodes {
		partIds := ""
		for partId := range runningPartInfos {
			partIds = fmt.Sprintf("%s%d,", partIds, partId)
		}
		logging.Info("Adjust concurrency, sent to node:%s partitions is:%s", nodeId, partIds)
	}
	return c.runningNodes
}

type executionProgress struct {
	partExecutions   []partitionExecution
	currentSessionId int64
	concurrency      concurrencyControl
}

func (p *executionProgress) reset(currentSession int64) {
	for i := range p.partExecutions {
		p.partExecutions[i].reset()
	}
	p.currentSessionId = currentSession
}

type TaskStat struct {
	logName              string
	state                utils.DroppableStateHolder
	scheduleIntervalSecs int64
	belongTo             *TableStats
	zkPath               string
	zkConn               metastore.MetaStore
	nodesConn            *rpc.ConnPool
	progress             executionProgress
	quit                 chan bool

	mu sync.Mutex
	// these fields may be accessed and updated concurrently
	// by TaskStat's public api
	// and they are protected by mu
	*pb.PeriodicTask
	execInfo    *pb.TaskExecInfo
	execHistory *list.List
	regions     []string
}

type taskStatOpt func(t *TaskStat)

func WithScheduleCmdIntervalSecs(i int64) taskStatOpt {
	return func(t *TaskStat) {
		t.scheduleIntervalSecs = i
	}
}

func NewTaskStat(table *TableStats, opts ...taskStatOpt) *TaskStat {
	result := &TaskStat{
		state:                utils.DroppableStateHolder{State: utils.StateInitializing},
		scheduleIntervalSecs: *flagScheduleCmdIntervalSecs,
		belongTo:             table,
		zkConn:               table.zkConn,
		nodesConn:            table.nodesConn,
		progress: executionProgress{
			partExecutions:   make([]partitionExecution, table.PartsCount),
			currentSessionId: cmd_base.INVALID_SESSION_ID,
			concurrency: concurrencyControl{
				hubNodes:     make(map[string]map[string]int),
				allNodes:     make(map[string]map[int32]*replicaExecution),
				runningNodes: make(map[string]map[int32]*replicaExecution),
				runningHub:   make(map[string]int),
			},
		},
		quit: make(chan bool),
	}

	for _, opt := range opts {
		opt(result)
	}

	result.execHistory = list.New()
	return result
}

func (t *TaskStat) subpath(node string) string {
	return t.zkPath + "/" + node
}

func (t *TaskStat) getCmdHandler(needLock bool) cmd_base.CmdHandler {
	if needLock {
		t.mu.Lock()
		defer t.mu.Unlock()
	}
	info := &cmd_base.HandlerParams{
		TableId:     t.belongTo.TableId,
		ServiceName: t.belongTo.serviceName,
		TableName:   t.belongTo.TableName,
		TaskName:    t.TaskName,
		Regions:     t.regions,
		KconfPath:   t.belongTo.KconfPath,
		Args:        t.Args,
	}

	handler := cmd.NewCmdHandler(t.TaskName, info)
	return handler
}

func (t *TaskStat) LoadFromZookeeper(parentPath, node string) {
	t.zkPath = fmt.Sprintf("%s/%s", parentPath, node)
	data, exists, succ := t.zkConn.Get(context.Background(), t.zkPath)
	logging.Assert(succ && exists, "")

	property := &pb.PeriodicTask{}
	utils.UnmarshalJsonOrDie(data, property)
	t.PeriodicTask = property
	t.logName = fmt.Sprintf(
		"%s@%s t%d",
		property.TaskName,
		t.belongTo.TableName,
		t.belongTo.TableId,
	)
	t.progress.concurrency.logName = t.logName
	t.regions = utils.RegionListOfAzs(t.belongTo.getHubAzs())

	if t.KeepNums == 0 {
		// Compatible with older versions
		t.KeepNums = 5
		propData := utils.MarshalJsonOrDie(t.PeriodicTask)
		logging.Assert(t.zkConn.Set(context.Background(), t.zkPath, propData), "")
	}

	children, exist, succ := t.zkConn.Children(context.Background(), t.zkPath)
	logging.Assert(exist && succ, "")

	sort.Strings(children)
	lastTaskInfo := &pb.TaskExecInfo{}
	for _, c := range children {
		data, exists, succ := t.zkConn.Get(context.Background(), t.subpath(c))
		logging.Assert(exists && succ, "")
		if c == kTaskExecZNode {
			// only /last
			utils.UnmarshalJsonOrDie(data, lastTaskInfo)
		} else {
			taskInfo := &pb.TaskExecInfo{}
			utils.UnmarshalJsonOrDie(data, taskInfo)
			t.execHistory.PushBack(taskInfo)
		}
	}

	if t.execHistory.Len() == 0 {
		infoData := utils.MarshalJsonOrDie(lastTaskInfo)
		succ := t.zkConn.Create(
			context.Background(),
			t.getTimeStampTaskExecInfoPath(lastTaskInfo.SessionId),
			infoData,
		)
		logging.Assert(succ, "")
		t.execHistory.PushBack(lastTaskInfo)
	}
	t.execInfo = t.execHistory.Back().Value.(*pb.TaskExecInfo)

	go t.scheduleTaskExecution()

	t.state.Set(utils.StateNormal)
}

func (t *TaskStat) InitializeNew(parentPath string, taskDescription *pb.PeriodicTask) {
	t.zkPath = fmt.Sprintf("%s/%s", parentPath, taskDescription.TaskName)
	t.logName = fmt.Sprintf("%s.%s", t.belongTo.TableName, taskDescription.TaskName)

	propData := utils.MarshalJsonOrDie(taskDescription)

	t.execInfo = &pb.TaskExecInfo{}
	t.execInfo.StartSecond = taskDescription.FirstTriggerUnixSeconds - taskDescription.PeriodSeconds
	t.execInfo.FinishSecond = t.execInfo.StartSecond
	t.execInfo.FinishStatus = pb.TaskExecInfo_kCancelled
	t.execInfo.SessionId = 1
	infoData := utils.MarshalJsonOrDie(&(t.execInfo))

	succ := t.zkConn.MultiCreate(
		context.Background(),
		[]string{t.zkPath, t.getTimeStampTaskExecInfoPath(t.execInfo.SessionId)},
		[][]byte{propData, infoData},
	)
	logging.Assert(succ, "")
	t.execHistory.PushBack(t.execInfo)

	t.PeriodicTask = taskDescription
	t.progress.concurrency.logName = t.logName
	t.regions = utils.RegionListOfAzs(t.belongTo.getHubAzs())
	go t.scheduleTaskExecution()

	t.state.Set(utils.StateNormal)
}

func (t *TaskStat) disallowToStartNewRound() bool {
	return t.Freezed || t.Paused
}

func (t *TaskStat) setExecutionFreezedFlag(flag bool) {
	logging.Info("%s set freezed flag %v", t.logName, flag)
	t.PeriodicTask.Freezed = flag
	propData := utils.MarshalJsonOrDie(t.PeriodicTask)
	logging.Assert(t.zkConn.Set(context.Background(), t.zkPath, propData), "")
}

func (t *TaskStat) UpdateRegions(regions []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.regions = regions
}

func (t *TaskStat) PrepareMimic(args map[string]string) error {
	return t.getCmdHandler(true).CheckMimicArgs(args, t.belongTo.Table)
}

func (t *TaskStat) FreezeCancelAndMimicExecution(args map[string]string) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if reflect.DeepEqual(t.execInfo.Args, args) && t.PeriodicTask.Freezed {
		if t.execInfo.FinishSecond == 0 {
			return cmd_base.INVALID_SESSION_ID
		} else {
			return t.execInfo.SessionId
		}
	} else {
		t.setExecutionFreezedFlag(true)
		if !t.finishOneRoundInLock(t.execInfo.SessionId, pb.TaskExecInfo_kCancelled) {
			return cmd_base.INVALID_SESSION_ID
		} else {
			t.startNewRound(args)
			return cmd_base.INVALID_SESSION_ID
		}
	}
}

func (t *TaskStat) UnfreezeExecution() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.setExecutionFreezedFlag(false)
}

func (t *TaskStat) Stop() {
	logging.Info("%s: notify to stop task", t.logName)
	t.quit <- true
	logging.Info("%s: finish notifying to stop task", t.logName)
}

func (t *TaskStat) getTimeStampTaskExecInfoPath(time int64) string {
	return fmt.Sprintf("%s/%s", t.zkPath, strconv.FormatInt(time, 10))
}

func (t *TaskStat) startNewRound(args map[string]string) {
	newExecInfo := &pb.TaskExecInfo{
		SessionId:    t.execInfo.SessionId + 1,
		StartSecond:  time.Now().Unix(),
		FinishSecond: 0,
		Args:         args,
		FinishStatus: pb.TaskExecInfo_kFinished,
	}
	if newExecInfo.SessionId < newExecInfo.StartSecond {
		newExecInfo.SessionId = newExecInfo.StartSecond
	}
	logging.Info(
		"%s start a new round, session id: %d, args: %v",
		t.logName,
		newExecInfo.SessionId,
		args,
	)

	t.execInfo = newExecInfo
	infoData := utils.MarshalJsonOrDie(newExecInfo)

	borrowId := t.getCmdHandler(false).BorrowFromHistory(t.execHistory, args)
	if borrowId == cmd_base.INVALID_SESSION_ID {
		logging.Info("%s don't borrow any old session, just durable new session", t.logName)
		succ := t.zkConn.Create(
			context.Background(),
			t.getTimeStampTaskExecInfoPath(t.execInfo.SessionId),
			infoData,
		)
		logging.Assert(succ, "")
		t.execHistory.PushBack(t.execInfo)
	} else {
		createOp := &metastore.CreateOp{
			Path: t.getTimeStampTaskExecInfoPath(t.execInfo.SessionId),
			Data: infoData,
		}
		deleteOp := &metastore.DeleteOp{
			Path: t.getTimeStampTaskExecInfoPath(borrowId),
		}
		succ := t.zkConn.WriteBatch(context.Background(), createOp, deleteOp)
		if !succ {
			data, exists, succ := t.zkConn.Get(context.Background(), createOp.Path)
			logging.Assert(succ && exists, "")
			logging.Assert(reflect.DeepEqual(data, createOp.Data), "")
			_, exists, succ = t.zkConn.Get(context.Background(), deleteOp.Path)
			logging.Assert(succ && !exists, "")
		}
		t.execHistory.PushBack(t.execInfo)
	}
}

func (t *TaskStat) finishOneRoundInLock(sessionId int64, status pb.TaskExecInfo_FinishStatus) bool {
	logging.Info(
		"%s finish a round, session id: %d, status: %s",
		t.logName,
		t.execInfo.SessionId,
		status.String(),
	)

	if t.execInfo.SessionId != sessionId {
		logging.Info(
			"%s current session %d don't match %d",
			t.logName,
			t.execInfo.SessionId,
			sessionId,
		)
		return true
	}
	if t.execInfo.FinishSecond != 0 {
		logging.Info(
			"%s current session %d has already been finished with state: %s",
			t.logName,
			t.execInfo.SessionId,
			t.execInfo.FinishStatus.String(),
		)
		return true
	}
	// as we are in task's lock,
	// so it's not a big deal that whether we update
	// the memory stat first or remote meta store
	//
	// TODO: please make sure to update remote meta store first
	// then local memory if we want to optimize this code
	//
	// NOTICE: executionHistory.Back() is updated automatically
	// as it shares *pb.TaskExecInfo point with t.execInfo
	t.execInfo.FinishSecond = time.Now().Unix()
	t.execInfo.FinishStatus = status
	if !t.getCmdHandler(false).BeforeExecutionDurable(t.execHistory) {
		logging.Error(
			"%s: session %d failed to run before durable stage, stop to finish this round",
			t.logName,
			t.execInfo.SessionId,
		)
		third_party.PerfLog3(
			"reco",
			"reco.colossusdb.task_before_duration_fail",
			third_party.GetKsnNameByServiceName(t.belongTo.serviceName),
			t.belongTo.serviceName,
			t.belongTo.TableName,
			t.TaskName,
			1,
		)
		t.execInfo.FinishSecond = 0
		t.execInfo.FinishStatus = pb.TaskExecInfo_kFinished
		return false
	}
	cost_time := t.execInfo.FinishSecond - t.execInfo.StartSecond
	third_party.PerfLog3(
		"reco",
		"reco.colossusdb.task_cost_time_secs",
		third_party.GetKsnNameByServiceName(t.belongTo.serviceName),
		t.belongTo.serviceName,
		t.belongTo.TableName,
		t.TaskName,
		uint64(cost_time),
	)

	infoData := utils.MarshalJsonOrDie(t.execInfo)
	succ := t.zkConn.Set(
		context.Background(),
		t.getTimeStampTaskExecInfoPath(t.execInfo.SessionId),
		infoData,
	)
	logging.Assert(succ, "")
	t.tryCleanExecutionHistory()
	return true
}

func (t *TaskStat) finishOneRound(sessionId int64, status pb.TaskExecInfo_FinishStatus) {
	t.progress.concurrency.reset()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finishOneRoundInLock(sessionId, status)
}

func (t *TaskStat) adjustCommandReceiver(
	currentSession int64,
	concurrencyContext concurrencyContext,
) int {
	if t.progress.currentSessionId != currentSession {
		logging.Info(
			"%s: receiver's current session %d -> %d",
			t.logName,
			t.progress.currentSessionId,
			currentSession,
		)
		t.progress.reset(currentSession)
	}

	t.belongTo.serviceLock.LockRead()
	defer t.belongTo.serviceLock.UnlockRead()

	if int(t.belongTo.PartsCount) > len(t.progress.partExecutions) {
		for i := len(t.progress.partExecutions); i < int(t.belongTo.PartsCount); i++ {
			t.progress.partExecutions = append(
				t.progress.partExecutions,
				partitionExecution{nil, false},
			)
		}
	} else if int(t.belongTo.PartsCount) < len(t.progress.partExecutions) {
		t.progress.partExecutions = t.progress.partExecutions[0:t.belongTo.PartsCount]
	}

	finishedCount := 0
	recvCommands := map[string]int{}
	for i := range t.progress.partExecutions {
		pp := &(t.progress.partExecutions[i])
		if pp.hasFinished() {
			finishedCount++
		} else {
			pp.adjustReceivers(t.logName, t.NotifyMode, int32(i), t.belongTo, recvCommands)
		}
	}

	if concurrencyContext.needLimit() {
		t.progress.concurrency.adjustNodes(&t.progress)
	}

	return finishedCount
}

func (t *TaskStat) sendCommand(
	partId int32,
	replica *replicaExecution,
	currentSession *pb.TaskExecInfo,
) {
	if replica.progress == kProgressFinished {
		return
	}
	if replica.nodeId == "" {
		return
	}

	t.nodesConn.Run(replica.nodeAddress, func(client interface{}) error {
		psc := client.(pb.PartitionServiceClient)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		resp, err := psc.HandleCustomCommand(ctx, &pb.CustomCommandRequest{
			PartitionId:           partId,
			TableId:               t.belongTo.TableId,
			CommandSessionId:      currentSession.SessionId,
			CommandIssueTimestamp: currentSession.StartSecond,
			CommandName:           t.TaskName,
			Args:                  t.Args,
			AuthKey:               t.belongTo.namespace,
		})

		if err != nil {
			logging.Warning(
				"%s.p%d send command to %s:%v failed: %s",
				t.logName,
				partId,
				replica.hub,
				replica.nodeAddress,
				err.Error(),
			)
			return err
		}

		if resp.Status.Code != int32(pb.PartitionError_kOK) {
			logging.Warning(
				"%s.p%d send command to %s:%v response failed: %s",
				t.logName,
				partId,
				replica.hub,
				replica.nodeAddress,
				resp.String(),
			)
			replica.nodeId = ""
		} else {
			oldProgress := replica.progress
			replica.progress = resp.Progress
			if oldProgress != resp.Progress {
				logging.Info("%s.p%d hub:%s addr:%v nodeId:%s command progress: %d -> %d",
					t.logName, partId, replica.hub, replica.nodeAddress, replica.nodeId, oldProgress, resp.Progress)
			}
		}
		return nil
	})
}

func (t *TaskStat) issueTaskCommand(
	currentSession *pb.TaskExecInfo,
	concurrencyContext concurrencyContext,
) {
	finishedCount := t.adjustCommandReceiver(currentSession.SessionId, concurrencyContext)
	third_party.PerfLog1(
		"reco",
		"reco.colossusdb.task_progress",
		t.belongTo.serviceName,
		t.TaskName,
		uint64(finishedCount),
	)
	if finishedCount == len(t.progress.partExecutions) {
		t.finishOneRound(currentSession.SessionId, pb.TaskExecInfo_kFinished)
		return
	}
	logging.Info(
		"%s: total %d, finished: %d",
		t.logName,
		len(t.progress.partExecutions),
		finishedCount,
	)

	if !concurrencyContext.needLimit() {
		wg := sync.WaitGroup{}
		for i := range t.progress.partExecutions {
			pp := &(t.progress.partExecutions[i])
			for j := range pp.receivers {
				wg.Add(1)
				go func(partId int32, replica *replicaExecution) {
					defer wg.Done()
					t.sendCommand(partId, replica, currentSession)
				}(int32(i), pp.receivers[j])
			}
		}
		wg.Wait()
		for i := range t.progress.partExecutions {
			t.progress.partExecutions[i].markIfAllFinished(t.logName, int32(i))
		}
		return
	}

	waitingExecuted := t.progress.concurrency.concurrent(concurrencyContext)
	wg := sync.WaitGroup{}
	for _, replicas := range waitingExecuted {
		for partId, replica := range replicas {
			wg.Add(1)
			go func(partId int32, replica *replicaExecution) {
				defer wg.Done()
				t.sendCommand(partId, replica, currentSession)
			}(partId, replica)
		}
	}
	wg.Wait()
	t.progress.concurrency.markIfAllFinished(t.progress.partExecutions)
}

func (t *TaskStat) shouldStartNewRound() bool {
	if t.disallowToStartNewRound() {
		logging.Info("%s: don't start new round, freezed(%v), paused(%v)",
			t.logName,
			t.PeriodicTask.Freezed,
			t.PeriodicTask.Paused,
		)
		return false
	}

	now := time.Now().Unix()
	if t.FirstTriggerUnixSeconds > now {
		logging.Info(
			"%s: haven't reach first trigger timestamp: %d",
			t.logName,
			t.FirstTriggerUnixSeconds,
		)
		return false
	}
	periodId := (now - t.FirstTriggerUnixSeconds) / t.PeriodSeconds
	rangeStart := t.FirstTriggerUnixSeconds + periodId*t.PeriodSeconds
	rangeEnd := rangeStart + t.PeriodSeconds

	if t.execInfo.StartSecond >= rangeStart && t.execInfo.StartSecond < rangeEnd {
		logging.Info("%s: has schedule one round at %d for range [%d,%d), don't schedule a new one",
			t.logName,
			t.execInfo.StartSecond,
			rangeStart,
			rangeEnd,
		)
		return false
	}
	return true
}

func (t *TaskStat) tryIssueTaskCommand() {
	var currentSession *pb.TaskExecInfo
	t.mu.Lock()
	if t.Paused {
		t.mu.Unlock()
		logging.Info(
			"%s: the task has been paused and is no longer sending any requests to the node.",
			t.logName,
		)
		return
	}

	if t.execInfo.FinishSecond != 0 {
		if !t.shouldStartNewRound() {
			t.mu.Unlock()
			return
		}
		t.startNewRound(nil)
	} else {
		now := time.Now().Unix()
		if (now - t.execInfo.StartSecond) > t.PeriodSeconds {
			third_party.PerfLog3(
				"reco",
				"reco.colossusdb.task_too_long",
				third_party.GetKsnNameByServiceName(t.belongTo.serviceName),
				t.belongTo.serviceName,
				t.belongTo.TableName,
				t.TaskName,
				1)
		}
	}

	currentSession = proto.Clone(t.execInfo).(*pb.TaskExecInfo)
	isMimicked := (len(t.execInfo.Args) > 0)
	concurrencyContext := concurrencyContext{
		maxConcurrentHubs:    int(t.MaxConcurrentHubs),
		maxConcurrentNodes:   int(t.MaxConcurrentNodes),
		maxConcurrentPerNode: int(t.MaxConcurrencyPerNode),
	}

	t.mu.Unlock()

	if isMimicked {
		t.finishOneRound(currentSession.SessionId, pb.TaskExecInfo_kFinished)
	} else {
		t.issueTaskCommand(currentSession, concurrencyContext)
	}
}

func (t *TaskStat) UpdateTask(desc *pb.PeriodicTask) *pb.ErrorStatus {
	if desc.FirstTriggerUnixSeconds == 0 {
		return t.issueCommandNow()
	}

	if desc.FirstTriggerUnixSeconds < 0 || desc.FirstTriggerUnixSeconds == desc.PeriodSeconds {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"The first trigger unix seconds must be greater than the current time:%s ",
			time.Now().Format("2006-01-02 15:04:05"),
		)
	}

	if desc.PeriodSeconds <= 0 || desc.PeriodSeconds > 3600*24*365 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"period seconds should larger than 0 and less than a year:%d",
			3600*24*365,
		)
	}
	if desc.KeepNums <= 0 && desc.KeepNums != -1 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"keep nums should larger than 0 or equal to -1",
		)
	}

	if desc.MaxConcurrentNodes < 0 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"The max concurrent nodes should greater than or equal to 0",
		)
	}
	if desc.MaxConcurrentHubs < 0 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"The max concurrent hubs should greater than or equal to 0",
		)
	}
	if desc.MaxConcurrencyPerNode < 0 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"The max concurrent per node should greater than or equal to 0",
		)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.execInfo.FinishSecond == 0 {
		if t.NotifyMode != desc.NotifyMode {
			return pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"The task is in running. Do not modify parameter NotifyMode",
			)
		}
	}

	t.FirstTriggerUnixSeconds = desc.FirstTriggerUnixSeconds
	t.PeriodSeconds = desc.PeriodSeconds
	t.NotifyMode = desc.NotifyMode
	t.KeepNums = desc.KeepNums
	t.MaxConcurrentNodes = desc.MaxConcurrentNodes
	t.MaxConcurrentHubs = desc.MaxConcurrentHubs
	t.MaxConcurrencyPerNode = desc.MaxConcurrencyPerNode
	t.PeriodicTask.Paused = desc.Paused
	if len(desc.Args) > 0 {
		t.Args = desc.Args
	}

	propData := utils.MarshalJsonOrDie(t.PeriodicTask)
	logging.Assert(t.zkConn.Set(context.Background(), t.zkPath, propData), "")
	logging.Info("%s Update task succeed", t.belongTo.TableName)
	return pb.ErrStatusOk()
}

func (t *TaskStat) QueryTask(resp *pb.QueryTaskResponse) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if state := t.state.Get(); state != utils.StateNormal {
		resp.Status = pb.AdminErrorMsg(pb.AdminError_kTaskNotExists,
			"task %s is in state %s, not normal",
			t.belongTo.TableName,
			state.String())
		return
	}
	resp.Status = pb.AdminErrorCode(pb.AdminError_kOk)

	resp.Task = proto.Clone(t.PeriodicTask).(*pb.PeriodicTask)
	for iter := t.execHistory.Back(); iter != nil; iter = iter.Prev() {
		item := iter.Value.(*pb.TaskExecInfo)
		resp.Infos = append(resp.Infos, proto.Clone(item).(*pb.TaskExecInfo))
		if t.execHistory.Len() >= kMaxQueryTaskInfo {
			return
		}
	}
}

func (t *TaskStat) IsPaused() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.Paused
}

func (t *TaskStat) Finished() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.execInfo.FinishSecond != 0
}

func (t *TaskStat) QueryTaskCurrentExecution(resp *pb.QueryTaskCurrentExecutionResponse) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if state := t.state.Get(); state != utils.StateNormal {
		resp.Status = pb.AdminErrorMsg(pb.AdminError_kTaskNotExists,
			"task %s is in state %s, not normal",
			t.belongTo.TableName,
			state.String())
		return
	}
	resp.Status = pb.AdminErrorCode(pb.AdminError_kOk)
	if t.execInfo.FinishSecond != 0 {
		return
	}

	for i := range t.progress.partExecutions {
		pp := &(t.progress.partExecutions[i])
		succNum := 0
		partExecution := pb.TaskPartExecution{}

		for _, replica := range pp.receivers {
			if replica == nil {
				continue
			}
			replicaExecution := pb.TaskReplicaExecution{}
			if replica.nodeAddress != nil {
				replicaExecution.Node = fmt.Sprintf(
					"%s:%d",
					replica.nodeAddress.NodeName,
					replica.nodeAddress.Port,
				)
			}

			replicaExecution.Progress = replica.progress
			if replica.progress == kProgressFinished {
				succNum++
			}
			partExecution.ReplicaExecutions = append(
				partExecution.ReplicaExecutions,
				&replicaExecution,
			)
		}
		partExecution.PartitionId = int32(i)
		if succNum == len(pp.receivers) {
			partExecution.Finish = true
		}

		resp.Executions = append(resp.Executions, &partExecution)
	}
}

func (t *TaskStat) TriggerDeleteTaskSideEffect() *pb.ErrorStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tryCleanExecutionHistory()
	return pb.ErrStatusOk()
}

func (t *TaskStat) issueCommandNow() *pb.ErrorStatus {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.execInfo.FinishSecond == 0 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"%s already has a command running",
			t.logName,
		)
	}
	if t.disallowToStartNewRound() {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"%s already paused or freezed",
			t.logName,
		)
	}
	t.startNewRound(nil)
	return pb.ErrStatusOk()
}

func (t *TaskStat) tryCleanExecutionHistory() {
	if t.KeepNums == -1 {
		return
	}

	finishedCount := 0
	for iter := t.execHistory.Front(); iter != nil; iter = iter.Next() {
		execInfo := iter.Value.(*pb.TaskExecInfo)
		if execInfo.FinishStatus == pb.TaskExecInfo_kFinished {
			finishedCount++
		}
	}

	for finishedCount > int(t.KeepNums) {
		iter := t.execHistory.Front()
		execInfo := iter.Value.(*pb.TaskExecInfo)
		if !t.getCmdHandler(false).CleanOneExecution(execInfo) {
			logging.Warning(
				"Fail to delete task data, table:%s, taskName:%s, start second:%d",
				t.belongTo.TableName,
				t.TaskName,
				execInfo.StartSecond,
			)
			return
		}
		logging.Info(
			"Successfully deleted task:%s side effect, session id: %d, timestamp:%d",
			t.TaskName,
			execInfo.SessionId,
			execInfo.StartSecond,
		)

		t.zkConn.Delete(
			context.Background(),
			t.getTimeStampTaskExecInfoPath(execInfo.SessionId),
		)
		if execInfo.FinishStatus == pb.TaskExecInfo_kFinished {
			finishedCount--
		}
		t.execHistory.Remove(iter)
	}
}

func (t *TaskStat) scheduleTaskExecution() {
	logging.Info("%s: start schedule loop", t.logName)
	tick := time.NewTicker(time.Duration(t.scheduleIntervalSecs) * time.Second)
	for {
		select {
		case <-tick.C:
			t.tryIssueTaskCommand()
		case <-t.quit:
			tick.Stop()
			return
		}
	}
}
