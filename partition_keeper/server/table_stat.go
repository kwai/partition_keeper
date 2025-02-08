package server

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/delay_execute"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd"
	cmd_base "github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/checkpoint"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy"
	strategy_base "github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"google.golang.org/protobuf/proto"
)

const (
	kTasksZNode   = "tasks"
	kPartsZNode   = "parts"
	kRestoreZNode = "restore"
	kSplitZNode   = "split"

	kScheduleGrayscaleMin = 0
	kScheduleGrayscaleMax = 100
)

var (
	flagScheduleDelaySecs = flag.Int64(
		"schedule_delay_secs",
		0,
		"delay seconds for a table to start schedule",
	)
)

type scheduleDelayer struct {
	startTime time.Time
}

func newScheduleDelayer() *scheduleDelayer {
	return &scheduleDelayer{
		startTime: time.Now().Add(time.Second * time.Duration(*flagScheduleDelaySecs)),
	}
}

func (s *scheduleDelayer) shouldSchedule() bool {
	return !s.startTime.After(time.Now())
}

type PartitionCheckResult struct {
	HasAction bool
	Normal    bool
}

type TableStats struct {
	state       utils.DroppableStateHolder
	serviceName string
	namespace   string
	usePaz      bool

	// these fields are owned by the service_stat
	// make sure to update them when they are changed in service_stat side
	// [
	serviceLock            *utils.LooseLock
	nodes                  *node_mgr.NodeStats
	hubs                   []*pb.ReplicaHub
	svcStrategy            strategy.ServiceStrategy
	nodesConn              *rpc.ConnPool
	zkConn                 metastore.MetaStore
	delayedExecutorManager *delay_execute.DelayedExecutorManager
	baseTable              *TableStats
	// ]

	*pb.Table
	tasks             map[string]*TaskStat
	currParts         []partition
	plans             sched.SchedulePlan
	planConductor     *PlanConductor
	publisher         *RoutePublisher
	restoreCtrl       *TableRestoreController
	splitCtrl         *TableSplitController
	scheduler         sched.Scheduler
	hashGroups        int
	estimatedReplicas map[string]int

	parentZkPath string
	zkPath       string
	tasksPath    string
	partsPath    string

	// delay sometime to warm up
	// other resources out of partition_keeper
	//
	// currently it's only necessary for partition_server to
	// load a valid kconf as the kconf is created by kaiserving
	// after partition_keeper creates a table.
	schedDelayer *scheduleDelayer
}

func NewTableStats(
	serviceName, namespace string,
	usePaz bool,
	lock *utils.LooseLock,
	nodes *node_mgr.NodeStats,
	hubs []*pb.ReplicaHub,
	st strategy.ServiceStrategy,
	pool *rpc.ConnPool,
	conn metastore.MetaStore,
	delayedExecutorManager *delay_execute.DelayedExecutorManager,
	parentZkPath string,
	baseTable *TableStats,
) *TableStats {
	return &TableStats{
		state:                  utils.DroppableStateHolder{State: utils.StateInitializing},
		serviceName:            serviceName,
		namespace:              namespace,
		usePaz:                 usePaz,
		serviceLock:            lock,
		nodes:                  nodes,
		hubs:                   hubs,
		svcStrategy:            st,
		nodesConn:              pool,
		zkConn:                 conn,
		delayedExecutorManager: delayedExecutorManager,
		baseTable:              baseTable,
		tasks:                  make(map[string]*TaskStat),
		currParts:              nil,
		planConductor:          nil,
		parentZkPath:           parentZkPath,
		schedDelayer:           newScheduleDelayer(),
	}
}

func getTaskExecutorName(tableId int32, taskName string) string {
	return strconv.FormatInt(int64(tableId), 10) + "_" + taskName
}

func (d *TableStats) getHubAzs() map[string]bool {
	ans := map[string]bool{}
	for _, h := range d.hubs {
		ans[h.Az] = true
	}
	return ans
}

func (d *TableStats) GetInfo() *pb.Table {
	return d.Table
}

func (d *TableStats) GetMembership(partId int32) *table_model.PartitionMembership {
	return &(d.currParts[partId].members)
}

func (d *TableStats) UpdateNodeFact(nodeId string, info *pb.ReplicaReportInfo) bool {
	if info.PartitionId >= d.PartsCount {
		return false
	}
	// ensure other peers has been stripped out
	return d.currParts[info.PartitionId].updateFact(
		nodeId,
		info.PeerInfo.MembershipVersion,
		info.PeerInfo.SplitVersion,
		info.PeerInfo.Peers[0],
	)
}

func (d *TableStats) GetPbPartitionInfo(partId int32) *pb.PartitionInfo {
	result := &pb.PartitionInfo{
		PartitionId:    partId,
		TableId:        d.TableId,
		TableName:      d.TableName,
		TableJsonArgs:  d.JsonArgs,
		TableKconfPath: d.KconfPath,
		// TODO: handle different service type
		ServiceType:       "",
		PartitionNum:      d.PartsCount,
		TableSplitVersion: d.SplitVersion,
	}
	return result
}

func (d *TableStats) GetEstimatedReplicasOnNode(nid string) int {
	return d.estimatedReplicas[nid]
}

func (d *TableStats) GetEstimatedReplicas() map[string]int {
	return d.estimatedReplicas
}

func (d *TableStats) GetOccupiedResource() utils.HardwareUnit {
	output := utils.HardwareUnit{}
	average := d.svcStrategy.GetTableResource(d.JsonArgs)
	average.Divide(int64(d.PartsCount))

	for i := 0; i < len(d.currParts); i++ {
		partRes, sampled := d.currParts[i].getOccupiedResource()
		if sampled {
			partRes.Upper(average)
			output.Add(partRes)
		} else {
			output.Add(average)
		}
	}

	return output
}

func (d *TableStats) QueryTable(withPartitions, withTasks bool, resp *pb.QueryTableResponse) {
	if state := d.state.Get(); state != utils.StateNormal {
		resp.Status = pb.AdminErrorMsg(pb.AdminError_kTableNotExists,
			"table %s is in state %s, not normal, treat as not exist",
			d.TableName,
			state.String())
		return
	}
	resp.Status = pb.AdminErrorCode(pb.AdminError_kOk)

	resp.Table = proto.Clone(d.Table).(*pb.Table)
	if withPartitions {
		var i int32 = 0
		count := (int32)(utils.Min(int(d.PartsCount), 128))
		for ; i < count; i++ {
			resp.Partitions = append(
				resp.Partitions,
				d.currParts[i].toPbPartitionInfo(d.nodes, true),
			)
		}
	}
	if withTasks {
		for _, task := range d.tasks {
			if task.state.Get() == utils.StateNormal {
				resp.Tasks = append(resp.Tasks, proto.Clone(task.PeriodicTask).(*pb.PeriodicTask))
			}
		}
	}
}

func (d *TableStats) QueryPartition(
	fromPartition, toPartition int32,
	resp *pb.QueryPartitionResponse,
) {
	if state := d.state.Get(); state != utils.StateNormal {
		resp.Status = pb.AdminErrorMsg(pb.AdminError_kTableNotExists,
			"table %s is in state %s, not normal, treat as not exist",
			d.TableName,
			state.String())
		return
	}
	resp.Status = pb.AdminErrorCode(pb.AdminError_kOk)

	resp.TablePartsCount = d.Table.PartsCount

	var start, end int32 = 0, 0
	if fromPartition != 0 || toPartition != 0 {
		if fromPartition >= 0 {
			start = fromPartition
			end = (int32)(utils.Min(int(d.PartsCount), int(toPartition+1)))
		}
	} else {
		start = 0
		end = (int32)(utils.Min(int(d.PartsCount), 128))
	}

	for ; start < end; start++ {
		resp.Partitions = append(
			resp.Partitions,
			d.currParts[start].toPbPartitionInfo(d.nodes, true),
		)
	}
	if resp.Partitions == nil {
		resp.Partitions = make([]*pb.PartitionPeerInfo, 0)
	}

}

func (d *TableStats) Stop(clean bool) {
	if current, swapped := d.state.Cas(utils.StateNormal, utils.StateDropping); !swapped {
		logging.Info("%s table not in normal state: %s", d.serviceName, current.String())
		return
	}
	for _, task := range d.tasks {
		task.Stop()
	}
	d.publisher.Stop(clean)
	d.nodes.UnsubscribeUpdate(fmt.Sprintf("table-%d", d.TableId))
	d.nodes.GetEstReplicas().Update(d.TableId, nil)
	d.state.Set(utils.StateDropped)
}

func (d *TableStats) initializeZkPath(zkNode string) {
	d.zkPath = fmt.Sprintf("%s/%s", d.parentZkPath, zkNode)
	d.tasksPath = fmt.Sprintf("%s/%s", d.zkPath, kTasksZNode)
	d.partsPath = fmt.Sprintf("%s/%s", d.zkPath, kPartsZNode)
}

type tablePbFieldsExistenceChecker struct {
	ScheduleGrayscale *int32 `json:"schedule_grayscale"`
}

func (d *TableStats) loadProperties() {
	tableProps, exists, succ := d.zkConn.Get(context.Background(), d.zkPath)
	logging.Assert(succ && exists, "")

	checker := &tablePbFieldsExistenceChecker{}
	utils.UnmarshalJsonOrDie(tableProps, checker)

	tablePb := &pb.Table{}
	utils.UnmarshalJsonOrDie(tableProps, tablePb)
	if checker.ScheduleGrayscale == nil {
		tablePb.ScheduleGrayscale = kScheduleGrayscaleMax
	}

	d.Table = tablePb
	d.currParts = make([]partition, d.PartsCount)
	for i := 0; i < int(d.PartsCount); i++ {
		d.currParts[i].initialize(d, int32(i))
	}

	schedName, grp := d.svcStrategy.GetSchedulerInfo(d.Table)
	d.hashGroups = grp
	if schedName == sched.HASH_GROUP_SCHEDULER {
		d.scheduler = sched.NewScheduler(schedName, d.serviceName)
	} else {
		if d.baseTable == nil {
			d.scheduler = sched.NewScheduler(schedName, d.serviceName)
		} else {
			d.scheduler = sched.NewShadedScheduler(d.serviceName, d.baseTable)
		}
	}
	logging.Info(
		"%s %s: create scheduler of type %s, groups: %d, base table: %s",
		d.serviceName,
		d.TableName,
		d.scheduler.Name(),
		grp,
		d.Table.BaseTable,
	)
}

func (d *TableStats) loadRestoreCtrl() {
	ctrl := NewTableRestoreController(d, d.getRestoreCtrlPath())
	if ctrl.LoadFromZookeeper() {
		d.restoreCtrl = ctrl
	} else {
		d.restoreCtrl = nil
	}
}

func (d *TableStats) loadSplitCtrl() {
	ctrl := NewTableSplitController(d, d.getSplitCtrlPath())
	if !ctrl.LoadFromZookeeper() {
		d.splitCtrl = nil
		return
	}
	d.splitCtrl = ctrl
	if d.SplitVersion >= d.splitCtrl.stableInfo.NewSplitVersion {
		for pid := d.PartsCount / 2; pid < d.PartsCount; pid++ {
			part := &(d.currParts[pid])
			parentPart := &(d.currParts[pid-d.PartsCount/2])
			if !part.hasZkPath {
				logging.Assert(parentPart.members.SplitVersion == d.SplitVersion-1, "")
				part.members.SplitVersion = d.SplitVersion - 1
			} else {
				logging.Assert(parentPart.members.SplitVersion == part.members.SplitVersion, "")
			}
		}
	}
}

func (d *TableStats) getPartitionPath(partId int32) string {
	return fmt.Sprintf("%s/%d", d.partsPath, partId)
}

func (d *TableStats) getRestoreCtrlPath() string {
	return fmt.Sprintf("%s/%s", d.zkPath, kRestoreZNode)
}

func (d *TableStats) getSplitCtrlPath() string {
	return fmt.Sprintf("%s/%s", d.zkPath, kSplitZNode)
}

func (d *TableStats) loadPartition(partId int32) []byte {
	partPath := d.getPartitionPath(int32(partId))
	props, exists, succ := d.zkConn.Get(context.Background(), partPath)
	logging.Assert(succ && exists, "succ: %v, exists: %v", succ, exists)
	return props
}

func (d *TableStats) refreshEstimatedReplicas() {
	var newEst map[string]int
	var err error

	tableModel := d
	if d.baseTable != nil {
		tableModel = d.baseTable
	}
	if newEst, err = sched.EstimateTableReplicas(d.serviceName, d.nodes, tableModel); err != nil {
		return
	}
	for node := range d.nodes.AllNodes() {
		old, new := d.estimatedReplicas[node], newEst[node]
		if old != new {
			logging.Info(
				"%s: node %s table %s estimated replicas %d -> %d",
				d.serviceName,
				node,
				d.TableName,
				old,
				new,
			)
		}
	}
	d.estimatedReplicas = newEst
	d.nodes.GetEstReplicas().Update(d.TableId, d.estimatedReplicas)
}

func (d *TableStats) loadPartitions() {
	partData := make([][]byte, d.PartsCount)

	children, exists, succ := d.zkConn.Children(context.Background(), d.partsPath)
	logging.Assert(succ && exists, "")

	wg := sync.WaitGroup{}
	wg.Add(len(children))
	for _, child := range children {
		go func(node string) {
			defer wg.Done()
			partId, err := strconv.Atoi(node)
			if err != nil {
				logging.Fatal(
					"%s: can't parse node %s to partition meta: %s",
					d.TableName,
					node,
					err.Error(),
				)
			}
			partData[partId] = d.loadPartition(int32(partId))
		}(child)
	}
	wg.Wait()

	for i, data := range partData {
		if len(data) > 0 {
			d.currParts[i].hasZkPath = true
			utils.UnmarshalJsonOrDie(data, &(d.currParts[i].members))
		} else {
			d.currParts[i].hasZkPath = false
		}
	}
}

func (d *TableStats) RecordNodesReplicas(nr recorder.NodesRecorder, isFact bool) {
	if state := d.state.Get(); state != utils.StateNormal {
		logging.Warning(
			"%s.%s is in state %s, don't count replicas",
			d.serviceName,
			d.TableName,
			state.String(),
		)
		return
	}

	for i := int32(0); i < d.PartsCount; i++ {
		m := &(d.currParts[i])
		if isFact {
			for node, fact := range m.facts {
				nr.Add(node, d.TableId, i, fact.Role, m.getWeight())
			}
		} else {
			for node, role := range m.members.Peers {
				nr.Add(node, d.TableId, i, role, m.getWeight())
			}
		}
	}
}

func (d *TableStats) Record(nr recorder.NodesRecorder) {
	d.RecordNodesReplicas(nr, false)
}

func (d *TableStats) GetPartWeight(partId int32) int {
	return d.currParts[partId].getWeight()
}

func (d *TableStats) PartSchedulable(partId int32) bool {
	return d.currParts[partId].schedulable()
}

func (d *TableStats) CreateTask(taskDesc *pb.PeriodicTask) *pb.ErrorStatus {
	if taskDesc.FirstTriggerUnixSeconds <= 0 ||
		taskDesc.FirstTriggerUnixSeconds == taskDesc.PeriodSeconds {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"The first trigger unix seconds must be greater than the current time:%s",
			time.Now().Format("2006-01-02 15:04:05"),
		)
	}
	if taskDesc.PeriodSeconds <= 0 || taskDesc.PeriodSeconds > 3600*24*365 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"The period seconds should larger than 0 and less than a year:%d",
			3600*24*365,
		)
	}
	if taskDesc.MaxConcurrencyPerNode < 0 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"The max concurrency per node should greater than or equal to 0",
		)
	}
	if taskDesc.KeepNums <= 0 && taskDesc.KeepNums != -1 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"keep nums should larger than 0 or equal to -1",
		)
	}

	if taskDesc.MaxConcurrentNodes < 0 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"The max concurrent nodes should greater than or equal to 0",
		)
	}

	if taskDesc.MaxConcurrentHubs < 0 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"The max concurrent hubs should greater than or equal to 0",
		)
	}

	if !cmd.NameRegistered(taskDesc.TaskName) {
		return pb.AdminErrorMsg(
			pb.AdminError_kUnsupportedTaskName,
			"Unsupported task name:%s",
			taskDesc.TaskName,
		)
	}

	_, ok := d.tasks[taskDesc.TaskName]
	if ok {
		return pb.AdminErrorMsg(pb.AdminError_kTaskExists,
			"The task:%s already exists in table %s", taskDesc.TaskName, d.TableName)
	}

	if d.delayedExecutorManager.ExecutorExist(getTaskExecutorName(d.TableId, taskDesc.TaskName)) {
		return pb.AdminErrorMsg(
			pb.AdminError_kTaskExists,
			"This task:%s is not currently allowed to be created in the table:%s",
			taskDesc.TaskName,
			d.TableName,
		)
	}

	task := NewTaskStat(d)
	task.InitializeNew(d.tasksPath, taskDesc)
	d.tasks[task.TaskName] = task
	return pb.AdminErrorCode(pb.AdminError_kOk)
}

func (d *TableStats) DeleteTask(
	taskName string,
	cleanTaskSideEffect bool,
	cleanDelayMinutes int32,
) *pb.ErrorStatus {
	if state := d.state.Get(); state != utils.StateNormal {
		return pb.AdminErrorMsg(pb.AdminError_kTableNotExists,
			"table %s is in state %s, not normal, treat as not exist",
			d.TableName,
			state.String())
	}
	task := d.tasks[taskName]
	if task == nil {
		return pb.AdminErrorMsg(pb.AdminError_kTaskNotExists,
			"can't find task %s",
			taskName)
	}

	if cleanTaskSideEffect {
		var regions []string
		for region := range utils.RegionsOfAzs(d.getHubAzs()) {
			regions = append(regions, region)
		}
		now := time.Now().Unix()

		logging.Info("Table:%s delete task:%s side effect", d.TableName, taskName)
		executeTime := now + int64(cleanDelayMinutes*60)
		taskContext := cmd_base.HandlerParams{
			TableId:     d.TableId,
			ServiceName: d.serviceName,
			TableName:   d.TableName,
			TaskName:    taskName,
			Regions:     regions,
			KconfPath:   d.KconfPath,
			Args:        task.Args,
		}

		delayedExecuteInfo := delay_execute.ExecuteInfo{
			ExecuteType:    taskName,
			ExecuteName:    getTaskExecutorName(d.TableId, taskName),
			ExecuteTime:    executeTime,
			ExecuteContext: taskContext,
		}

		d.delayedExecutorManager.AddTask(delayedExecuteInfo)
		logging.Info(
			"Add delayed executor, service:%s, table:%s, task:%s, executor time:%d",
			d.serviceName,
			d.TableName,
			taskName,
			delayedExecuteInfo.ExecuteTime,
		)

	}
	task.Stop()
	succ := d.zkConn.RecursiveDelete(context.Background(), d.tasksPath+"/"+taskName)
	logging.Assert(succ, "")
	delete(d.tasks, taskName)
	return pb.ErrStatusOk()
}

func (d *TableStats) UpdateTask(taskDesc *pb.PeriodicTask) *pb.ErrorStatus {
	task, ok := d.tasks[taskDesc.TaskName]
	if !ok {
		return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter,
			"can't find task %s in table %s", taskDesc.TaskName, d.TableName)
	}
	return task.UpdateTask(taskDesc)
}

func (d *TableStats) StartSplit(req *pb.SplitTableRequest) *pb.ErrorStatus {
	if !d.scheduler.SupportSplit() {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"table %s is using %s scheduler, which doesn't support split",
			d.TableName,
			d.scheduler.Name(),
		)
	}
	if req.Options.MaxConcurrentParts <= 0 {
		return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, "max concurrent parts must > 0")
	}
	if d.restoreCtrl != nil {
		return pb.AdminErrorMsg(
			pb.AdminError_kWorkingInprogress,
			"%s %s running restore currently, skip split",
			d.serviceName,
			d.TableName,
		)
	}
	for name, task := range d.tasks {
		if !task.IsPaused() || !task.Finished() {
			return pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"%s %s please wait %s to finish and then pause it before split",
				d.serviceName,
				d.TableName,
				name,
			)
		}
	}
	for i := 0; i < int(d.PartsCount); i++ {
		if !d.currParts[i].hasZkPath && d.currParts[i].members.SplitVersion == d.SplitVersion {
			return pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"%s %s is not fully initialized, disallow split",
				d.serviceName,
				d.TableName,
			)
		}
	}

	if d.splitCtrl != nil {
		return d.splitCtrl.UpdateOptions(req.Options)
	} else {
		if d.SplitVersion+1 != req.NewSplitVersion {
			return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, "%s %s current split version %d, reject %d",
				d.serviceName, d.TableName, d.SplitVersion, req.NewSplitVersion,
			)
		}
		d.splitCtrl = NewTableSplitController(d, d.getSplitCtrlPath())
		d.splitCtrl.InitializeNew(req)
		return pb.ErrStatusOk()
	}
}

func (d *TableStats) StartRestore(req *pb.RestoreTableRequest) *pb.ErrorStatus {
	if d.splitCtrl != nil {
		return pb.AdminErrorMsg(
			pb.AdminError_kWorkingInprogress,
			"%s %s is splitting, reject to restore",
			d.serviceName,
			d.TableName,
		)
	}
	if d.tasks[checkpoint.TASK_NAME] == nil {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"%s %s don't have checkpoint task, don't support to do restore",
			d.serviceName,
			d.TableName,
		)
	}

	if req.Opts.MaxConcurrentNodesPerHub <= 0 && req.Opts.MaxConcurrentNodesPerHub != -1 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"%s %s MaxConcurrentNodesPerHub should larger than 0 or equal to -1",
			d.serviceName,
			d.TableName,
		)
	}

	if req.Opts.MaxConcurrentPartsPerNode <= 0 && req.Opts.MaxConcurrentPartsPerNode != -1 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"%s %s MaxConcurrentPartsPerNode should larger than 0 or equal to -1",
			d.serviceName,
			d.TableName,
		)
	}

	args := map[string]string{"restore_path": req.RestorePath}
	if err := d.tasks[checkpoint.TASK_NAME].PrepareMimic(args); err != nil {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"invalid restore path for %s %s: %s",
			d.serviceName,
			d.TableName,
			err.Error(),
		)
	}
	if d.restoreCtrl != nil {
		d.restoreCtrl.Update(req)
	} else {
		ctrl := NewTableRestoreController(d, d.getRestoreCtrlPath())
		ctrl.InitializeNew(req)
		d.restoreCtrl = ctrl
	}
	return pb.ErrStatusOk()
}

func (d *TableStats) loadTasks() {
	children, exists, succ := d.zkConn.Children(context.Background(), d.tasksPath)
	logging.Assert(succ && exists, "")
	for _, child := range children {
		task := NewTaskStat(d)
		task.LoadFromZookeeper(d.tasksPath, child)
		d.tasks[child] = task
	}

	if d.tasks[checkpoint.TASK_NAME] == nil {
		if d.svcStrategy.CreateCheckpointByDefault() || d.restoreCtrl != nil {
			defaultTask := &pb.PeriodicTask{
				TaskName: checkpoint.TASK_NAME,
				// first trigger checkpoint in 3 hours later
				FirstTriggerUnixSeconds: time.Now().Unix() + 7200,
				PeriodSeconds:           3600 * 24,
				MaxConcurrencyPerNode:   5,
				MaxConcurrentNodes:      0,
				MaxConcurrentHubs:       0,
				NotifyMode:              pb.TaskNotifyMode_NOTIFY_EVERY_REGION,
				Args:                    map[string]string{},
				KeepNums:                5,
			}
			d.CreateTask(defaultTask)
		}
	}
}

func (d *TableStats) QueryTask(taskName string, resp *pb.QueryTaskResponse) {
	if state := d.state.Get(); state != utils.StateNormal {
		resp.Status = pb.AdminErrorMsg(pb.AdminError_kTableNotExists,
			"table %s is in state %s, not normal, treat as not exist",
			d.TableName,
			state.String())
		return
	}
	task := d.tasks[taskName]
	if task == nil {
		resp.Status = pb.AdminErrorMsg(pb.AdminError_kTaskNotExists,
			"can't find task %s",
			taskName)
		return
	}
	task.QueryTask(resp)
}

func (d *TableStats) QueryTaskCurrentExecution(
	taskName string,
	resp *pb.QueryTaskCurrentExecutionResponse,
) {
	if state := d.state.Get(); state != utils.StateNormal {
		resp.Status = pb.AdminErrorMsg(pb.AdminError_kTableNotExists,
			"table %s is in state %s, not normal, treat as not exist",
			d.TableName,
			state.String())
		return
	}
	task := d.tasks[taskName]
	if task == nil {
		resp.Status = pb.AdminErrorMsg(pb.AdminError_kTaskNotExists,
			"can't find task %s",
			taskName)
		return
	}
	task.QueryTaskCurrentExecution(resp)
}

func (d *TableStats) TriggerDeleteTaskSideEffect(taskName string) *pb.ErrorStatus {
	task, ok := d.tasks[taskName]
	if !ok {
		return pb.AdminErrorMsg(pb.AdminError_kTaskNotExists,
			"can't find task %s in table %s", taskName, d.TableName)
	}
	return task.TriggerDeleteTaskSideEffect()
}

func (d *TableStats) CleanTaskSideEffect(cleanDelayMinutes int32) {
	var regions []string
	for region := range utils.RegionsOfAzs(d.getHubAzs()) {
		regions = append(regions, region)
	}
	now := time.Now().Unix()

	logging.Info("Table:%s clean task side effect, task num:%d", d.TableName, len(d.tasks))
	for taskName, task := range d.tasks {
		executeTime := now + int64(cleanDelayMinutes*60)
		taskContext := cmd_base.HandlerParams{
			TableId:     d.TableId,
			ServiceName: d.serviceName,
			TableName:   d.TableName,
			TaskName:    taskName,
			Regions:     regions,
			KconfPath:   d.KconfPath,
			Args:        task.Args,
		}

		delayedExecuteInfo := delay_execute.ExecuteInfo{
			ExecuteType:    taskName,
			ExecuteName:    getTaskExecutorName(d.TableId, taskName),
			ExecuteTime:    executeTime,
			ExecuteContext: taskContext,
		}

		d.delayedExecutorManager.AddTask(delayedExecuteInfo)
		logging.Info(
			"Add delayed executor, service:%s, table:%s, task:%s, executor time:%d",
			d.serviceName,
			d.TableName,
			taskName,
			delayedExecuteInfo.ExecuteTime,
		)
	}
}

func (d *TableStats) readRouteEntries(opts []*route.StoreOption) *pb.RouteEntries {
	store := route.NewCascadeStore(opts)
	var entries *pb.RouteEntries
	var err error
	for {
		entries, err = store.Get()
		if err != nil {
			logging.Warning("%s: recover entries failed: %s", d.TableName, err.Error())
			time.Sleep(time.Second)
			continue
		}
		break
	}
	// these fields are checked before restore is triggered
	// with the restore client admin
	logging.Assert(
		len(entries.ReplicaHubs) == len(d.hubs),
		"%s %s hubs not equal %d vs %d",
		d.serviceName,
		d.TableName,
		len(entries.ReplicaHubs),
		len(d.hubs),
	)
	logging.Assert(
		len(entries.Partitions) == int(d.PartsCount),
		"%s %s partition count not equal %d vs %d",
		d.serviceName,
		d.TableName,
		len(entries.Partitions),
		d.PartsCount,
	)

	// entries we got from route are biz ports, we must convert them to server ports
	biz2Server := map[string]*utils.RpcNode{}
	for _, np := range d.nodes.GetNodeLivenessMap(false) {
		bizAddr := fmt.Sprintf("%s:%d", np.Address.NodeName, np.BizPort)
		if current, ok := biz2Server[bizAddr]; ok {
			logging.Warning(
				"%s: %s and %s has conflict biz port %d, use latter",
				d.serviceName,
				current.String(),
				np.Address.String(),
				np.BizPort,
			)
		}
		biz2Server[bizAddr] = np.Address
	}
	for _, server := range entries.Servers {
		bizAddr := fmt.Sprintf("%s:%d", server.Host, server.Port)
		if node, ok := biz2Server[bizAddr]; ok {
			logging.Info(
				"%s: %s amend biz addr %s to server node %s",
				d.serviceName,
				d.TableName,
				bizAddr,
				node.String(),
			)
			server.Port = node.Port
		} else {
			logging.Warning("%s: %s can't find related server for biz server %s", d.serviceName, d.TableName, bizAddr)
		}
	}
	return entries
}

func (d *TableStats) durableRecoveredPartitions() {
	wg := sync.WaitGroup{}
	for i := range d.currParts {
		if !d.currParts[i].membershipChanged {
			continue
		}
		wg.Add(1)
		go func(index int32) {
			defer wg.Done()
			d.currParts[index].durableNewMembership()
			d.currParts[index].applyNewMembership()
		}(int32(i))
	}
	wg.Wait()
	logging.Info("%s: durable new partition finished, start to apply locally", d.TableName)
}

func (d *TableStats) applyRouteEntries(entries *pb.RouteEntries) {
	for i, part := range entries.Partitions {
		tblPart := &(d.currParts[i])
		if tblPart.members.MembershipVersion >= part.Version {
			logging.Info(
				"%s: %s t%d.p%d skip to recover as version maintained %d >= given %d, crash may happen",
				d.serviceName,
				d.TableName,
				d.TableId,
				i,
				tblPart.members.MembershipVersion,
				part.Version,
			)
			continue
		}
		tblPart.members.CloneTo(&(tblPart.newMembers))
		tblPart.newMembers.MembershipVersion = part.Version
		tblPart.membershipChanged = true
		if len(part.Replicas) == 0 {
			logging.Warning(
				"%s: %s t%d.p%d don't have any replica",
				d.serviceName,
				d.TableName,
				d.TableId,
				i,
			)
		} else {
			for _, rep := range part.Replicas {
				server := entries.Servers[rep.ServerIndex]
				rpcNode := utils.RpcNode{
					NodeName: server.Host,
					Port:     server.Port,
				}
				node := d.nodes.GetNodeInfoByAddr(&rpcNode, true)
				if node == nil {
					logging.Warning(
						"%s %s: can't get %s from node stats, remove it from part %d",
						d.serviceName,
						d.TableName,
						rpcNode.String(),
						i,
					)
					tblPart.newMembers.MembershipVersion++
				} else if !node.IsAlive && rep.Role != pb.ReplicaRole_kLearner {
					logging.Warning("%s: node %s has dead, transfer part %d from %s to learner",
						d.TableName,
						rpcNode.String(),
						i,
						rep.Role.String(),
					)
					tblPart.newMembers.MembershipVersion++
					tblPart.newMembers.Peers[node.Id] = pb.ReplicaRole_kLearner
				} else {
					tblPart.newMembers.Peers[node.Id] = rep.Role
				}
			}
		}
		logging.Info(
			"%s %s: t%d.p%d will recovery with members %v version %d, given version %d",
			d.serviceName,
			d.TableName,
			d.TableId,
			i,
			tblPart.newMembers.Peers,
			tblPart.newMembers.MembershipVersion,
			part.Version,
		)
	}
}

func (d *TableStats) recoverPartitions(opts []*route.StoreOption) {
	entries := d.readRouteEntries(opts)
	d.applyRouteEntries(entries)
	d.durableRecoveredPartitions()

	d.RecoverPartitionsFromRoute = false
	tableProps, err := json.Marshal(d.Table)
	if err != nil {
		logging.Fatal("marshall %v to json failed: %v", d.Table, err)
	}
	succ := d.zkConn.Set(context.Background(), d.zkPath, tableProps)
	logging.Assert(succ, "")
}

func (d *TableStats) verifyPartitions() {
	entries, err := d.publisher.routeStore.Get()
	if err != nil {
		logging.Warning("%s %s can't find entries from store: %s", d.serviceName, d.TableName, err)
		return
	}

	for i, part := range entries.Partitions {
		rv, pv := part.Version, d.currParts[i].members.MembershipVersion
		logging.Assert(rv <= pv, "%s %s: part %d route version %d vs keeper version %d",
			d.serviceName,
			d.TableName,
			i, rv, pv,
		)
	}
}

func (d *TableStats) LoadFromZookeeper(zkNode string) {
	d.initializeZkPath(zkNode)

	d.loadProperties()
	d.loadPartitions()
	d.loadRestoreCtrl()
	d.loadSplitCtrl()

	opts, err := d.svcStrategy.MakeStoreOpts(
		utils.RegionsOfAzs(d.getHubAzs()),
		d.serviceName,
		d.TableName,
		d.JsonArgs,
	)
	logging.Assert(err == nil, "check has been done in create table")

	d.publisher = NewRoutePublisher(d, opts)
	if d.RecoverPartitionsFromRoute {
		d.recoverPartitions(opts)
	} else {
		d.verifyPartitions()
	}
	d.publisher.Start()
	d.loadTasks()

	d.state.Set(utils.StateNormal)
	d.refreshEstimatedReplicas()
	d.nodes.SubscribeUpdate(fmt.Sprintf("table-%d", d.TableId), d.refreshEstimatedReplicas)
}

func (d *TableStats) InitializeNew(proto *pb.Table, restorePath string) {
	if proto.RecoverPartitionsFromRoute {
		proto.ScheduleGrayscale = kScheduleGrayscaleMin
	} else {
		proto.ScheduleGrayscale = kScheduleGrayscaleMax
	}
	tableProps, err := json.Marshal(proto)
	if err != nil {
		logging.Fatal("marshall %v to json failed: %v", d.Table, err)
	}
	d.initializeZkPath(proto.TableName)

	if restorePath == "" {
		d.zkConn.MultiCreate(
			context.Background(),
			[]string{d.zkPath, d.tasksPath, d.partsPath},
			[][]byte{tableProps, nil, nil},
		)
	} else {
		fakeReq := &pb.RestoreTableRequest{
			ServiceName: d.serviceName,
			TableName:   proto.TableName,
			RestorePath: restorePath,
			Opts: &pb.RestoreOpts{
				MaxConcurrentNodesPerHub:  -1,
				MaxConcurrentPartsPerNode: -1,
			},
		}
		restoreProps := SerializeRestoreStableInfo(fakeReq)
		d.zkConn.MultiCreate(
			context.Background(),
			[]string{d.zkPath, d.tasksPath, d.partsPath, d.getRestoreCtrlPath()},
			[][]byte{tableProps, nil, nil, restoreProps},
		)
	}
	d.LoadFromZookeeper(proto.TableName)
}

func (d *TableStats) updateMeta(updater func(tablePb *pb.Table)) {
	newProto := proto.Clone(d.Table).(*pb.Table)
	updater(newProto)
	tableProps := utils.MarshalJsonOrDie(newProto)
	d.serviceLock.AllowRead()
	logging.Assert(d.zkConn.Set(context.Background(), d.zkPath, tableProps), "")
	d.serviceLock.DisallowRead()
	d.Table = newProto
}

func (d *TableStats) UpdateTableGrayscale(input *pb.Table) *pb.ErrorStatus {
	if input.ScheduleGrayscale < kScheduleGrayscaleMin ||
		input.ScheduleGrayscale > kScheduleGrayscaleMax {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"grayscale value %d not within [%d,%d]",
			input.ScheduleGrayscale,
			kScheduleGrayscaleMin,
			kScheduleGrayscaleMax,
		)
	}
	updater := func(tablePb *pb.Table) {
		tablePb.ScheduleGrayscale = input.ScheduleGrayscale
	}
	d.updateMeta(updater)
	return pb.ErrStatusOk()
}

func (d *TableStats) UpdateTableJsonArgs(jsonArgs string) *pb.ErrorStatus {
	opts, err := d.svcStrategy.MakeStoreOpts(
		utils.RegionsOfAzs(d.getHubAzs()),
		d.serviceName,
		d.TableName,
		jsonArgs,
	)
	if err != nil {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"invalid json_args",
		)
	}
	d.publisher.UpdateStores(opts)

	updater := func(tablePb *pb.Table) {
		tablePb.JsonArgs = jsonArgs
	}
	d.updateMeta(updater)
	return pb.ErrStatusOk()
}

func (d *TableStats) UpdateHubs(hubs []*pb.ReplicaHub) {
	d.hubs = hubs
	opts, _ := d.svcStrategy.MakeStoreOpts(
		utils.RegionsOfAzs(d.getHubAzs()),
		d.serviceName,
		d.TableName,
		d.JsonArgs,
	)
	d.publisher.UpdateStores(opts)
	for _, task := range d.tasks {
		task.UpdateRegions(utils.RegionListOfAzs(d.getHubAzs()))
	}
	d.CancelPlan()
	logging.Info("%s: update hub to %v", d.TableName, d.hubs)
}

func (d *TableStats) StatsConsistency(reconcileIfNecessary bool) bool {
	inConsistency := true
	wg := sync.WaitGroup{}
	for i := int32(0); i < d.PartsCount; i++ {
		if reconcileIfNecessary {
			if !d.currParts[i].reconcileFacts(&wg) {
				inConsistency = false
			}
		} else {
			if !d.currParts[i].isConsistency(d.nodes) {
				inConsistency = false
			}
		}
	}
	return inConsistency
}

func (d *TableStats) shouldScheduleNode(nid string) bool {
	if d.ScheduleGrayscale >= kScheduleGrayscaleMax {
		return true
	}
	if d.ScheduleGrayscale <= kScheduleGrayscaleMin {
		return false
	}
	nodeOrder := d.nodes.MustGetNodeOrder(nid)
	nodePercentile := float32(nodeOrder+1) / float32(len(d.nodes.AllNodes()))
	grayscale := float32(d.ScheduleGrayscale) / float32(kScheduleGrayscaleMax-kScheduleGrayscaleMin)
	return nodePercentile <= grayscale
}

func (d *TableStats) optsForSvcStrategy() *strategy_base.TableRunningOptions {
	return &strategy_base.TableRunningOptions{
		IsRestoring: d.restoreCtrl != nil,
	}
}

func (d *TableStats) passiveTransform(recorder *recorder.AllNodesRecorder, partId int32) {
	part := &(d.currParts[partId])

	check := func(oldRole pb.ReplicaRole, nodeId string) pb.ReplicaRole {
		info := d.nodes.MustGetNodeInfo(nodeId)
		if !info.Normal() {
			return oldRole
		}
		newRole := part.checkPassiveTransform(nodeId)
		if newRole == pb.ReplicaRole_kInvalid {
			recorder.Remove(nodeId, d.TableId, partId, oldRole, part.getWeight())
			return pb.ReplicaRole_kInvalid
		}
		if newRole != oldRole {
			recorder.Transform(nodeId, d.TableId, partId, oldRole, newRole, part.getWeight())
			return newRole
		}
		return oldRole
	}

	// first transform primary, then other roles, as other role's state transform
	// are controlled by primary
	pri, secs, learners := part.members.DivideRoles()
	for _, primary := range pri {
		if check(pb.ReplicaRole_kPrimary, primary) != pb.ReplicaRole_kPrimary {
			return
		}
	}
	for _, sec := range secs {
		if check(pb.ReplicaRole_kSecondary, sec) != pb.ReplicaRole_kSecondary {
			return
		}
	}
	for _, learner := range learners {
		if check(pb.ReplicaRole_kLearner, learner) != pb.ReplicaRole_kLearner {
			return
		}
	}
}

func (d *TableStats) scheduleAsPlan() map[int32]bool {
	planedParts := map[int32]bool{}
	if len(d.plans) == 0 {
		return planedParts
	}

	for partition, partPlan := range d.plans {
		if partPlan.HasAction() {
			partPlan.ConsumeAction(&(d.currParts[partition]))
			planedParts[partition] = true
		}
	}

	if len(planedParts) > 0 {
		d.execPartitionCustomRunner()
		d.execPartitionUpdates()
	} else {
		d.plans = nil
	}
	return planedParts
}

func (d *TableStats) checkPartitionStatePassively(planedParts map[int32]bool) PartitionCheckResult {
	result := PartitionCheckResult{false, true}

	r := recorder.NewAllNodesRecorder(recorder.NewNodePartitions)
	d.RecordNodesReplicas(r, false)
	for i := int32(0); i < d.PartsCount; i++ {
		if _, ok := planedParts[i]; !ok {
			d.passiveTransform(r, i)
			if d.currParts[i].membershipChanged {
				result.HasAction = true
				result.Normal = false
			}
		}
	}

	d.execPartitionUpdates()
	return result
}

func (d *TableStats) CheckPartitions() PartitionCheckResult {
	planedParts := d.scheduleAsPlan()
	checkResult := d.checkPartitionStatePassively(planedParts)
	if len(planedParts) > 0 {
		return PartitionCheckResult{true, false}
	} else {
		return checkResult
	}
}

func (d *TableStats) RemoveReplicas(
	req *pb.ManualRemoveReplicasRequest,
	resp *pb.ManualRemoveReplicasResponse,
) {
	hasSuccess := 0
	for _, rep := range req.Replicas {
		var info *node_mgr.NodeInfo = nil
		if rep.NodeUniqId != "" {
			info = d.nodes.GetNodeInfo(rep.NodeUniqId)
		} else {
			info = d.nodes.GetNodeInfoByAddr(utils.FromPb(rep.Node), true)
		}
		subError := pb.ErrStatusOk()
		if info == nil {
			subError = pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"can't find node %s",
				rep.Node.ToHostPort(),
			)
		} else {
			if rep.PartitionId < 0 || rep.PartitionId >= d.PartsCount {
				subError = pb.AdminErrorMsg(
					pb.AdminError_kInvalidParameter,
					"invalid partition id %d",
					rep.PartitionId,
				)
			} else {
				part := &(d.currParts[rep.PartitionId])
				if part.members.HasMember(info.Id) {
					part.RemoveNode(info.Id)
					hasSuccess++
				} else {
					subError = pb.AdminErrorMsg(
						pb.AdminError_kInvalidParameter,
						"can't find %s in part %d",
						info.LogStr(),
						rep.PartitionId,
					)
				}
			}
		}
		resp.ReplicasResult = append(resp.ReplicasResult, subError)
	}

	if hasSuccess > 0 {
		logging.Info(
			"%s %s t%d %d replicas removed, cancel plan",
			d.serviceName,
			d.TableName,
			d.TableId,
			hasSuccess,
		)
		d.CancelPlan()
	}
	d.execPartitionUpdates()
}

func (d *TableStats) tryPartitionDowngrade(partId int32, nodes map[string]bool) {
	p := &(d.currParts[partId])
	// if don't have a zk path, the partition surely don't contain node
	if !p.hasZkPath {
		return
	}

	var updateFacts []string = nil
	var transform []string = nil

	for node, role := range p.members.Peers {
		if _, ok := nodes[node]; ok {
			if role != pb.ReplicaRole_kLearner {
				logging.Warning(
					"%s t%d.p%d node %s role %s is dead, downgrade to learner",
					d.TableName,
					d.TableId,
					partId,
					node,
					role.String(),
				)
				transform = append(transform, node)
				updateFacts = append(updateFacts, node)
			} else {
				updateFacts = append(updateFacts, node)
			}
		}
	}

	p.members.CloneTo(&p.newMembers)
	if len(transform) > 0 {
		for _, n := range transform {
			p.newMembers.Transform(n, pb.ReplicaRole_kLearner)
		}
		p.membershipChanged = true
	}
	for _, n := range updateFacts {
		p.updateDeadFact(n, p.newMembers.MembershipVersion)
	}
}

func (d *TableStats) MarkDeadNodes(nodes map[string]bool) {
	d.CancelPlan()
	for i := int32(0); i < d.PartsCount; i++ {
		d.tryPartitionDowngrade(i, nodes)
	}
	d.execPartitionUpdates()
}

func (d *TableStats) AddPartitionPlan(pid int32, act *actions.PartitionActions) {
	if d.plans == nil {
		d.plans = make(map[int32]*actions.PartitionActions)
	}
	d.plans[pid] = act
}

func (d *TableStats) AddPlan(p sched.SchedulePlan) {
	d.plans = p
}

func (d *TableStats) ApplyPlan(p sched.SchedulePlan) {
	d.plans = p
	d.scheduleAsPlan()
}

func (d *TableStats) CancelPlan() {
	logging.Info("%s %s: cancel running plan", d.serviceName, d.TableName)
	d.plans = nil
}

func (d *TableStats) RefreshRestoreVersion() bool {
	if d.restoreCtrl == nil {
		return true
	}
	if d.restoreCtrl.stats.Stage != RestorePending {
		return true
	}
	checkpointTask := d.tasks[checkpoint.TASK_NAME]
	logging.Assert(checkpointTask != nil, "")

	args := map[string]string{
		"restore_path": d.restoreCtrl.stats.RestorePath,
	}
	ts := checkpointTask.FreezeCancelAndMimicExecution(args)
	if ts != cmd_base.INVALID_SESSION_ID {
		logging.Info("%s %s: generate new task session id %d", d.serviceName, d.TableName, ts)
		d.updateMeta(func(tablePb *pb.Table) {
			tablePb.RestoreVersion = ts
		})
		d.restoreCtrl.StartRunning()
		return true
	} else {
		logging.Info("%s %s: generating new task session id, please wait a while", d.serviceName, d.TableName)
		return false
	}
}

func (d *TableStats) ScheduleRestoreIfNecessary() {
	if d.restoreCtrl == nil {
		return
	}
	plan, finished := d.restoreCtrl.GenerateRestorePlan()
	if len(plan) > 0 {
		d.ApplyPlan(plan)
	} else if finished {
		logging.Info("%s %s: restore has finished, teardown the controller", d.serviceName, d.TableName)
		d.tasks[checkpoint.TASK_NAME].UnfreezeExecution()
		logging.Assert(d.zkConn.Delete(context.Background(), d.getRestoreCtrlPath()), "")
		d.restoreCtrl = nil
	}
}

func (d *TableStats) updateSplitMetadata() {
	if d.SplitVersion >= d.splitCtrl.stableInfo.NewSplitVersion {
		return
	}
	d.updateMeta(func(tablePb *pb.Table) {
		tablePb.SplitVersion += 1
		tablePb.PartsCount *= 2
	})

	expandSize := int(d.PartsCount) - len(d.currParts)
	for i := 0; i < expandSize; i++ {
		d.currParts = append(d.currParts, partition{})
	}
	for i := int(d.PartsCount) - expandSize; i < int(d.PartsCount); i++ {
		d.currParts[i].initialize(d, int32(i))
		d.currParts[i].members.SplitVersion = d.SplitVersion - 1
	}
	d.refreshEstimatedReplicas()
}

func (d *TableStats) ScheduleSplitIfNecessary() {
	if d.splitCtrl == nil {
		return
	}
	d.updateSplitMetadata()
	plan, finished := d.splitCtrl.GeneratePlan()
	if len(plan) > 0 {
		d.ApplyPlan(plan)
	} else if finished {
		logging.Assert(d.zkConn.Delete(context.Background(), d.getSplitCtrlPath()), "")
		d.splitCtrl.Teardown()
		d.splitCtrl = nil
	}
}

func (d *TableStats) prepareSchedule(schedOpts *pb.ScheduleOptions) {
	d.planConductor = NewPlanConductor(schedOpts, d)
}

func (d *TableStats) Schedule(doSchedule bool, schedOpts *pb.ScheduleOptions) {
	if !d.schedDelayer.shouldSchedule() {
		logging.Info(
			"%s %s: don't start schedule until at %s",
			d.serviceName,
			d.TableName,
			d.schedDelayer.startTime.Format(time.UnixDate),
		)
		return
	}
	if !d.RefreshRestoreVersion() {
		logging.Info(
			"%s %s: restore version hasn't refreshed yet, don't do any schedule",
			d.serviceName,
			d.TableName,
		)
		return
	}
	if !d.StatsConsistency(true) {
		logging.Info(
			"%s %s: facts aren't in consistency with expect, return",
			d.serviceName,
			d.TableName,
		)
		return
	}
	d.prepareSchedule(schedOpts)
	checkRes := d.CheckPartitions()
	if checkRes.HasAction {
		logging.Info("%s %s: check partition has plan, return", d.serviceName, d.TableName)
		return
	}
	if !checkRes.Normal {
		logging.Info("%s %s: not normal, return", d.serviceName, d.TableName)
		return
	}
	if !doSchedule {
		logging.Info("%s %s: schedule is disabled, return", d.serviceName, d.TableName)
		return
	}
	scheduleInput := sched.ScheduleInput{}
	scheduleInput.Table = d
	scheduleInput.Nodes = d.nodes
	scheduleInput.Hubs = d.hubs
	scheduleInput.Opts.Configs = schedOpts
	scheduleInput.Opts.PrepareRestoring = (d.restoreCtrl != nil)
	scheduleInput.Opts.PartGroupCnt = d.hashGroups
	plan := d.scheduler.Schedule(&scheduleInput)
	if len(plan) > 0 {
		logging.Info("%s %s: has generate balance plan, return", d.serviceName, d.TableName)
		d.ApplyPlan(plan)
		return
	}
	d.ScheduleRestoreIfNecessary()
	d.ScheduleSplitIfNecessary()
}

func (d *TableStats) propagateUpdatesInSplitChain() ([][]int32, map[int32]bool) {
	updateGroups := [][]int32{}
	grouped := map[int32]bool{}

	for i := int32(0); i < d.PartsCount/2; i++ {
		parent := &(d.currParts[i])
		child := &(d.currParts[i+d.PartsCount/2])
		logging.Assert(parent.members.SplitVersion == child.members.SplitVersion, "")
		if parent.members.SplitVersion < d.SplitVersion {
			if child.oldChildPrimaryRecentElected() {
				parent.incSplitVersion()
				child.incSplitVersion()
				updateGroups = append(updateGroups, []int32{parent.partId, child.partId})
				grouped[parent.partId] = true
				grouped[child.partId] = true
			} else if parent.oldParentChangePrimary() && child.hasZkPath {
				child.incMembershipVersion()
				updateGroups = append(updateGroups, []int32{parent.partId, child.partId})
				grouped[parent.partId] = true
				grouped[child.partId] = true
			}
		}
	}
	return updateGroups, grouped
}

func (d *TableStats) execPartitionUpdates() {
	updateGroups, grouped := d.propagateUpdatesInSplitChain()
	for i := int32(0); i < d.PartsCount; i++ {
		if d.currParts[i].membershipChanged && !grouped[i] {
			updateGroups = append(updateGroups, []int32{i})
		}
	}
	if len(updateGroups) == 0 {
		return
	}

	d.serviceLock.AllowRead()
	defer d.serviceLock.DisallowRead()

	wg := sync.WaitGroup{}
	wg.Add(len(updateGroups))
	for _, update := range updateGroups {
		go func(indexes []int32) {
			defer wg.Done()
			ops := []metastore.WriteOp{}
			for _, index := range indexes {
				ops = append(ops, d.currParts[index].getDurableMembershipOp())
			}
			d.zkConn.WriteBatch(context.Background(), ops...)
		}(update)
	}
	wg.Wait()

	logging.Info("%s: durable new partition finished, start to apply locally", d.TableName)

	d.serviceLock.DisallowRead()
	for _, indexes := range updateGroups {
		for _, update := range indexes {
			d.currParts[update].applyNewMembership()
		}
	}
	d.serviceLock.AllowRead()

	logging.Info("%s: apply new partition finished, start to notify nodes", d.TableName)
	for _, indexes := range updateGroups {
		for _, update := range indexes {
			d.currParts[update].reconcileFacts(&wg)
		}
	}
	wg.Wait()
	logging.Info("%s: notify nodes finished, start to publish", d.TableName)
	d.publisher.NotifyChange()
}

func (d *TableStats) execPartitionCustomRunner() {
	d.serviceLock.AllowRead()
	defer d.serviceLock.DisallowRead()
	wg := sync.WaitGroup{}
	logging.Info("%s: start to notify nodes run schedule action custom runners", d.TableName)
	for i := int32(0); i < d.PartsCount; i++ {
		if d.currParts[i].customRunner != nil {
			d.currParts[i].customRunner(
				utils.MakeTblPartID(d.TableId, i),
				&wg,
			)
			d.currParts[i].customRunner = nil
		}
	}
	wg.Wait()
	logging.Info("%s: notify nodes run schedule action custom runners finished", d.TableName)
}

func (d *TableStats) partsCountMonitor(tableName string) {
	totalLearner := 0
	totalErrorCount := 0
	for i := 0; i < len(d.currParts); i++ {
		partition := &(d.currParts[i])
		learner := 0
		secondary := 0
		primary := 0
		for _, role := range partition.members.Peers {
			switch role {
			case pb.ReplicaRole_kLearner:
				learner++
				totalLearner++
			case pb.ReplicaRole_kSecondary:
				secondary++
			case pb.ReplicaRole_kPrimary:
				primary++
			}
		}
		if learner > 0 || primary != 1 {
			totalErrorCount++
		}
	}
	third_party.PerfLog1(
		"reco",
		"reco.colossusdb.replicas.abnormal_count",
		d.serviceName,
		tableName,
		uint64(totalErrorCount),
	)
	third_party.PerfLog1(
		"reco",
		"reco.colossusdb.learner_count",
		d.serviceName,
		tableName,
		uint64(totalLearner),
	)
}
