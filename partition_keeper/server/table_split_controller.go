package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
)

type SplitStableInfo struct {
	*pb.SplitTableOptions
	NewSplitVersion int32 `json:"new_split_version"`
}

type TableSplitController struct {
	logName string
	table   *TableStats

	stableInfo SplitStableInfo
	zkPath     string

	lastSplitSecond int64
}

func NewTableSplitController(table *TableStats, zkPath string) *TableSplitController {
	output := &TableSplitController{
		logName:         fmt.Sprintf("%s-%s", table.serviceName, table.TableName),
		table:           table,
		stableInfo:      SplitStableInfo{},
		zkPath:          zkPath,
		lastSplitSecond: time.Now().Unix(),
	}
	return output
}

func (ctrl *TableSplitController) InitializeNew(req *pb.SplitTableRequest) {
	ctrl.lastSplitSecond = time.Now().Unix() - int64(req.Options.DelaySeconds)
	data := utils.MarshalJsonOrDie(&SplitStableInfo{
		SplitTableOptions: req.Options,
		NewSplitVersion:   req.NewSplitVersion,
	})

	succ := ctrl.table.zkConn.Create(
		context.Background(),
		ctrl.zkPath,
		data,
	)
	logging.Assert(succ, "")
	succ = ctrl.LoadFromZookeeper()
	logging.Assert(succ, "")
}

func (ctrl *TableSplitController) LoadFromZookeeper() bool {
	data, exists, succ := ctrl.table.zkConn.Get(context.Background(), ctrl.zkPath)
	logging.Assert(succ, "")
	if exists {
		utils.UnmarshalJsonOrDie(data, &(ctrl.stableInfo))
		logging.Info("%s: load split info: %v", ctrl.logName, &(ctrl.stableInfo))
		return true
	} else {
		logging.Info("%s: can't find split ctrl", ctrl.logName)
		return false
	}
}

func (ctrl *TableSplitController) Teardown() {
	succ := ctrl.table.zkConn.Delete(context.Background(), ctrl.zkPath)
	logging.Assert(succ, "")
}

func (ctrl *TableSplitController) UpdateOptions(options *pb.SplitTableOptions) *pb.ErrorStatus {
	if proto.Equal(ctrl.stableInfo.SplitTableOptions, options) {
		return pb.ErrStatusOk()
	}
	ctrl.table.serviceLock.AllowRead()
	newInfo := ctrl.stableInfo
	newInfo.SplitTableOptions = proto.Clone(options).(*pb.SplitTableOptions)
	data := utils.MarshalJsonOrDie(&newInfo)
	logging.Assert(ctrl.table.zkConn.Set(context.Background(), ctrl.zkPath, data), "")
	ctrl.table.serviceLock.DisallowRead()
	ctrl.stableInfo.SplitTableOptions = newInfo.SplitTableOptions
	return pb.ErrStatusOk()
}

func (ctrl *TableSplitController) GeneratePlan() (sched.SchedulePlan, bool) {
	// TODO(huyifan03): schedule split options
	output := sched.SchedulePlan{}
	for i := int32(0); i < ctrl.table.PartsCount/2; i++ {
		parent := &(ctrl.table.currParts[i])
		child := &(ctrl.table.currParts[i+ctrl.table.PartsCount/2])
		logging.Assert(parent.members.SplitVersion == child.members.SplitVersion, "")
		if parent.members.SplitVersion < ctrl.table.SplitVersion {
			if !child.hasZkPath {
				if ctrl.lastSplitSecond+int64(ctrl.stableInfo.DelaySeconds) <= time.Now().Unix() {
					action := actions.CreateMembersAction{}
					parent.members.CloneTo(&action.Members)
					for node := range action.Members.Peers {
						action.Members.Peers[node] = pb.ReplicaRole_kLearner
					}
					action.Members.MembershipVersion = 1
					logging.Info(
						"%s: partition %d is empty, create members: %v",
						ctrl.logName,
						child.partId,
						action.Members,
					)
					output[child.partId] = actions.MakeActions(&action)
					ctrl.lastSplitSecond = time.Now().Unix()
					return output, false
				} else {
					logging.Info("%s: previous split happened at %d, with delay seconds %d",
						ctrl.logName, ctrl.lastSplitSecond,
						ctrl.stableInfo.DelaySeconds,
					)
					return nil, false
				}
			} else {
				logging.Info("%s: partition %d split from %d, split version: %d, wait to catch up",
					ctrl.logName, child.partId, parent.partId,
					parent.members.SplitVersion,
				)
				return output, false
			}
		} else {
			if !parent.doSplitCleanup() {
				return nil, false
			}
			if !child.doSplitCleanup() {
				return nil, false
			}
		}
	}
	return output, true
}
