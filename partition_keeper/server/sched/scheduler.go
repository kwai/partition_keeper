package sched

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
)

const (
	SCHEDULE_RATIO_MAX_VALUE = 1000
)

type ScheduleCtrlOptions struct {
	Configs          *pb.ScheduleOptions
	PrepareRestoring bool
	PartGroupCnt     int
}

type ScheduleInput struct {
	Table table_model.TableModel
	Nodes *node_mgr.NodeStats
	Hubs  []*pb.ReplicaHub
	Opts  ScheduleCtrlOptions
}

type SchedulePlan = map[int32]*actions.PartitionActions

type schedulerBase struct {
	subs []subScheduler
}

func (s *schedulerBase) appendSubScheduler(sub subScheduler) {
	s.subs = append(s.subs, sub)
}

func (s *schedulerBase) installLogName(name string) {
	for _, sub := range s.subs {
		sub.SetLogName(name)
	}
}

func (s *schedulerBase) Schedule(input *ScheduleInput) SchedulePlan {
	rec := recorder.NewAllNodesRecorder(recorder.NewNodePartitions)
	input.Table.Record(rec)
	output := SchedulePlan{}
	for _, subSched := range s.subs {
		subSched.Schedule(input, rec, output)
	}
	return output
}

type Scheduler interface {
	Name() string
	SupportSplit() bool
	Schedule(input *ScheduleInput) SchedulePlan
}
