package sched

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
)

type subSchedulerBase struct {
	logName string
	*ScheduleInput
	rec  *recorder.AllNodesRecorder
	plan SchedulePlan
}

func (s *subSchedulerBase) SetLogName(name string) {
	s.logName = name
}

func (s *subSchedulerBase) Initialize(
	input *ScheduleInput,
	rec *recorder.AllNodesRecorder,
	plan SchedulePlan,
) {
	s.ScheduleInput = input
	s.rec = rec
	s.plan = plan
}

func (s *subSchedulerBase) GetTableName() string {
	return s.Table.GetInfo().TableName
}

func (s *subSchedulerBase) GetTableId() int32 {
	return s.Table.GetInfo().TableId
}

func (s *subSchedulerBase) GetPartsCount() int32 {
	return s.Table.GetInfo().PartsCount
}

func (s *subSchedulerBase) GetMembership(pid int32) *table_model.PartitionMembership {
	return s.Table.GetMembership(pid)
}

func (s *subSchedulerBase) HasPlan(pid int32) bool {
	_, ok := s.plan[pid]
	return ok
}

func (s *subSchedulerBase) removeExtraHubs(pid int32, nodes []string) bool {
	if len(nodes) > 0 {
		s.removeNode(pid, nodes[0])
		return true
	} else {
		return false
	}
}

func (s *subSchedulerBase) addLearner(pid int32, node string) {
	s.plan[pid] = actions.MakeActions(&actions.AddLearnerAction{Node: node})
	s.rec.Add(node, s.GetTableId(), pid, pb.ReplicaRole_kLearner, s.Table.GetPartWeight(pid))
}

func (s *subSchedulerBase) transform(pid int32, node string, from, to pb.ReplicaRole) {
	s.plan[pid] = actions.MakeActions(&actions.TransformAction{Node: node, ToRole: to})
	s.rec.Transform(node, s.GetTableId(), pid, from, to, s.Table.GetPartWeight(pid))
}

func (s *subSchedulerBase) removeNode(pid int32, node string) {
	currentRole := s.GetMembership(pid).GetMember(node)
	if currentRole == pb.ReplicaRole_kLearner {
		s.plan[pid] = actions.RemoveLearner(node)
		s.rec.Remove(
			node,
			s.GetTableId(),
			pid,
			pb.ReplicaRole_kLearner,
			s.Table.GetPartWeight(pid),
		)
	} else {
		s.plan[pid] = actions.FluentRemoveNode(node)
		s.rec.Remove(node, s.GetTableId(), pid, currentRole, s.Table.GetPartWeight(pid))
	}
}

func (s *subSchedulerBase) switchPrimary(pid int32, from, to string) {
	s.plan[pid] = actions.SwitchPrimary(from, to)
	s.rec.Transform(
		from,
		s.GetTableId(),
		pid,
		pb.ReplicaRole_kPrimary,
		pb.ReplicaRole_kSecondary,
		s.Table.GetPartWeight(pid),
	)
	s.rec.Transform(
		to,
		s.GetTableId(),
		pid,
		pb.ReplicaRole_kSecondary,
		pb.ReplicaRole_kPrimary,
		s.Table.GetPartWeight(pid),
	)
}

type subScheduler interface {
	// both rec and plan are inputs & outputs
	//
	// "rec" stands for current replica distribution on nodes
	// when it is passed to a subScheduler,
	// and it'll be modified after the subScheduler runs
	// scheduling, which will represent the new distribution.
	//
	// "plan" is similar, which represents current plan which has generated
	// by other schedulers. Sub-schedulers running later are free to
	// add new partition actions to the plan, but deleting/modifying old ones
	// are not recommended.
	SetLogName(name string)
	Schedule(input *ScheduleInput, rec *recorder.AllNodesRecorder, plan SchedulePlan)
}
