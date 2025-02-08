package sched

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type restartOpInspector struct {
	subSchedulerBase
}

func (r *restartOpInspector) Schedule(
	input *ScheduleInput,
	rec *recorder.AllNodesRecorder,
	plan SchedulePlan,
) {
	r.Initialize(input, rec, plan)
	if len(plan) > 0 {
		logging.Info(
			"%s: don't schedule restarting as current schedule plan is not empty",
			r.logName,
		)
		return
	}

	for pid := int32(0); pid < r.GetPartsCount(); pid++ {
		primary, secs, _ := r.Table.GetMembership(pid).DivideRoles()
		logging.Assert(
			len(primary) <= 1,
			"%s t%s.p%d has more than 1 primary",
			r.logName,
			r.GetTableName(),
			pid,
		)

		hasAction := false
		// TODO: control the max ratio of learners caused by restarting
		for _, p := range primary {
			info := r.Nodes.MustGetNodeInfo(p)
			if info.Op == pb.AdminNodeOp_kRestart {
				hasAction = true
				if len(secs) > 0 {
					use := utils.FindMinIndex(len(secs), func(i, j int) bool {
						left, right := secs[i], secs[j]
						infoL := r.Nodes.MustGetNodeInfo(left)
						infoR := r.Nodes.MustGetNodeInfo(right)
						allowL := utils.Bool2Int(
							infoL.AllowRole(r.Hubs, pb.ReplicaRole_kPrimary),
						)
						allowR := utils.Bool2Int(
							infoR.AllowRole(r.Hubs, pb.ReplicaRole_kPrimary),
						)
						if allowL != allowR {
							return allowL > allowR
						}
						cnt1 := rec.Count(left, pb.ReplicaRole_kPrimary)
						cnt2 := rec.Count(right, pb.ReplicaRole_kPrimary)
						return cnt1 < cnt2
					})
					plan[int32(pid)] = actions.MakeActions(
						&actions.AtomicSwitchPrimaryAction{FromNode: p, ToNode: secs[use]},
						&actions.TransformAction{Node: p, ToRole: pb.ReplicaRole_kLearner},
					)
					rec.Transform(
						p,
						r.GetTableId(),
						int32(pid),
						pb.ReplicaRole_kPrimary,
						pb.ReplicaRole_kLearner,
						r.Table.GetPartWeight(pid),
					)
					rec.Transform(
						secs[use],
						r.GetTableId(),
						int32(pid),
						pb.ReplicaRole_kSecondary,
						pb.ReplicaRole_kPrimary,
						r.Table.GetPartWeight(pid),
					)
				} else {
					plan[int32(pid)] = actions.MakeActions(&actions.TransformAction{Node: p, ToRole: pb.ReplicaRole_kLearner})
					rec.Transform(p, r.GetTableId(), int32(pid), pb.ReplicaRole_kPrimary, pb.ReplicaRole_kLearner, r.Table.GetPartWeight(pid))
				}
				break
			}
		}
		if hasAction {
			continue
		}

		for _, s := range secs {
			info := r.Nodes.MustGetNodeInfo(s)
			if info.Op == pb.AdminNodeOp_kRestart {
				plan[int32(pid)] = actions.MakeActions(
					&actions.TransformAction{Node: s, ToRole: pb.ReplicaRole_kLearner},
				)
				rec.Transform(
					s,
					r.GetTableId(),
					int32(pid),
					pb.ReplicaRole_kSecondary,
					pb.ReplicaRole_kLearner,
					r.Table.GetPartWeight(pid),
				)
				break
			}
		}
	}
}
