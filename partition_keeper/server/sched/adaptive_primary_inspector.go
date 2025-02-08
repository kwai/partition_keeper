package sched

import (
	"fmt"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type adaptivePrimaryInspector struct {
	subSchedulerBase
}

func (s *adaptivePrimaryInspector) assignPrimary(partId int32, secs []string) {
	node := utils.FindMinIndex(len(secs), func(i, j int) bool {
		left, right := secs[i], secs[j]
		infoL, infoR := s.Nodes.MustGetNodeInfo(left), s.Nodes.MustGetNodeInfo(right)

		// prefer normal
		normalFlag1, normalFlag2 := utils.Bool2Int(infoL.Normal()), utils.Bool2Int(infoR.Normal())
		if normalFlag1 != normalFlag2 {
			return normalFlag1 > normalFlag2
		}

		// prefer nodes which allow to have primaries.
		// however, we will use a node even the primary is forbidden
		// if we can't select a better one
		allowLeft := utils.Bool2Int(infoL.AllowRole(s.Hubs, pb.ReplicaRole_kPrimary))
		allowRight := utils.Bool2Int(infoR.AllowRole(s.Hubs, pb.ReplicaRole_kPrimary))
		if allowLeft != allowRight {
			return allowLeft > allowRight
		}

		// prefer less primary
		pLeft := s.rec.Count(left, pb.ReplicaRole_kPrimary)
		pRight := s.rec.Count(right, pb.ReplicaRole_kPrimary)
		if pLeft != pRight {
			return pLeft < pRight
		}

		// prefer lighter loads
		gapLeft := s.Table.GetEstimatedReplicasOnNode(left) - s.rec.CountAll(left)
		gapRight := s.Table.GetEstimatedReplicasOnNode(right) - s.rec.CountAll(right)
		return gapLeft > gapRight
	})

	if node == -1 {
		logging.Verbose(
			1,
			"%s t%d.p%d: no secondary to promote to primary",
			s.GetTableName(),
			s.GetTableId(),
			partId,
		)
		return
	}
	if !s.Nodes.MustGetNodeInfo(secs[node]).Normal() {
		logging.Verbose(
			1,
			"%s t%d.p%d: can't find secondary to promote as all of them are not normal",
			s.GetTableName(),
			s.GetTableId(),
			partId,
		)
		third_party.PerfLog2(
			"reco",
			"reco.colossusdb.promote_primary_fail",
			s.logName,
			s.GetTableName(),
			fmt.Sprintf("p%d", partId),
			1,
		)
		return
	}
	s.rec.Transform(
		secs[node],
		s.GetTableId(),
		partId,
		pb.ReplicaRole_kSecondary,
		pb.ReplicaRole_kPrimary,
		s.Table.GetPartWeight(partId),
	)
	s.plan[partId] = actions.MakeActions(
		&actions.TransformAction{Node: secs[node], ToRole: pb.ReplicaRole_kPrimary},
	)
}

func (s *adaptivePrimaryInspector) schedulePrimary(partId int32) {
	if _, ok := s.plan[partId]; ok {
		return
	}

	if len(s.Hubs) == 0 {
		return
	}

	members := s.GetMembership(partId)
	primaries, secs, _ := members.DivideRoles()
	logging.Assert(
		len(primaries) <= 1,
		"t%d.p%d have %d primaries",
		s.GetTableId(),
		partId,
		len(primaries),
	)
	if len(primaries) == 1 {
		if s.Opts.PrepareRestoring {
			s.rec.Transform(
				primaries[0],
				s.GetTableId(),
				partId,
				pb.ReplicaRole_kPrimary,
				pb.ReplicaRole_kSecondary,
				s.Table.GetPartWeight(partId),
			)
			s.plan[partId] = actions.MakeActions(
				&actions.TransformAction{Node: primaries[0], ToRole: pb.ReplicaRole_kSecondary},
			)
		}
	} else {
		if !s.Opts.PrepareRestoring {
			s.assignPrimary(partId, secs)
		}
	}
}

func (s *adaptivePrimaryInspector) Schedule(
	input *ScheduleInput,
	rec *recorder.AllNodesRecorder,
	plan SchedulePlan,
) {
	s.Initialize(input, rec, plan)
	for i := int32(0); i < s.GetPartsCount(); i++ {
		s.schedulePrimary(i)
	}
}
