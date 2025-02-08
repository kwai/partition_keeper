package sched

import (
	"sort"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type adaptivePartitionInspector struct {
	subSchedulerBase
}

func (s *adaptivePartitionInspector) sortHubs(inHubs map[string][]string) (hubsPriority []struct {
	name  string
	nodes []string
}) {
	for n, nd := range inHubs {
		hubsPriority = append(hubsPriority, struct {
			name  string
			nodes []string
		}{name: n, nodes: nd})
	}
	sort.Slice(hubsPriority, func(i, j int) bool {
		// give a hub with higher priority if it has less nodes,
		// which implies adding nodes (mostly) happens earlier than removing nodes
		l1, l2 := len(hubsPriority[i].nodes), len(hubsPriority[j].nodes)
		switch {
		case l1 < l2:
			return true
		case l1 > l2:
			return false
		}
		// if two hubs have same nodes, then we order it with hubname.
		// which means that hubs will be handled one by one
		return hubsPriority[i].name < hubsPriority[j].name
	})
	return
}

func (s *adaptivePartitionInspector) fillActiveHubs(partId int32, inHubs map[string][]string) bool {
	members := s.Table.GetMembership(partId)
	hubsPriority := s.sortHubs(inHubs)
	for _, hubPri := range hubsPriority {
		hub := hubPri.name
		nodes := hubPri.nodes

		// case 1: no nodes exist in this hub
		if len(nodes) == 0 {
			candidates := s.Nodes.FilterNodes(
				node_mgr.ExcludeNodeAdminByOp(pb.AdminNodeOp_kOffline),
				node_mgr.GetNodeForHub(hub),
				node_mgr.ExcludeFrom(members.Peers))

			useNode := utils.FindMinIndex(len(candidates), func(i, j int) bool {
				left, right := candidates[i], candidates[j]
				gapLeft := s.Table.GetEstimatedReplicasOnNode(left) - s.rec.CountAll(left)
				gapRight := s.Table.GetEstimatedReplicasOnNode(right) - s.rec.CountAll(right)
				// prefer node with lighter loads
				if gapLeft != gapRight {
					return gapLeft > gapRight
				}
				// prefer node with less learner
				learnL := s.rec.Count(left, pb.ReplicaRole_kLearner)
				learnR := s.rec.Count(right, pb.ReplicaRole_kLearner)
				return learnL < learnR
			})

			if useNode == -1 {
				logging.Verbose(1, "%s t%d.p%d: can't find node in hub %s to add replica",
					s.GetTableName(), s.GetTableId(), partId, hub,
				)
				continue
			} else {
				s.rec.Add(candidates[useNode], s.GetTableId(), partId, pb.ReplicaRole_kLearner, s.Table.GetPartWeight(partId))
				s.plan[partId] = actions.MakeActions(&actions.AddLearnerAction{Node: candidates[useNode]})
				return true
			}
		}

		// case 2: 1 node exist in this hub, which is the perfect state we expect
		if len(nodes) == 1 {
			logging.Verbose(
				1, "%s t%d.p%d: one node exist in hub %s, continue",
				s.GetTableName(), s.GetTableId(), partId, hub)
			continue
		}

		// case 3: more than 1 nodes exists, remove the redundant ones
		servingNodes := map[string]bool{}
		for _, node := range nodes {
			info := s.Nodes.MustGetNodeInfo(node)
			if info.IsAlive && pb.RoleInServing(members.Peers[node]) {
				servingNodes[node] = true
			}
		}

		node := utils.FindMinIndex(len(nodes), func(i, j int) bool {
			left, right := nodes[i], nodes[j]
			infoL, infoR := s.Nodes.MustGetNodeInfo(left), s.Nodes.MustGetNodeInfo(right)

			// prefer learner
			leftLearner := utils.Bool2Int(members.Peers[left] == pb.ReplicaRole_kLearner)
			rightLearner := utils.Bool2Int(members.Peers[right] == pb.ReplicaRole_kLearner)
			if leftLearner != rightLearner {
				return leftLearner > rightLearner
			}

			// prefer heavier loads
			gapLeft := s.Table.GetEstimatedReplicasOnNode(left) - s.rec.CountAll(left)
			gapRight := s.Table.GetEstimatedReplicasOnNode(right) - s.rec.CountAll(right)
			if gapLeft != gapRight {
				return gapLeft < gapRight
			}

			// prefer offline
			leftOffline := utils.Bool2Int(infoL.Op != pb.AdminNodeOp_kOffline)
			rightOffline := utils.Bool2Int(infoR.Op != pb.AdminNodeOp_kOffline)
			if leftOffline != rightOffline {
				return leftOffline < rightOffline
			}

			// prefer dead
			leftAlive, rightAlive := utils.Bool2Int(infoL.IsAlive), utils.Bool2Int(infoR.IsAlive)
			return leftAlive < rightAlive
		})

		if len(servingNodes) == 1 && servingNodes[nodes[node]] {
			logging.Info(
				"%s t%d.p%d: hub %s prefer to move %s as planned, but it's the only serving one.",
				s.GetTableName(),
				s.GetTableId(),
				partId,
				hub,
				nodes[node],
			)
			continue
		}
		if members.Peers[nodes[node]] == pb.ReplicaRole_kLearner {
			s.rec.Remove(
				nodes[node],
				s.GetTableId(),
				partId,
				pb.ReplicaRole_kLearner,
				s.Table.GetPartWeight(partId),
			)
			s.plan[partId] = actions.RemoveLearner(nodes[node])
			return true
		} else {
			s.rec.Remove(nodes[node], s.GetTableId(), partId, members.Peers[nodes[node]], s.Table.GetPartWeight(partId))
			s.plan[partId] = actions.FluentRemoveNode(nodes[node])
			return true
		}
	}
	return false
}

func (s *adaptivePartitionInspector) meetHubConstraint(partId int32) {
	if s.HasPlan(partId) {
		return
	}
	// splitting partition is not allowed to create empty replicas
	tableSplitVersion := s.Table.GetInfo().SplitVersion
	if s.GetMembership(partId).SplitVersion < tableSplitVersion &&
		partId >= (s.GetPartsCount()/2) {
		return
	}
	inHubs, outHubs := s.Table.GetMembership(partId).DivideByHubs(s.Nodes, s.Hubs)
	// we must try to add before try to remove for safety
	if s.fillActiveHubs(partId, inHubs) {
		return
	}
	if len(outHubs) > 0 {
		s.removeExtraHubs(partId, outHubs)
	}
}

func (s *adaptivePartitionInspector) Schedule(
	input *ScheduleInput,
	nr *recorder.AllNodesRecorder,
	plan SchedulePlan,
) {
	s.Initialize(input, nr, plan)
	for i := int32(0); i < s.GetPartsCount(); i++ {
		s.meetHubConstraint(i)
	}
}
