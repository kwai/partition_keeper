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

const (
	kSourceNode = "_SRC_"
	kSinkNode   = "_SINK_"
)

type primaryBalancer struct {
	subSchedulerBase
}

func (s *primaryBalancer) buildNetwork(rec *recorder.AllNodesRecorder) map[string]map[string]int {
	network := map[string]map[string]int{}
	for _, info := range s.Nodes.AllNodes() {
		logging.Assert(info.Id != kSourceNode, "")
		logging.Assert(info.Id != kSinkNode, "")
		network[info.Id] = map[string]int{}
	}

	network[kSourceNode] = map[string]int{}
	network[kSinkNode] = map[string]int{}

	totalPrimaries := 0
	for i := int32(0); i < s.GetPartsCount(); i++ {
		members := s.GetMembership(i)
		pri, secs, _ := members.DivideRoles()
		logging.Assert(
			len(pri) <= 1,
			"%s: t%d.p%d multiple primaries!!!",
			s.logName,
			s.GetTableId(),
			i,
		)
		if len(pri) == 0 {
			logging.Verbose(1, "%s: t%d.p%d no primary, skip", s.logName, s.GetTableId(), i)
			continue
		}
		totalPrimaries++
		for _, sec := range secs {
			network[pri[0]][sec]++
		}
	}

	if totalPrimaries <= 0 {
		logging.Info(
			"%s: no primary exists in table %s, please check",
			s.logName,
			s.GetTableName(),
		)
		return nil
	}
	var totalScores int64 = 0
	priAllowed := map[string]*node_mgr.NodeInfo{}
	for _, node := range s.Nodes.AllNodes() {
		if node.Normal() &&
			node.AllowRole(s.Hubs, pb.ReplicaRole_kPrimary) {
			if node.WeightedScore() <= 0 {
				logging.Warning(
					"%s-%s: node %s don't have valid score, please check",
					s.logName,
					s.GetTableName(),
					node.LogStr(),
				)
				return nil
			}
			n := int64(node.WeightedScore())
			totalScores += n
			if totalScores < 0 || totalScores < n {
				logging.Assert(
					false,
					"%s: %s total scores overflow, please check",
					s.logName,
					s.GetTableName(),
				)
			}
			priAllowed[node.Id] = node
		}
	}
	if totalScores <= 0 {
		logging.Error(
			"%s: table %s can't find nodes to hold primary, please check",
			s.logName,
			s.GetTableName(),
		)
		return nil
	}

	leastPrimaries := map[string]int{}
	underLeastNodes := 0
	remained := totalPrimaries
	for _, node := range priAllowed {
		atLeastCount := int64(node.WeightedScore()) * int64(totalPrimaries) / totalScores
		logging.Assert(
			atLeastCount >= 0,
			"%s-%s: %d times %d overflow",
			s.logName,
			s.GetTableName(),
			node.WeightedScore(),
			totalPrimaries,
		)
		leastPrimaries[node.Id] = int(atLeastCount)
		remained -= int(atLeastCount)
		if rec.Count(node.Id, pb.ReplicaRole_kPrimary) < int(atLeastCount) {
			underLeastNodes++
		}
	}

	moreThanEst, lessThanEst := 0, 0
	for node, nodeRec := range rec.GetNodes() {
		var estimated int
		if _, ok := priAllowed[node]; !ok {
			estimated = 0
		} else {
			if underLeastNodes == 0 && remained > 0 {
				estimated = leastPrimaries[node] + 1
			} else {
				estimated = leastPrimaries[node]
			}
		}

		priCount := nodeRec.Count(pb.ReplicaRole_kPrimary)
		if priCount != estimated {
			logging.Verbose(
				1, "%s: %s %s pri current(%d)/estimated(%d)",
				s.logName, s.GetTableName(),
				node, priCount, estimated,
			)
		}
		if priCount > estimated {
			network[kSourceNode][node] = priCount - estimated
			moreThanEst++
		} else if priCount < estimated {
			network[node][kSinkNode] = estimated - priCount
			lessThanEst++
		}
	}
	logging.Info("%s: %s primaries %d more than estimated, %d less than estimated",
		s.logName, s.GetTableName(),
		moreThanEst, lessThanEst,
	)
	return network
}

func (s *primaryBalancer) switchPrimariesNaive(network map[string]map[string]int) {
	primarySource := []string{}
	for node := range network[kSourceNode] {
		primarySource = append(primarySource, node)
	}
	sort.Slice(primarySource, func(i, j int) bool {
		left, right := primarySource[i], primarySource[j]
		return network[kSourceNode][left] > network[kSourceNode][right]
	})

	maxActionCount := int(
		s.Table.GetInfo().PartsCount * int32(
			s.Opts.Configs.MaxSchedRatio,
		) / SCHEDULE_RATIO_MAX_VALUE,
	)
	if maxActionCount < 1 {
		maxActionCount = 1
	}

	for _, node := range primarySource {
		maxOutPrimaries := network[kSourceNode][node]
		primaries := s.rec.GetNode(node).(*recorder.NodePartitions).Primaries
		for part := range primaries {
			if len(s.plan) >= maxActionCount {
				return
			}
			if maxOutPrimaries <= 0 {
				break
			}
			if part.Table() != s.Table.GetInfo().TableId {
				continue
			}
			if _, ok := s.plan[part.Part()]; ok {
				continue
			}
			_, secs, _ := s.Table.GetMembership(part.Part()).DivideRoles()
			targetNode := ""
			targetNeed := -1
			for _, sec := range secs {
				nodeRequire := network[sec][kSinkNode]
				if nodeRequire > 0 {
					if targetNeed == -1 || nodeRequire > targetNeed {
						targetNode = sec
						targetNeed = nodeRequire
					}
				}
			}
			if targetNeed != -1 {
				logging.Info(
					"%s: plan to switch %s primary %s -> %s",
					s.logName,
					part.String(),
					node,
					targetNode,
				)
				maxOutPrimaries--
				s.plan[part.Part()] = actions.SwitchPrimary(node, targetNode)
				s.rec.Transform(
					node,
					part.Table(),
					part.Part(),
					pb.ReplicaRole_kPrimary,
					pb.ReplicaRole_kSecondary,
					s.Table.GetPartWeight(part.Part()),
				)
				s.rec.Transform(
					targetNode,
					part.Table(),
					part.Part(),
					pb.ReplicaRole_kSecondary,
					pb.ReplicaRole_kPrimary,
					s.Table.GetPartWeight(part.Part()),
				)
				network[kSourceNode][node]--
				network[targetNode][kSinkNode]--
			}
		}
	}
}

func (s *primaryBalancer) switchPrimariesGlobally(network map[string]map[string]int) {
	flowFromSource := map[string]int{}
	flowFromSource[kSourceNode] = int(s.Table.GetInfo().PartsCount) + 1

	visit := map[string]bool{}
	visit[kSourceNode] = true

	prev := map[string]string{}

	selected := kSourceNode
	for selected != kSinkNode {
		for node, weight := range network[selected] {
			selectedFlow := flowFromSource[selected]
			if current, ok := flowFromSource[node]; !ok {
				flowFromSource[node] = utils.Min(selectedFlow, weight)
				prev[node] = selected
			} else if !visit[node] && current < utils.Min(selectedFlow, weight) {
				flowFromSource[node] = utils.Min(selectedFlow, weight)
				prev[node] = selected
			}
		}

		maxWeight := -1
		for node, weight := range flowFromSource {
			if !visit[node] && weight > maxWeight {
				maxWeight = weight
				selected = node
			}
		}
		if maxWeight == -1 {
			break
		}
		visit[selected] = true
	}

	if !visit[kSinkNode] {
		logging.Info("%s:%s can't find path to do balance", s.logName, s.Table.GetInfo().TableName)
		return
	}

	logging.Assert(flowFromSource[kSinkNode] > 0, "")
	to := prev[kSinkNode]
	from := prev[to]
	for from != kSourceNode {
		fromPrimaries := s.rec.GetNode(from).(*recorder.NodePartitions).Primaries
		transferCount := flowFromSource[kSinkNode]
		for pid := range fromPrimaries {
			if transferCount == 0 {
				break
			}
			if pid.Table() != s.Table.GetInfo().TableId {
				continue
			}
			part := s.Table.GetMembership(pid.Part())
			if part.HasMember(to) {
				logging.Info(
					"%s: plan to switch %s primary %s -> %s",
					s.logName,
					pid.String(),
					from,
					to,
				)
				s.rec.Transform(
					from,
					pid.Table(),
					pid.Part(),
					pb.ReplicaRole_kPrimary,
					pb.ReplicaRole_kSecondary,
					s.Table.GetPartWeight(pid.Part()),
				)
				s.rec.Transform(
					to,
					pid.Table(),
					pid.Part(),
					pb.ReplicaRole_kSecondary,
					pb.ReplicaRole_kPrimary,
					s.Table.GetPartWeight(pid.Part()),
				)
				s.plan[pid.Part()] = actions.SwitchPrimary(from, to)
				transferCount--
			}
		}
		if transferCount > 0 {
			logging.Info("%s: from %s to %s", s.logName, from, to)
			for end := kSinkNode; end != kSourceNode; {
				p := prev[end]
				logging.Info(
					"%s: path %s -> %s, weight: %d, flow: %d",
					s.logName,
					p,
					end,
					network[p][end],
					flowFromSource[end],
				)
				end = p
			}
			for pid := range fromPrimaries {
				if pid.Table() == s.Table.GetInfo().TableId {
					part := s.Table.GetMembership(pid.Part())
					logging.Info("%s: members of %s: %s", s.logName, pid.String(), part.Peers)
				}
			}
			logging.Fatal(
				"%s: remain %d replicas can't transfer primary, expect: %d, max_flow: %d, weight: %d",
				s.logName,
				transferCount,
				flowFromSource[kSinkNode],
				flowFromSource[from],
				network[from][to],
			)
		}
		to = from
		from = prev[to]
	}
}

func (s *primaryBalancer) Schedule(
	input *ScheduleInput,
	rec *recorder.AllNodesRecorder,
	plan SchedulePlan,
) {
	s.Initialize(input, rec, plan)

	if len(plan) > 0 {
		logging.Info(
			"%s: %s skip to run primary balancer has already has plan",
			s.logName,
			s.GetTableName(),
		)
		return
	}
	if !s.Opts.Configs.EnablePrimaryScheduler || s.Opts.PrepareRestoring {
		logging.Info(
			"%s: %s skip to run primary balancer, enable: %v, restore: %v",
			s.logName,
			s.GetTableName(),
			s.Opts.Configs.EnablePrimaryScheduler,
			s.Opts.PrepareRestoring,
		)
		return
	}

	// TODO: give each node an int id, which should be faster
	network := s.buildNetwork(rec)
	if network == nil {
		return
	}
	s.switchPrimariesNaive(network)
	if len(plan) > 0 {
		return
	}
	s.switchPrimariesGlobally(network)
}
