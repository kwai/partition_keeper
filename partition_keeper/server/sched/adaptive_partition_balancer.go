package sched

import (
	"math/rand"
	"strings"

	"github.com/emirpasic/gods/sets/treeset"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type nodeBalanceEntity struct {
	capacity int
	rec      *recorder.NodePartitions
}

func (n *nodeBalanceEntity) extraCapacity() int {
	return n.capacity - n.rec.WeightedCountAll()
}

func (n *nodeBalanceEntity) absExtraCap() int {
	if n.capacity > n.rec.WeightedCountAll() {
		return n.capacity - n.rec.WeightedCountAll()
	} else {
		return n.rec.WeightedCountAll() - n.capacity
	}
}

type balanceTracker struct {
	// nodeBalanceEntity with comparedNodeLoad
	nodes *treeset.Set
}

func cmpNodeLoad(left, right interface{}) int {
	leftNode := left.(*nodeBalanceEntity)
	rightNode := right.(*nodeBalanceEntity)

	gapL, gapR := leftNode.capacity-leftNode.rec.WeightedCountAll(), rightNode.capacity-rightNode.rec.WeightedCountAll()
	if gapL != gapR {
		return gapR - gapL
	}

	primaryL, primaryR := leftNode.rec.WeightedCount(
		pb.ReplicaRole_kPrimary,
	), rightNode.rec.WeightedCount(
		pb.ReplicaRole_kPrimary,
	)
	if primaryL != primaryR {
		return primaryL - primaryR
	}

	learnerL, learnerR := leftNode.rec.WeightedCount(
		pb.ReplicaRole_kLearner,
	), rightNode.rec.WeightedCount(
		pb.ReplicaRole_kLearner,
	)
	if learnerL != learnerR {
		return learnerL - learnerR
	}
	return strings.Compare(leftNode.rec.NodeId, rightNode.rec.NodeId)
}

func newBalanceTracker() *balanceTracker {
	return &balanceTracker{
		nodes: treeset.NewWith(cmpNodeLoad),
	}
}

func (r *balanceTracker) popMin() *nodeBalanceEntity {
	iter := r.nodes.Iterator()
	logging.Assert(iter.First(), "shouldn't get here if items size is less than 1")
	ans := iter.Value().(*nodeBalanceEntity)
	r.nodes.Remove(ans)
	return ans
}

func (r *balanceTracker) popMax() *nodeBalanceEntity {
	iter := r.nodes.Iterator()
	logging.Assert(iter.Last(), "shouldn't get here if items size is less than 1")
	ans := iter.Value().(*nodeBalanceEntity)
	r.nodes.Remove(ans)
	return ans
}

func (r *balanceTracker) insert(t *nodeBalanceEntity) {
	r.nodes.Add(t)
}

type adaptivePartitionBalancer struct {
	subSchedulerBase
}

func (s *adaptivePartitionBalancer) transfer(
	tp utils.TblPartID,
	from, to *nodeBalanceEntity,
) (p *actions.PartitionActions) {
	current := s.Table.GetMembership(tp.Part())

	oldRole, ok := current.Peers[from.rec.NodeId]
	logging.Assert(
		ok,
		"%s: can't find %s in %s: %v",
		s.logName,
		from.rec.NodeId,
		tp.String(),
		current.Peers,
	)
	switch oldRole {
	case pb.ReplicaRole_kPrimary:
		p = actions.TransferPrimary(from.rec.NodeId, to.rec.NodeId)
	case pb.ReplicaRole_kSecondary:
		p = actions.TransferSecondary(from.rec.NodeId, to.rec.NodeId, false)
	case pb.ReplicaRole_kLearner:
		p = actions.TransferLearner(from.rec.NodeId, to.rec.NodeId, false)
	default:
		logging.Fatal("%s: shouldn't reach here", s.logName)
	}

	logging.Info(
		"%s: plan to transfer %s@%s from %s to %s",
		s.logName,
		oldRole.String(),
		tp.String(),
		from.rec.NodeId,
		to.rec.NodeId,
	)
	from.rec.Remove(tp.Table(), tp.Part(), oldRole, s.Table.GetPartWeight(tp.Part()))
	to.rec.Add(tp.Table(), tp.Part(), oldRole, s.Table.GetPartWeight(tp.Part()))
	return
}

func (s *adaptivePartitionBalancer) chooseNodeToTransfer(
	from *nodeBalanceEntity,
	plan SchedulePlan,
) utils.TblPartID {
	// 分片选择逻辑
	//   0. 该partition还未在调度计划中
	//   1. partition的权重，不能超过节点自身的extraCapacity
	//   2. partition必须是movable的，即已经split完了，或者还没开始split (隐含了child是不可迁移的)
	//   3. 优先搬迁learner，因为影响最小
	//   4. primary 和 secondary 依照数量，按概率进行选取
	findPart := func(parts map[utils.TblPartID]int) utils.TblPartID {
		for part, weight := range parts {
			if weight > from.absExtraCap() || weight == 0 {
				continue
			}
			if !s.Table.PartSchedulable(part.Part()) {
				continue
			}
			if _, ok := plan[part.Part()]; ok {
				continue
			}
			return part
		}
		return utils.InvalidTPID
	}
	if p := findPart(from.rec.Learners); p != utils.InvalidTPID {
		return p
	}
	totalWeight := from.rec.WeightedCount(
		pb.ReplicaRole_kPrimary,
	) + from.rec.WeightedCount(
		pb.ReplicaRole_kSecondary,
	)
	if totalWeight <= 0 {
		return utils.InvalidTPID
	}
	nextGroup, thirdGroup := from.rec.Secondaries, from.rec.Primaries
	if n := rand.Intn(totalWeight); n < from.rec.WeightedCount(pb.ReplicaRole_kPrimary) {
		nextGroup, thirdGroup = from.rec.Primaries, from.rec.Secondaries
	}
	if p := findPart(nextGroup); p != utils.InvalidTPID {
		return p
	}
	return findPart(thirdGroup)
}

func (s *adaptivePartitionBalancer) balanceHub(
	hub string,
	t *balanceTracker,
	plan map[int32]*actions.PartitionActions,
) {
	if t.nodes.Size() <= 1 {
		logging.Verbose(1, "%s: only have one node in hub %s", s.logName, hub)
		return
	}

	for {
		minNode, maxNode := t.popMin(), t.popMax()
		logging.Info("%s %s: hub %s min node %s %d/%d, max node %s %d/%d",
			s.logName,
			s.Table.GetInfo().TableName,
			hub,
			minNode.rec.NodeId, minNode.rec.WeightedCountAll(), minNode.extraCapacity(),
			maxNode.rec.NodeId, maxNode.rec.WeightedCountAll(), maxNode.extraCapacity(),
		)
		if minNode.extraCapacity() < 0 {
			logging.Warning("%s %s: even min node has exceed the max capacity. "+
				"must be all nodes marked offline for an active hub, please check",
				s.logName,
				s.GetTableName(),
			)
			return
		}
		if maxNode.extraCapacity() == 0 {
			logging.Assert(minNode.extraCapacity() == 0, "")
			return
		}
		if minNode.extraCapacity() == 0 {
			logging.Assert(maxNode.extraCapacity() == 0, "")
			return
		}

		p := s.chooseNodeToTransfer(maxNode, plan)
		if p != utils.InvalidTPID {
			plan[p.Part()] = s.transfer(p, maxNode, minNode)
		} else {
			return
		}

		t.insert(minNode)
		t.insert(maxNode)
	}
}

func (s *adaptivePartitionBalancer) divideByHubs(
	initial map[string]recorder.NodeRecorder,
) map[string]*balanceTracker {
	result := make(map[string]*balanceTracker)

	for _, hub := range s.Hubs {
		result[hub.Name] = newBalanceTracker()
	}

	for node, info := range s.Nodes.AllNodes() {
		rec, ok1 := initial[node]
		if !ok1 {
			rec = recorder.NewNodePartitions(node)
		}
		if tracker, ok := result[info.Hub]; ok {
			entity := &nodeBalanceEntity{
				capacity: s.Table.GetEstimatedReplicasOnNode(node),
				rec:      rec.(*recorder.NodePartitions),
			}
			tracker.insert(entity)
		} else {
			logging.Verbose(1, "%s: node %s's hub %s don't belong to table, may be removed hub", s.logName, node, info.Hub)
		}
	}

	for _, info := range s.Nodes.AllNodes() {
		if info.Op != pb.AdminNodeOp_kOffline && info.WeightedScore() <= 0 {
			logging.Warning(
				"%s: node %s score not valid, don't scheduler hub %s",
				s.logName,
				info.LogStr(),
				info.Hub,
			)
			delete(result, info.Hub)
		}
	}
	return result
}

func (s *adaptivePartitionBalancer) readyForSchedule() bool {
	if len(s.plan) > 0 {
		logging.Info(
			"%s: %s already has schedule plan, skip to run global partition balancer",
			s.logName,
			s.GetTableName(),
		)
		return false
	}

	for i := int32(0); i < s.GetPartsCount(); i++ {
		members := s.GetMembership(i)
		if s.Table.GetInfo().SplitVersion > members.SplitVersion {
			if !s.Opts.Configs.EnableSplitBalancer {
				logging.Info(
					"%s: %s skip balancer as split detected, t%d.p%d tsv %d vs sv %d",
					s.logName,
					s.GetTableName(),
					s.GetTableId(),
					i,
					s.Table.GetInfo().SplitVersion,
					members.SplitVersion,
				)
				return false
			}
			if !s.Table.PartSchedulable(i) {
				logging.Verbose(
					1,
					"%s: %s t%d.p%d is unschedulable, don't block balancer",
					s.logName,
					s.GetTableName(),
					s.GetTableId(),
					i,
				)
			}
			continue
		}
		inHubs, outHubs := members.DivideByHubs(s.Nodes, s.Hubs)
		if len(outHubs) > 0 {
			logging.Info("%s: %s t%d.p%d has useless hubs, skip to run global partition balancer",
				s.logName, s.GetTableName(), s.GetTableId(), i,
			)
			return false
		}
		for hub, replicas := range inHubs {
			if len(replicas) != 1 {
				logging.Info(
					"%s: %s t%d.p%d %d nodes in hub %s, skip to run global partition balancer",
					s.logName,
					s.GetTableName(),
					s.GetTableId(),
					i,
					len(replicas),
					hub,
				)
				return false
			}
		}
	}

	return true
}

func (s *adaptivePartitionBalancer) Schedule(
	input *ScheduleInput,
	rec *recorder.AllNodesRecorder,
	plan SchedulePlan,
) {
	s.Initialize(input, rec, plan)
	if !s.readyForSchedule() {
		return
	}

	trackers := s.divideByHubs(rec.GetNodes())
	logging.Verbose(
		1,
		"%s: table %s divided to %d hubs",
		s.logName,
		s.Table.GetInfo().TableName,
		len(trackers),
	)
	for hub, tracker := range trackers {
		s.balanceHub(hub, tracker, plan)
	}
}
