package sched

import (
	"fmt"
	"sort"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

const (
	TRANSER_DEP_FAKE_SOURCE = "__FAKE_SOURCE__"
)

// hub -> node
type briefMembership = map[string]string

type groupedPartsIter struct {
	curPartId   int
	curGroup    int
	totalParts  int
	totalGroups int
}

func newGroupedPartsIter(totalParts, totalGroups int) *groupedPartsIter {
	return &groupedPartsIter{
		curPartId:   0,
		curGroup:    0,
		totalParts:  totalParts,
		totalGroups: totalGroups,
	}
}

func (iter *groupedPartsIter) next() int {
	ans := iter.curPartId
	iter.curPartId += iter.totalGroups
	if iter.curPartId >= iter.totalParts {
		iter.curGroup++
		iter.curPartId = iter.curGroup
	}
	return ans
}

func (iter *groupedPartsIter) hasNext() bool {
	return iter.curGroup < iter.totalGroups
}

type nodeGroup struct {
	lowerCapNodes int
	upperCapNodes int
}

type groupPartsDecider struct {
	logName         string
	partsCount      int
	partGroupSize   int
	partGroupCnt    int
	lowerNodesCnt   int
	nodeCapLower    int
	upperNodesCnt   int
	nodeCapUpper    int
	validNodeGroups []nodeGroup
	maxGroups       [][]int
	groupPrev       [][]int
}

func newGroupPartsDecider(
	logName string,
	partsCount, partGrpCnt, totalNodes int,
) *groupPartsDecider {
	logging.Assert(
		partsCount%partGrpCnt == 0,
		"%s: parts(%d) vs groups(%d)",
		logName,
		partsCount,
		partGrpCnt,
	)
	output := &groupPartsDecider{
		logName:       logName,
		partsCount:    partsCount,
		partGroupSize: partsCount / partGrpCnt,
		partGroupCnt:  partGrpCnt,
		lowerNodesCnt: totalNodes - partsCount%totalNodes,
		nodeCapLower:  partsCount / totalNodes,
		upperNodesCnt: partsCount % totalNodes,
		nodeCapUpper:  (partsCount + totalNodes - 1) / totalNodes,
	}
	return output
}

func (c *groupPartsDecider) addNodeGroup(ng *nodeGroup) {
	for i := range c.validNodeGroups {
		vng := &(c.validNodeGroups[i])
		if ng.lowerCapNodes >= vng.lowerCapNodes &&
			ng.upperCapNodes >= vng.upperCapNodes {
			return
		}
	}
	c.validNodeGroups = append(c.validNodeGroups, *ng)
}

func (c *groupPartsDecider) dpInitialize() {
	maxNodesForLower := c.partGroupSize / utils.Gcd(c.nodeCapLower, c.partGroupSize)
	maxNodesForUpper := c.partGroupSize / utils.Gcd(c.nodeCapUpper, c.partGroupSize)

	for i := 0; i <= maxNodesForLower; i++ {
		if i > c.lowerNodesCnt {
			break
		}
		for j := 0; j <= maxNodesForUpper; j++ {
			if i == 0 && j == 0 {
				continue
			}
			if i == maxNodesForLower && j == maxNodesForUpper {
				continue
			}
			if j > c.upperNodesCnt {
				break
			}
			cap := i*c.nodeCapLower + j*c.nodeCapUpper
			if cap > c.partsCount {
				break
			}
			if cap%c.partGroupSize == 0 {
				c.addNodeGroup(&nodeGroup{i, j})
			}
		}
	}

	logging.Info("%s: valid groups: %v", c.logName, c.validNodeGroups)
	logging.Assert(len(c.validNodeGroups) >= 1, "%s: can't find valid groups", c.logName)
	c.maxGroups = make([][]int, c.lowerNodesCnt+1)
	c.groupPrev = make([][]int, c.lowerNodesCnt+1)
	for i := 0; i <= c.lowerNodesCnt; i++ {
		c.maxGroups[i] = make([]int, c.upperNodesCnt+1)
		c.groupPrev[i] = make([]int, c.upperNodesCnt+1)
		for j := 0; j <= c.upperNodesCnt; j++ {
			c.maxGroups[i][j] = -1
			c.groupPrev[i][j] = -1
		}
	}
}

func (c *groupPartsDecider) dpFindMaxGroups() {
	c.maxGroups[0][0] = 0
	for i := 0; i <= c.lowerNodesCnt; i++ {
		for j := 0; j <= c.upperNodesCnt; j++ {
			if c.maxGroups[i][j] == -1 {
				continue
			}
			for k := range c.validNodeGroups {
				grp := &(c.validNodeGroups[k])
				nextLower, nextUpper := i+grp.lowerCapNodes, j+grp.upperCapNodes
				if nextLower <= c.lowerNodesCnt && nextUpper <= c.upperNodesCnt {
					if c.maxGroups[nextLower][nextUpper] < c.maxGroups[i][j]+1 {
						c.maxGroups[nextLower][nextUpper] = c.maxGroups[i][j] + 1
						c.groupPrev[nextLower][nextUpper] = i*(c.upperNodesCnt+1) + j
					}
				}
			}
		}
	}
	logging.Assert(
		c.maxGroups[c.lowerNodesCnt][c.upperNodesCnt] > 0,
		"%s: can't find way to make group",
		c.logName,
	)
	logging.Info(
		"%s: separate low:%d up:%d to %d groups",
		c.logName,
		c.lowerNodesCnt,
		c.upperNodesCnt,
		c.maxGroups[c.lowerNodesCnt][c.upperNodesCnt],
	)
}

func (c *groupPartsDecider) buildGreedyAssignStrategy() {
	c.groupPrev = make([][]int, c.lowerNodesCnt+1)
	for i := 0; i <= c.lowerNodesCnt; i++ {
		c.groupPrev[i] = make([]int, c.upperNodesCnt+1)
	}
	c.groupPrev[c.lowerNodesCnt][c.upperNodesCnt] = 0
}

func (c *groupPartsDecider) buildDpAssignStrategy() {
	c.dpInitialize()
	c.dpFindMaxGroups()
}

func (c *groupPartsDecider) prepare() {
	if c.upperNodesCnt == 0 || c.nodeCapLower == 0 {
		c.buildGreedyAssignStrategy()
	} else {
		c.buildDpAssignStrategy()
	}
}

func (c *groupPartsDecider) assignPartitions(
	lowerNodes, upperNodes []string,
	hub string,
	output []briefMembership,
) {
	logging.Assert(
		len(lowerNodes) == c.lowerNodesCnt,
		"%s: %d vs %d",
		c.logName,
		len(lowerNodes),
		c.lowerNodesCnt,
	)
	logging.Assert(
		len(upperNodes) == c.upperNodesCnt,
		"%s: %d vs %d",
		c.logName,
		len(upperNodes),
		c.upperNodesCnt,
	)

	curLower := c.lowerNodesCnt
	curUpper := c.upperNodesCnt

	partIter := newGroupedPartsIter(c.partsCount, c.partGroupCnt)

	groupId := 0
	for curLower > 0 || curUpper > 0 {
		prevState := c.groupPrev[curLower][curUpper]
		lowCount := curLower - prevState/(c.upperNodesCnt+1)
		upCount := curUpper - prevState%(c.upperNodesCnt+1)
		logging.Assert(
			lowCount >= 0 && upCount >= 0,
			"%s: lower: %d, upper: %d",
			c.logName,
			lowCount,
			upCount,
		)

		for _, node := range lowerNodes[0:lowCount] {
			nodeServed := []int{}
			for i := 0; i < c.nodeCapLower; i++ {
				curPartition := partIter.next()
				output[curPartition][hub] = node
				nodeServed = append(nodeServed, curPartition)
			}
			logging.Verbose(
				1,
				"%s: group: %d, %s@%s serve: %v",
				c.logName,
				groupId,
				node,
				hub,
				nodeServed,
			)
		}

		for _, node := range upperNodes[0:upCount] {
			nodeServed := []int{}
			for i := 0; i < c.nodeCapUpper; i++ {
				curPartition := partIter.next()
				output[curPartition][hub] = node
				nodeServed = append(nodeServed, curPartition)
			}
			logging.Verbose(
				1,
				"%s: group: %d, %s@%s serve: %v",
				c.logName,
				groupId,
				node,
				hub,
				nodeServed,
			)
		}

		lowerNodes = lowerNodes[lowCount:]
		upperNodes = upperNodes[upCount:]
		curLower -= lowCount
		curUpper -= upCount
		groupId++
	}

	logging.Assert(
		len(lowerNodes) == 0 && len(upperNodes) == 0,
		"%s: low remain: %d, up remain: %d",
		c.logName,
		len(lowerNodes),
		len(upperNodes),
	)
	logging.Info("%s: hub %s, lower: %d*%d, upper: %d*%d, separated to %d groups",
		c.logName, hub, c.lowerNodesCnt, c.nodeCapLower,
		c.upperNodesCnt, c.nodeCapUpper, groupId,
	)
}

type cachedDeciderItem struct {
	refCount int
	decider  *groupPartsDecider
}

type deciderCache struct {
	logName  string
	deciders map[string]*cachedDeciderItem
}

func (d *deciderCache) resetRefCount() {
	for _, item := range d.deciders {
		item.refCount = 0
	}
}

func (d *deciderCache) getDecider(
	tableName string,
	parts, groups, nodes int,
) *groupPartsDecider {
	key := fmt.Sprintf("%s-%d-%d-%d", tableName, parts, groups, nodes)
	if item, ok := d.deciders[key]; ok {
		item.refCount++
		return item.decider
	} else {
		item := &cachedDeciderItem{
			refCount: 1,
			decider: newGroupPartsDecider(
				fmt.Sprintf("%s-%s", d.logName, tableName),
				parts,
				groups,
				nodes,
			),
		}
		item.decider.prepare()
		d.deciders[key] = item
		return item.decider
	}
}

func (d *deciderCache) reclaim() {
	for key, item := range d.deciders {
		if item.refCount == 0 {
			logging.Info("%s: reclaim unused %s", d.logName, key)
			delete(d.deciders, key)
		}
	}
}

type scheduleSummary struct {
	redudantReplicaCount         int
	actionCount                  int
	actionLimitedByCapacityCount int
	limitedTransferNext          map[string]map[string]int
	limitedTransferPrev          map[string]map[string]int
	grossIncreased               recorder.NodesRecorder
}

func newScheduleSummary(intialRec recorder.NodesRecorder) *scheduleSummary {
	return &scheduleSummary{
		redudantReplicaCount:         0,
		actionCount:                  0,
		actionLimitedByCapacityCount: 0,
		limitedTransferNext:          map[string]map[string]int{},
		limitedTransferPrev:          map[string]map[string]int{},
		grossIncreased:               intialRec,
	}
}

func (s *scheduleSummary) addTransferDependency(from, to string) {
	addTo := func(m map[string]map[string]int, s1, s2 string) {
		if submap, ok := m[s1]; !ok {
			submap = map[string]int{s2: 1}
			m[s1] = submap
		} else {
			submap[s2]++
		}
	}
	addTo(s.limitedTransferNext, from, to)
	addTo(s.limitedTransferPrev, to, from)
}

func (s *scheduleSummary) scheduleStuck() bool {
	return s.redudantReplicaCount == 0 && s.actionCount == 0 && s.actionLimitedByCapacityCount > 0
}

func (s *scheduleSummary) topoSortFindTailNode() string {
	depedencyOrder := []string{}
	for node := range s.limitedTransferNext {
		fromNodes := s.limitedTransferPrev[node]
		if len(fromNodes) == 0 {
			depedencyOrder = append(depedencyOrder, node)
		}
	}
	head := 0
	for head < len(depedencyOrder) {
		fromNode := depedencyOrder[head]
		toNodes := s.limitedTransferNext[fromNode]
		if len(toNodes) > 0 {
			for node := range toNodes {
				n := s.limitedTransferPrev[node]
				delete(n, fromNode)
				if len(n) == 0 {
					delete(s.limitedTransferPrev, node)
					depedencyOrder = append(depedencyOrder, node)
				}
			}
		}
		head++
	}
	if len(s.limitedTransferPrev) == 0 {
		for i := head; i > 0; i-- {
			if depedencyOrder[head-1] != TRANSER_DEP_FAKE_SOURCE {
				return depedencyOrder[head-1]
			}
		}
	} else {
		for node := range s.limitedTransferPrev {
			logging.Assert(node != TRANSER_DEP_FAKE_SOURCE, "")
			return node
		}
	}
	return ""
}

type hashGroupArranger struct {
	subSchedulerBase
	cache           *deciderCache
	nextDisorderHub string
}

func (s *hashGroupArranger) simpleAssignPartitions(
	hub string,
	nodes []*node_mgr.NodeInfo,
	output []briefMembership,
) {
	iter := newGroupedPartsIter(int(s.GetPartsCount()), s.Opts.PartGroupCnt)
	for _, node := range nodes {
		nodeCap := s.Table.GetEstimatedReplicasOnNode(node.Id)
		nodeServed := []int{}
		for i := 0; i < nodeCap; i++ {
			logging.Assert(
				iter.hasNext(),
				"curPart: %d, curGroup: %d",
				iter.curPartId,
				iter.curGroup,
			)
			curPartition := iter.next()
			output[curPartition][hub] = node.Id
			nodeServed = append(nodeServed, curPartition)
		}
		logging.Verbose(
			1,
			"%s %s: %s@%s server: %v",
			s.logName,
			s.GetTableName(),
			node.Id,
			hub,
			nodeServed,
		)
	}
	logging.Assert(!iter.hasNext(), "")
}

func (s *hashGroupArranger) generateNewPlacement() []briefMembership {
	output := make([]briefMembership, s.GetPartsCount())
	for i := range output {
		output[i] = make(map[string]string)
	}

	s.cache.resetRefCount()
	defer s.cache.reclaim()

	hubNodes := s.Nodes.DivideByHubs(true)
	for hub, nodes := range hubNodes {
		if !HubAllNodesReady(s.logName, hub, nodes) {
			continue
		}
		sort.Slice(nodes, func(i, j int) bool {
			ln, rn := nodes[i], nodes[j]
			if ln.NodeIndex != rn.NodeIndex {
				return ln.NodeIndex < rn.NodeIndex
			}
			// TODO(huyifan03): generate NodeIndex for non-cloud-hosts
			// so that we can make the partition assignment stable
			return ln.Id < rn.Id
		})

		lowerBound := int(s.GetPartsCount()) / len(nodes)
		upperBound := (int(s.GetPartsCount()) + len(nodes) - 1) / len(nodes)

		lowerNodes := []string{}
		upperNodes := []string{}
		for _, info := range nodes {
			cap := s.Table.GetEstimatedReplicasOnNode(info.Id)
			if cap == lowerBound {
				lowerNodes = append(lowerNodes, info.Id)
			} else if cap == upperBound {
				upperNodes = append(upperNodes, info.Id)
			}
		}
		if len(lowerNodes)+len(upperNodes) == len(nodes) {
			decider := s.cache.getDecider(
				s.GetTableName(),
				int(s.GetPartsCount()),
				s.Opts.PartGroupCnt,
				len(nodes),
			)
			decider.assignPartitions(lowerNodes, upperNodes, hub, output)
		} else {
			logging.Info("%s: replicas of table %s can't evenly placed on hub %s", s.logName, s.GetTableName(), hub)
			s.simpleAssignPartitions(hub, nodes, output)
		}
	}

	return output
}

func (s *hashGroupArranger) tryRemoveRedundant(
	pid int32,
	curNodes []string,
	expectNode string,
	res *scheduleSummary,
) bool {
	_, count := s.GetMembership(pid).CountServing(s.Nodes, s.Hubs)
	for _, node := range curNodes {
		if node == expectNode {
			continue
		}
		if s.GetMembership(pid).GetMember(node) == pb.ReplicaRole_kLearner {
			s.plan[pid] = actions.RemoveLearner(node)
			s.rec.Remove(
				node,
				s.GetTableId(),
				pid,
				pb.ReplicaRole_kLearner,
				s.Table.GetPartWeight(pid),
			)
			res.actionCount++
			return true
		} else if count > 1 {
			s.plan[pid] = actions.FluentRemoveNode(node)
			s.rec.Remove(node, s.GetTableId(), pid, pb.ReplicaRole_kLearner, s.Table.GetPartWeight(pid))
			res.actionCount++
			return true
		}
	}
	return false
}

func (s *hashGroupArranger) transferIfCapacityAllowed(
	pid int32,
	fromNode, toNode string,
	curRole pb.ReplicaRole,
	res *scheduleSummary,
) bool {
	logPrefix := fmt.Sprintf(
		"%s %s: t%d.p%d current %s, expect %s, role: %s, hub: %s",
		s.logName,
		s.GetTableName(),
		s.GetTableId(),
		pid,
		fromNode,
		toNode,
		curRole.String(),
		s.nextDisorderHub,
	)
	if !s.Opts.Configs.HashArrangerAddReplicaFirst {
		logging.Info("%s force transfer", logPrefix)
		if curRole == pb.ReplicaRole_kLearner {
			s.plan[pid] = actions.TransferLearner(fromNode, toNode, true)
		} else {
			s.plan[pid] = actions.MakeActions(
				&actions.TransformAction{Node: fromNode, ToRole: pb.ReplicaRole_kLearner},
				&actions.RemoveNodeAction{Node: fromNode},
				&actions.AddLearnerAction{Node: toNode},
			)
		}
		s.rec.Remove(fromNode, s.GetTableId(), pid, curRole, s.Table.GetPartWeight(pid))
		s.rec.Add(toNode, s.GetTableId(), pid, pb.ReplicaRole_kLearner, s.Table.GetPartWeight(pid))
		res.actionCount++
		return true
	}

	expectedCount := res.grossIncreased.GetNode(toNode).WeightedCountAll() + 1
	maxCount := s.Table.GetEstimatedReplicasOnNode(
		toNode,
	) + int(
		s.Opts.Configs.HashArrangerMaxOverflowReplicas,
	)
	if expectedCount <= maxCount {
		logging.Info(
			"%s fluent transfer, expCount(%d), maxCount(%d)",
			logPrefix,
			expectedCount,
			maxCount,
		)
		switch curRole {
		case pb.ReplicaRole_kLearner:
			s.plan[pid] = actions.TransferLearner(fromNode, toNode, false)
		case pb.ReplicaRole_kSecondary:
			s.plan[pid] = actions.TransferSecondary(fromNode, toNode, false)
		case pb.ReplicaRole_kPrimary:
			s.plan[pid] = actions.TransferPrimary(fromNode, toNode)
		default:
			logging.Assert(false, "")
		}
		s.rec.Remove(fromNode, s.GetTableId(), pid, curRole, s.Table.GetPartWeight(pid))
		s.rec.Add(toNode, s.GetTableId(), pid, curRole, s.Table.GetPartWeight(pid))
		res.grossIncreased.Add(toNode, s.GetTableId(), pid, curRole, s.Table.GetPartWeight(pid))
		res.actionCount++
		return true
	}
	logging.Verbose(1,
		"%s remove reject, expCount(%d), maxCount(%d)",
		logPrefix,
		expectedCount,
		maxCount,
	)
	res.addTransferDependency(fromNode, toNode)
	res.actionLimitedByCapacityCount++
	return false
}

func (s *hashGroupArranger) addLearnerIfCapacityAllowed(
	pid int32,
	target string,
	res *scheduleSummary,
) bool {
	expectedCount := res.grossIncreased.GetNode(target).WeightedCountAll() + 1
	maxCount := s.Table.GetEstimatedReplicasOnNode(
		target,
	) + int(
		s.Opts.Configs.HashArrangerMaxOverflowReplicas,
	)

	logPrefix := fmt.Sprintf("%s %s: t%d.p%d assign to %s, expCount(%d), maxCount(%d)", s.logName,
		s.GetTableName(),
		s.GetTableId(),
		pid,
		target,
		expectedCount,
		maxCount,
	)
	if !s.Opts.Configs.HashArrangerAddReplicaFirst || expectedCount <= maxCount {
		logging.Info(logPrefix)
		s.plan[pid] = actions.MakeActions(&actions.AddLearnerAction{Node: target})
		s.rec.Add(target, s.GetTableId(), pid, pb.ReplicaRole_kLearner, s.Table.GetPartWeight(pid))
		res.grossIncreased.Add(
			target,
			s.GetTableId(),
			pid,
			pb.ReplicaRole_kLearner,
			s.Table.GetPartWeight(pid),
		)
		res.actionCount++
		return true
	}
	logging.Verbose(1, "%s rejected by capacity", logPrefix)
	res.addTransferDependency(TRANSER_DEP_FAKE_SOURCE, target)
	res.actionLimitedByCapacityCount++
	return false
}

func (s *hashGroupArranger) adjustActiveHub(
	pid int32,
	curNodes []string,
	expectNode string,
	hubName string,
	res *scheduleSummary,
) bool {
	if len(curNodes) > 1 {
		res.redudantReplicaCount++
		return s.tryRemoveRedundant(pid, curNodes, expectNode, res)
	} else if len(curNodes) == 1 {
		if curNodes[0] != expectNode {
			curRole := s.GetMembership(pid).GetMember(curNodes[0])
			if curRole == pb.ReplicaRole_kLearner {
				return s.transferIfCapacityAllowed(pid, curNodes[0], expectNode, curRole, res)
			} else {
				serving, count := s.GetMembership(pid).CountServing(s.Nodes, s.Hubs)
				if len(serving) == len(s.Hubs) && count == len(s.Hubs) {
					if hubName == s.nextDisorderHub {
						return s.transferIfCapacityAllowed(pid, curNodes[0], expectNode, curRole, res)
					} else {
						return false
					}
				} else {
					return false
				}
			}
		} else {
			return false
		}
	} else {
		return s.addLearnerIfCapacityAllowed(pid, expectNode, res)
	}
}

func (s *hashGroupArranger) adjustActiveHubs(
	pid int32,
	inHubs map[string][]string,
	newMembers briefMembership,
	res *scheduleSummary,
) bool {
	for _, hub := range s.Hubs {
		curNodes := inHubs[hub.Name]
		expectNode, ok := newMembers[hub.Name]
		if !ok {
			logging.Verbose(
				1,
				"%s-%s: haven't assign node for part %d of hub %s",
				s.logName,
				s.GetTableName(),
				pid,
				hub,
			)
			continue
		}
		if s.adjustActiveHub(pid, curNodes, expectNode, hub.Name, res) {
			return true
		}
	}
	return false
}

func (s *hashGroupArranger) adjustPartition(
	pid int32,
	newMembers briefMembership,
	res *scheduleSummary,
) {
	inHubs, outHubs := s.GetMembership(pid).DivideByHubs(s.Nodes, s.Hubs)
	if s.adjustActiveHubs(pid, inHubs, newMembers, res) {
		return
	}
	if len(outHubs) > 0 {
		s.removeExtraHubs(pid, outHubs)
	}
}

func (s *hashGroupArranger) fixStuckSchedule(node string, newMembers []briefMembership) {
	if node == "" {
		return
	}
	np := s.rec.GetNode(node).(*recorder.NodePartitions)
	currentHub := s.Nodes.MustGetNodeInfo(node).Hub

	selectPartition := int32(-1)
	selectRole := pb.ReplicaRole_kInvalid
	findReplicaToMoveOut := func(parts map[utils.TblPartID]int, role pb.ReplicaRole) {
		for pid := range parts {
			if !s.HasPlan(pid.Part()) {
				newLocation := newMembers[pid.Part()][currentHub]
				if newLocation != node {
					selectPartition = pid.Part()
					selectRole = role
				}
			}
		}
	}

	findReplicaToMoveOut(np.Primaries, pb.ReplicaRole_kPrimary)
	findReplicaToMoveOut(np.Secondaries, pb.ReplicaRole_kSecondary)
	findReplicaToMoveOut(np.Learners, pb.ReplicaRole_kLearner)

	if selectPartition != -1 {
		logging.Info(
			"%s-%s: t%d.p%d force remove %s/%s to fix scheduling stuck",
			s.logName,
			s.GetTableName(),
			s.GetTableId(),
			selectPartition,
			node,
			selectRole.String(),
		)
		if selectRole == pb.ReplicaRole_kLearner {
			s.plan[selectPartition] = actions.RemoveLearner(node)
			s.rec.Remove(
				node,
				s.GetTableId(),
				selectPartition,
				pb.ReplicaRole_kLearner,
				s.Table.GetPartWeight(selectPartition),
			)
		} else {
			s.plan[selectPartition] = actions.FluentRemoveNode(node)
			s.rec.Remove(node, s.GetTableId(), selectPartition, selectRole, s.Table.GetPartWeight(selectPartition))
		}
	} else {
		logging.Warning("%s-%s: %s can't find remove to remove to fix scheduling stuck", s.logName, s.GetTableName(), node)
	}
}

func (s *hashGroupArranger) adjust(newPlacement []briefMembership) {
	summary := newScheduleSummary(s.rec.Clone())
	for i := int32(0); i < s.GetPartsCount(); i++ {
		if s.HasPlan(i) {
			continue
		}
		s.adjustPartition(i, newPlacement[i], summary)
	}
	if summary.scheduleStuck() {
		s.fixStuckSchedule(summary.topoSortFindTailNode(), newPlacement)
	}
}

func (s *hashGroupArranger) getNextDisorderedHub(newPlacement []briefMembership) string {
	hubOrdered := map[string]int{}

	for i := int32(0); i < s.GetPartsCount(); i++ {
		members := s.GetMembership(i)
		inHubs, _ := members.DivideByHubs(s.Nodes, s.Hubs)
		for hub, nodes := range inHubs {
			if len(nodes) == 1 && newPlacement[i][hub] == nodes[0] &&
				members.GetMember(nodes[0]).IsServing() {
				hubOrdered[hub]++
			}
		}
	}

	for _, hub := range s.Hubs {
		if hubOrdered[hub.Name] != int(s.GetPartsCount()) {
			logging.Info(
				"%s-%s: get first disorder hub %s, the ordered partition count: %d",
				s.logName,
				s.GetTableName(),
				hub.Name,
				hubOrdered[hub.Name],
			)
			return hub.Name
		}
	}
	return ""
}

func (s *hashGroupArranger) Schedule(
	input *ScheduleInput,
	rec *recorder.AllNodesRecorder,
	plan SchedulePlan,
) {
	s.Initialize(input, rec, plan)
	if s.cache == nil {
		s.cache = &deciderCache{
			logName:  s.logName,
			deciders: map[string]*cachedDeciderItem{},
		}
	}
	newPlacement := s.generateNewPlacement()
	s.nextDisorderHub = s.getNextDisorderedHub(newPlacement)
	s.adjust(newPlacement)
}
