package server

import (
	"fmt"
	"sync"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/dbg"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"context"
)

type replicaFact struct {
	*pb.PartitionReplica
	partSplitVersion  int32
	membershipVersion int64
}

// implements actions.ActionAcceptor
type partition struct {
	table             *TableStats
	partId            int32
	members           table_model.PartitionMembership
	newMembers        table_model.PartitionMembership
	membershipChanged bool
	customRunner      func(utils.TblPartID, *sync.WaitGroup)
	facts             map[string]*replicaFact
	hasZkPath         bool
}

func (p *partition) logStr() string {
	return fmt.Sprintf(
		"%s t%d.p%d version: %d",
		p.table.TableName,
		p.table.TableId,
		p.partId,
		p.members.MembershipVersion,
	)
}

func (p *partition) initialize(table *TableStats, partId int32) {
	p.table = table
	p.partId = partId
	p.members.Initialize()
	p.membershipChanged = false
	p.customRunner = nil
	p.facts = make(map[string]*replicaFact)
	p.hasZkPath = false
}

func (p *partition) logHead() string {
	return fmt.Sprintf("%s t%d.p%d", p.table.TableName, p.table.TableId, p.partId)
}

func (p *partition) getReplicaFact(node string) *replicaFact {
	return p.facts[node]
}

func (p *partition) getPartitionReplica(node string) *pb.PartitionReplica {
	fact := p.facts[node]
	if fact == nil {
		return nil
	}
	return fact.PartitionReplica
}

func (p *partition) getPeerPartitionReplicas(excludeNode string) []*pb.PartitionReplica {
	output := []*pb.PartitionReplica{}
	for node, peer := range p.facts {
		if node != excludeNode {
			output = append(output, peer.PartitionReplica)
		}
	}
	return output
}

func (p *partition) isChild() bool {
	return p.members.SplitVersion < p.table.SplitVersion && p.partId >= p.table.PartsCount/2
}

func (p *partition) getParent() *partition {
	if !p.isChild() {
		return nil
	}
	return &(p.table.currParts[p.partId-p.table.PartsCount/2])
}

func (p *partition) isParent() bool {
	return p.members.SplitVersion < p.table.SplitVersion && p.partId < p.table.PartsCount/2
}

func (p *partition) getChild() *partition {
	if !p.isParent() {
		return nil
	}
	return &(p.table.currParts[p.partId+p.table.PartsCount/2])
}

func (p *partition) getWeight() int {
	if p.members.SplitVersion == p.table.SplitVersion {
		return 1
	}
	if p.isParent() {
		return 2
	} else {
		return 0
	}
}

func (p *partition) schedulable() bool {
	// we can schedule a normal partition
	if p.members.SplitVersion == p.table.SplitVersion {
		return true
	}
	// we can't schedule a child
	if p.isChild() {
		return false
	}
	// we can only schedule a parent if it hasn't started a split
	child := p.getChild()
	return child.members.MembershipVersion == 0 && len(child.newMembers.Peers) == 0
}

func (p *partition) checkPassiveTransform(nodeId string) pb.ReplicaRole {
	currentRole := p.members.GetMember(nodeId)
	logging.Assert(currentRole != pb.ReplicaRole_kInvalid, "")
	if fact, ok := p.facts[nodeId]; ok {
		if fact.membershipVersion != p.members.MembershipVersion || fact.Role != currentRole {
			logging.Error("%s t%d.p%d role of %s not in consistency, current: %s@%d, fact: %s@%d",
				p.table.TableName,
				p.table.TableId,
				p.partId,
				nodeId,
				currentRole.String(),
				p.members.MembershipVersion,
				fact.Role.String(),
				fact.membershipVersion,
			)
			return currentRole
		}
		var parentReplicas []*pb.PartitionReplica = nil
		if parent := p.getParent(); parent != nil {
			parentReplicas = parent.getPeerPartitionReplicas("")
		}
		target := p.table.svcStrategy.Transform(
			utils.MakeTblPartID(p.table.TableId, p.partId),
			fact.PartitionReplica,
			p.getPeerPartitionReplicas(nodeId),
			parentReplicas,
			p.table.optsForSvcStrategy(),
		)
		if target == pb.ReplicaRole_kInvalid {
			logging.Info(
				"[change_state] %s t%d.p%d node %s must be removed as %s by service strategy",
				p.table.TableName,
				p.table.TableId,
				p.partId,
				nodeId,
				currentRole.String(),
			)
			p.RemoveNode(nodeId)
			return pb.ReplicaRole_kInvalid
		}
		if target != currentRole {
			logging.Info(
				"[change_state] %s t%d.p%d node %s must transform from %s to %s passively by service strategy",
				p.table.TableName,
				p.table.TableId,
				p.partId,
				nodeId,
				currentRole.String(),
				target.String(),
			)
			p.Transform(nodeId, target)
			return target
		}
	}
	return currentRole
}

func (p *partition) PromoteLearner(node string) actions.ActionResult {
	newRole := p.checkPassiveTransform(node)
	switch newRole {
	case pb.ReplicaRole_kSecondary:
		return actions.RunActionOk
	case pb.ReplicaRole_kLearner:
		return actions.ActionShouldPending
	default:
		return actions.ActionShouldAbort
	}
}

func (p *partition) Transform(nodeId string, to pb.ReplicaRole) {
	logging.Info(
		"[change_state] %s t%d.p%d version: %d, start to transform %s: %s -> %s",
		p.table.TableName,
		p.table.TableId,
		p.partId,
		p.members.MembershipVersion,
		nodeId,
		p.members.Peers[nodeId],
		to,
	)
	p.members.CloneTo(&(p.newMembers))
	p.newMembers.Transform(nodeId, to)
	if to == pb.ReplicaRole_kLearner {
		if fact, ok := p.facts[nodeId]; ok {
			fact.ReadyToPromote = false
		}
	}
	p.membershipChanged = true
}

func (p *partition) CheckSwitchPrimary(from, to string) actions.ActionResult {
	from_fact, ok := p.facts[from]
	if !ok {
		logging.Error("%s: Check switch primary can't get facts from %s", p.logStr(), from)
		return actions.ActionShouldPending
	}
	to_fact, ok := p.facts[to]
	if !ok {
		logging.Error("%s: Check switch primary can't get facts from %s", p.logStr(), to)
		return actions.ActionShouldPending
	}

	if p.table.svcStrategy.CheckSwitchPrimary(
		utils.MakeTblPartID(p.table.TableId, p.partId),
		from_fact.PartitionReplica,
		to_fact.PartitionReplica,
	) {
		return actions.RunActionOk
	} else {
		oldPrimary := p.table.nodes.MustGetNodeInfo(from).Address
		p.customRunner = func(tpid utils.TblPartID, wg *sync.WaitGroup) {
			p.table.svcStrategy.NotifySwitchPrimary(tpid, wg, oldPrimary, p.table.nodesConn)
		}
		return actions.ActionShouldPending
	}
}

func (p *partition) AtomicSwitchPrimary(from, to string) actions.ActionResult {
	logging.Info("[change_state] %s: start to switch primary %s -> %s", p.logStr(), from, to)
	if p.members.GetMember(from) != pb.ReplicaRole_kPrimary {
		logging.Error("%s: %s is not primary, perhaps a stale plan not cancel", p.logStr(), from)
		return actions.ActionShouldAbort
	}
	if p.members.GetMember(to) != pb.ReplicaRole_kSecondary {
		logging.Error("%s: %s is not secondary, perhaps a stale plan not cancel", p.logStr(), to)
		return actions.ActionShouldAbort
	}

	result := p.CheckSwitchPrimary(from, to)
	if result != actions.RunActionOk {
		return result
	}

	p.members.CloneTo(&(p.newMembers))
	p.newMembers.Transform(from, pb.ReplicaRole_kSecondary)
	p.newMembers.Transform(to, pb.ReplicaRole_kPrimary)
	p.membershipChanged = true
	return actions.RunActionOk
}

func (p *partition) AddLearner(nodeId string) actions.ActionResult {
	conductor := p.table.planConductor
	if conductor != nil && !conductor.TryAddLearner(p.logStr(), p.partId, &p.members, nodeId) {
		return actions.ActionShouldPending
	}

	logging.Info(
		"[change_state] %s t%d.p%d version: %d, restore version: %d, start to add learner %s",
		p.table.TableName,
		p.table.TableId,
		p.partId,
		p.members.MembershipVersion,
		p.table.RestoreVersion,
		nodeId,
	)
	p.members.CloneTo(&(p.newMembers))
	p.newMembers.AddLearner(nodeId, p.table.RestoreVersion)
	p.membershipChanged = true
	return actions.RunActionOk
}

func (p *partition) RemoveLearner(nodeId string) {
	logging.Info("[change_state] %s t%d.p%d version: %d, start to remove learner %s",
		p.table.TableName, p.table.TableId, p.partId, p.members.MembershipVersion, nodeId)
	p.members.CloneTo(&(p.newMembers))
	p.newMembers.RemoveLearner(nodeId)
	delete(p.facts, nodeId)
	p.membershipChanged = true
}

func (p *partition) RemoveNode(nodeId string) {
	logging.Info("[change_state] %s t%d.p%d version: %d, start to remove node %s of role: %s",
		p.table.TableName,
		p.table.TableId,
		p.partId,
		p.members.MembershipVersion,
		nodeId,
		p.members.GetMember(nodeId).String(),
	)
	p.members.CloneTo(&(p.newMembers))
	p.newMembers.RemoveNode(nodeId)
	delete(p.facts, nodeId)
	p.membershipChanged = true
}

func (p *partition) CreateMembers(members *table_model.PartitionMembership) {
	logging.Info(
		"[change_state] %s t%d.p%d, old: %v, new: %v",
		p.table.TableName,
		p.table.TableId,
		p.partId,
		&(p.members),
		members,
	)
	members.CloneTo(&(p.newMembers))
	p.membershipChanged = true
}

func (p *partition) updateDeadFact(nodeId string, version int64) {
	if f, ok := p.facts[nodeId]; ok {
		f.PartitionReplica.Role = pb.ReplicaRole_kLearner
		f.PartitionReplica.ReadyToPromote = false
		f.membershipVersion = version
	}
}

func (p *partition) updateFact(
	nodeId string,
	version int64,
	splitVersion int32,
	rep *pb.PartitionReplica,
) bool {
	logging.Assert(
		version <= p.members.MembershipVersion,
		"%s t%d.p%d node %s version(%d) larger than keeper's(%d)",
		p.table.Table.TableName,
		p.table.TableId,
		p.partId,
		nodeId,
		version,
		p.members.MembershipVersion,
	)

	if p.members.HasMember(nodeId) {
		if p.members.RestoreVersion[nodeId] > rep.RestoreVersion {
			logging.Info(
				"%s t%d.p%d node %s report restore version(%d) smaller than keeper's(%d), reject it",
				p.table.Table.TableName,
				p.table.TableId,
				p.partId,
				nodeId,
				rep.RestoreVersion,
				p.members.RestoreVersion[nodeId],
			)
			logging.Assert(
				p.table.restoreCtrl != nil,
				"%s t%d.p%d version smaller, but not in restore",
				p.table.Table.TableName,
				p.table.TableId,
				p.partId,
			)
			delete(p.facts, nodeId)
			return false
		} else {
			oldFact := p.facts[nodeId]
			p.facts[nodeId] = &replicaFact{
				membershipVersion: version,
				partSplitVersion:  splitVersion,
				PartitionReplica:  rep,
			}
			if oldFact == nil {
				logging.Info(
					"%s t%d.p%d get first node fact from %s, version: %d, rv: %d, sv: %d, scv: %d",
					p.table.TableName,
					p.table.TableId,
					p.partId,
					nodeId,
					version,
					rep.RestoreVersion,
					splitVersion,
					rep.SplitCleanupVersion,
				)
			} else if oldFact.membershipVersion != version ||
				oldFact.partSplitVersion != splitVersion ||
				oldFact.SplitCleanupVersion != rep.SplitCleanupVersion {
				logging.Info("%s t%d.p%d: refresh node fact from %s, version: %d -> %d, sv: %d -> %d, scv: %d -> %d",
					p.table.TableName,
					p.table.TableId,
					p.partId,
					nodeId,
					oldFact.membershipVersion,
					version,
					oldFact.partSplitVersion,
					splitVersion,
					oldFact.SplitCleanupVersion,
					rep.SplitCleanupVersion,
				)
			}
			return true
		}
	}
	return false
}

func (p *partition) getOccupiedResource() (utils.HardwareUnit, bool) {
	sum := utils.HardwareUnit{}
	sampled := 0
	for _, fact := range p.facts {
		if fact.membershipVersion != p.members.MembershipVersion {
			continue
		}
		if fact.Role == pb.ReplicaRole_kLearner {
			continue
		}
		res, err := p.table.svcStrategy.GetReplicaResource(fact.PartitionReplica)
		if err == nil {
			sampled++
			sum.Add(res)
		} else {
			logging.Warning("%s t%d.p%d: get replica resource failed: %s",
				p.table.TableName,
				p.table.TableId,
				p.partId,
				err.Error(),
			)
		}
	}
	if sampled == 0 {
		return nil, false
	}
	sum.Divide(int64(sampled))
	return sum, true
}

// this function is mainly used for test
func (p *partition) isConsistency(nodes *node_mgr.NodeStats) bool {
	for node := range p.members.Peers {
		if !nodes.MustGetNodeInfo(node).IsAlive {
			continue
		}
		rf, ok := p.facts[node]
		if !ok {
			return false
		}
		if rf.membershipVersion != p.members.MembershipVersion {
			return false
		}
	}
	return true
}

func (p *partition) addReplicaToNode(
	info *node_mgr.NodeInfo,
	wg *sync.WaitGroup,
	shouldSchedule bool,
) {
	req := &pb.AddReplicaRequest{
		Part:              p.table.GetPbPartitionInfo(p.partId),
		PeerInfo:          p.toPbPartitionInfo(p.table.nodes, false),
		EstimatedReplicas: int32(p.table.GetEstimatedReplicasOnNode(info.Id)),
		ForRestore:        p.table.restoreCtrl != nil,
		AuthKey:           p.table.namespace,
		ParentInfo:        nil,
	}
	if parent := p.getParent(); parent != nil {
		req.ParentInfo = parent.toPbPartitionInfo(p.table.nodes, false)
	}
	address := info.Address
	if dbg.RunInSafeMode() || !shouldSchedule {
		logging.Info(
			"[change_state] ask %s to add replica t%d.p%d, skip schedule",
			address.String(),
			req.Part.TableId,
			req.Part.PartitionId,
		)
		return
	}
	wg.Add(1)
	go p.table.nodesConn.Run(
		address,
		func(psClient interface{}) error {
			defer wg.Done()
			psc := psClient.(pb.PartitionServiceClient)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			status, e := psc.AddReplica(ctx, req)
			if e != nil {
				logging.Warning(
					"[change_state] ask %s to add replica t%d.p%d failed, estimated replicas: %d: %s",
					address.String(),
					req.Part.TableId,
					req.Part.PartitionId,
					req.EstimatedReplicas,
					e.Error(),
				)
			} else if status.Code != int32(pb.PartitionError_kOK) {
				logging.Warning("[change_state] ask %s to add replica t%d.p%d, estimated replicas: %d response: %s",
					address.String(),
					req.Part.TableId,
					req.Part.PartitionId,
					req.EstimatedReplicas,
					status.Message,
				)
			} else {
				logging.Info("[change_state] ask %s to add replica t%d.p%d succeed, estimated replicas: %d",
					address.String(),
					req.Part.TableId,
					req.Part.PartitionId,
					req.EstimatedReplicas,
				)
			}
			return e
		},
	)
}

func (p *partition) notifySplit(info *node_mgr.NodeInfo, wg *sync.WaitGroup, shouldSchedule bool) {
	req := &pb.ReplicaSplitRequest{
		TableId:     p.table.TableId,
		PartitionId: p.partId,
		NewTableInfo: &pb.DynamicTableInfo{
			PartitionNum:      p.table.PartsCount,
			TableSplitVersion: p.table.SplitVersion,
		},
		Peers:      p.toPbPartitionInfo(p.table.nodes, false),
		ChildPeers: nil,
	}
	if child := p.getChild(); child != nil {
		req.ChildPeers = child.toPbPartitionInfo(p.table.nodes, false)
		req.Peers = p.toPbPartitionInfo(p.table.nodes, false)
	}

	address := info.Address
	if dbg.RunInSafeMode() || !shouldSchedule {
		logging.Info("[change_state] ask %s to split t%d.p%d, skip sched",
			address.String(),
			req.TableId,
			req.PartitionId,
		)
		return
	}
	wg.Add(1)
	go p.table.nodesConn.Run(address, func(psClient interface{}) error {
		defer wg.Done()
		psc := psClient.(pb.PartitionServiceClient)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		status, e := psc.ReplicaSplit(ctx, req)
		if e != nil {
			logging.Warning(
				"[change_state] ask %s to split t%d.p%d failed: %s",
				address.String(),
				req.TableId,
				req.PartitionId,
				e.Error(),
			)
		} else if status.Code != int32(pb.PartitionError_kOK) {
			logging.Warning("[change_state] ask %s to split t%d.p%d response: %s",
				address.String(), req.TableId, req.PartitionId, status.Message)
		} else {
			logging.Info("[change_state] ask %s to split t%d.p%d succeed",
				address.String(), req.TableId, req.PartitionId)
		}
		return e
	})
}

func (p *partition) spawnReplica(
	info *node_mgr.NodeInfo,
	wg *sync.WaitGroup,
	shouldSchedule bool,
) {
	if parent := p.getParent(); parent != nil {
		logging.Assert(parent.members.SplitVersion == p.members.SplitVersion, "")
		if parent.members.HasMember(info.Id) {
			parent.notifySplit(info, wg, shouldSchedule)
		} else {
			logging.Warning("%s: can't find parent at %s, skip to create child", p.logHead(), info.Id)
		}
		return
	}
	p.addReplicaToNode(info, wg, shouldSchedule)
}

func (p *partition) notifyNodeReconfigure(
	info *node_mgr.NodeInfo,
	wg *sync.WaitGroup,
	shouldSchedule bool,
) {
	// reconfigure
	req := &pb.ReconfigPartitionRequest{
		PartitionId: p.partId,
		TableId:     p.table.TableId,
		AuthKey:     p.table.namespace,
		PeerInfo:    p.toPbPartitionInfo(p.table.nodes, false),
		ParentInfo:  nil,
		TableInfo: &pb.DynamicTableInfo{
			PartitionNum:      p.table.PartsCount,
			TableSplitVersion: p.table.SplitVersion,
		},
	}
	if parent := p.getParent(); parent != nil {
		req.ParentInfo = parent.toPbPartitionInfo(p.table.nodes, false)
	}
	address := info.Address
	if dbg.RunInSafeMode() || !shouldSchedule {
		logging.Info(
			"[change_state] ask %s to reconfig t%d.p%d version to %d, skip schedule",
			address.String(),
			req.TableId,
			req.PartitionId,
			req.PeerInfo.MembershipVersion,
		)
		return
	}
	wg.Add(1)
	go p.table.nodesConn.Run(address, func(client interface{}) error {
		defer wg.Done()
		psc := client.(pb.PartitionServiceClient)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		status, e := psc.Reconfigure(ctx, req)
		if e != nil {
			logging.Warning("[change_state] ask %s to reconfig t%d.p%d version %d failed: %s",
				address.String(), req.TableId, req.PartitionId,
				req.PeerInfo.MembershipVersion, e.Error())
		} else if status.Code != int32(pb.PartitionError_kOK) {
			logging.Warning("[change_state] ask %s to reconfig t%d.p%d version %d response: %s",
				address.String(), req.TableId, req.PartitionId,
				req.PeerInfo.MembershipVersion, status.Message)
		} else {
			logging.Info("[change_state] ask %s to reconfig t%d.p%d version %d succeed",
				address.String(), req.TableId, req.PartitionId,
				req.PeerInfo.MembershipVersion)
		}
		return e
	})
}

func (p *partition) reconcileFacts(wg *sync.WaitGroup) bool {
	inConsistency := true
	for node := range p.members.Peers {
		rf, ok := p.facts[node]
		if ok && rf.membershipVersion == p.members.MembershipVersion {
			continue
		}
		info := p.table.nodes.MustGetNodeInfo(node)
		if !info.IsAlive {
			continue
		}
		inConsistency = false
		// add replica
		if !ok {
			p.spawnReplica(info, wg, p.table.shouldScheduleNode(info.Id))
		} else {
			p.notifyNodeReconfigure(info, wg, p.table.shouldScheduleNode(info.Id))
		}
	}
	return inConsistency
}

func (p *partition) durableNewMembership() {
	path := p.table.getPartitionPath(p.partId)
	if p.hasZkPath {
		logging.Assert(
			p.table.zkConn.Set(context.Background(), path, utils.MarshalJsonOrDie(p.newMembers)),
			"",
		)
	} else {
		logging.Assert(p.table.zkConn.Create(context.Background(), path, utils.MarshalJsonOrDie(p.newMembers)), "")
	}
}

func (p *partition) getDurableMembershipOp() metastore.WriteOp {
	path := p.table.getPartitionPath(p.partId)
	if p.hasZkPath {
		return &metastore.SetOp{
			Path: path,
			Data: utils.MarshalJsonOrDie(p.newMembers),
		}
	} else {
		return &metastore.CreateOp{
			Path: path,
			Data: utils.MarshalJsonOrDie(p.newMembers),
		}
	}
}

func (p *partition) applyNewMembership() {
	p.members = p.newMembers
	p.hasZkPath = true
	p.membershipChanged = false
}

func (p *partition) toPbPartitionInfo(
	nodes *node_mgr.NodeStats,
	withReplicaInfos bool,
) *pb.PartitionPeerInfo {
	result := &pb.PartitionPeerInfo{
		MembershipVersion: p.members.MembershipVersion,
		SplitVersion:      p.members.SplitVersion,
		Peers:             []*pb.PartitionReplica{},
	}

	for node, role := range p.members.Peers {
		info := nodes.MustGetNodeInfo(node)
		replicaPb := &pb.PartitionReplica{
			Role:           role,
			Node:           info.Address.ToPb(),
			NodeUniqueId:   info.Id,
			HubName:        info.Hub,
			RestoreVersion: p.members.RestoreVersion[node],
		}
		if withReplicaInfos {
			replica := p.getPartitionReplica(node)
			if replica != nil {
				replicaPb.ReadyToPromote = replica.ReadyToPromote
				replicaPb.StatisticsInfo = make(map[string]string)
				for key, value := range replica.StatisticsInfo {
					replicaPb.StatisticsInfo[key] = value
				}
				replicaPb.SplitCleanupVersion = replica.SplitCleanupVersion
			}
		}
		result.Peers = append(result.Peers, replicaPb)
	}

	return result
}

func (p *partition) oldParentChangePrimary() bool {
	if !p.membershipChanged {
		return false
	}
	if !p.isParent() {
		return false
	}
	oldP, _, _ := p.members.DivideRoles()
	newP, _, _ := p.newMembers.DivideRoles()
	if len(oldP) != len(newP) {
		return true
	}
	if len(oldP) == 0 {
		return false
	}
	return oldP[0] != newP[0]
}

func (p *partition) oldChildPrimaryRecentElected() bool {
	if !p.membershipChanged {
		return false
	}
	if !p.isChild() {
		return false
	}
	pri, _, _ := p.newMembers.DivideRoles()
	return len(pri) > 0
}

func (p *partition) incSplitVersion() {
	logging.Assert(
		p.newMembers.SplitVersion < p.table.SplitVersion,
		"t%d.p%d %d vs %d",
		p.table.TableId,
		p.partId,
		p.newMembers.SplitVersion,
		p.table.SplitVersion,
	)
	if p.membershipChanged {
		p.newMembers.SplitVersion = p.table.SplitVersion
	} else {
		p.members.CloneTo(&p.newMembers)
		p.newMembers.MembershipVersion++
		p.newMembers.SplitVersion = p.table.SplitVersion
		p.membershipChanged = true
	}
}

func (p *partition) incMembershipVersion() {
	if p.membershipChanged {
		return
	}
	p.members.CloneTo(&p.newMembers)
	p.newMembers.MembershipVersion++
	p.membershipChanged = true
}

func (p *partition) notifySplitCleanup(
	info *node_mgr.NodeInfo,
	wg *sync.WaitGroup,
	shouldSchedule bool,
) {
	req := &pb.ReplicaSplitCleanupRequest{
		TableId:               p.table.TableId,
		PartitionId:           p.partId,
		PartitionSplitVersion: p.members.SplitVersion,
	}
	address := info.Address
	if dbg.RunInSafeMode() || !shouldSchedule {
		logging.Info(
			"[change_state] ask %s to do split-cleanup: %v, skip schedule",
			address.String(),
			req,
		)
		return
	}
	wg.Add(1)
	go p.table.nodesConn.Run(address, func(client interface{}) error {
		defer wg.Done()
		psc := client.(pb.PartitionServiceClient)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		status, e := psc.ReplicaSplitCleanup(ctx, req)
		if e != nil {
			logging.Warning(
				"[change_state] ask %s to do split-cleanup %v failed: %s",
				address.String(),
				req,
				e.Error(),
			)
		} else if status.Code != int32(pb.PartitionError_kOK) {
			logging.Warning("[change_state] ask %s to do split-cleanup %v got %s", address.String(), req, status.Message)
		} else {
			logging.Info("[change_state] ask %s to do split-cleanup %v succeed", address.String(), req)
		}
		return e
	})
}

func (p *partition) doSplitCleanup() bool {
	logging.Assert(
		p.members.SplitVersion == p.table.SplitVersion,
		"t%d.p%d %d vs %d",
		p.table.TableId,
		p.partId,
		p.members.SplitVersion,
		p.table.SplitVersion,
	)
	unfinishedReplicas := 0
	deadReplicas := 0

	wg := &sync.WaitGroup{}
	for node := range p.members.Peers {
		info := p.table.nodes.MustGetNodeInfo(node)
		if !info.IsAlive {
			logging.Info("%s node %s dead, skip cleanup check", p.logHead(), info.LogStr())
			deadReplicas++
			continue
		}
		fact := p.facts[node]
		if fact == nil {
			logging.Info(
				"%s can't get facts from %s, treat partition split not finished",
				p.logHead(), node,
			)
			unfinishedReplicas++
			continue
		}
		logging.Assert(fact.partSplitVersion <= p.members.SplitVersion, "")
		logging.Assert(fact.SplitCleanupVersion <= fact.partSplitVersion, "")
		if fact.SplitCleanupVersion == p.members.SplitVersion {
			logging.Verbose(1, "%s: %s has finished cleanup", p.logHead(), info.LogStr())
		} else {
			unfinishedReplicas++
			p.notifySplitCleanup(info, wg, p.table.shouldScheduleNode(node))
		}
	}
	wg.Wait()
	if unfinishedReplicas != 0 {
		logging.Info(
			"%s: %d replicas haven't finish split cleanup",
			p.logHead(),
			unfinishedReplicas,
		)
		return false
	}
	if deadReplicas > 1 || deadReplicas >= len(p.members.Peers) {
		logging.Info("%s: %d replicas dead for split cleanup, too many", p.logHead(), deadReplicas)
		return false
	}
	return true
}
