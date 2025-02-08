package table_model

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
)

var (
	validRoleTransform = [][]bool{
		{false, true, true},
		{true, false, true},
		{false, true, false},
	}
)

type PartitionMembership struct {
	MembershipVersion int64                     `json:"version"`
	SplitVersion      int32                     `json:"split_version"`
	Peers             map[string]pb.ReplicaRole `json:"peers"`
	RestoreVersion    map[string]int64          `json:"restore_version"`
}

func (p *PartitionMembership) Initialize() {
	p.Peers = make(map[string]pb.ReplicaRole)
	p.RestoreVersion = make(map[string]int64)
}

func (p *PartitionMembership) CloneTo(membership *PartitionMembership) {
	membership.MembershipVersion = p.MembershipVersion
	membership.SplitVersion = p.SplitVersion
	membership.Peers = make(map[string]pb.ReplicaRole)
	for node, role := range p.Peers {
		membership.Peers[node] = role
	}
	membership.RestoreVersion = make(map[string]int64)
	for node, rv := range p.RestoreVersion {
		membership.RestoreVersion[node] = rv
	}
}

func (p *PartitionMembership) HasMember(node string) bool {
	_, ok := p.Peers[node]
	return ok
}

func (p *PartitionMembership) GetMember(node string) pb.ReplicaRole {
	if role, ok := p.Peers[node]; ok {
		return role
	}
	return pb.ReplicaRole_kInvalid
}

func (p *PartitionMembership) Transform(nodeId string, to pb.ReplicaRole) {
	from, ok := p.Peers[nodeId]
	logging.Assert(ok, "can't find node %s to transform", nodeId)
	logging.Assert(
		validRoleTransform[int(from)][int(to)],
		"%s: invalid role transform from %s to %s",
		nodeId,
		from.String(),
		to.String(),
	)
	p.Peers[nodeId] = to
	p.MembershipVersion++
}

func (p *PartitionMembership) AddLearner(nodeId string, rv int64) {
	logging.Assert(!p.HasMember(nodeId), "node %s already in partition", nodeId)
	p.Peers[nodeId] = pb.ReplicaRole_kLearner
	p.RestoreVersion[nodeId] = rv
	p.MembershipVersion++
}

func (p *PartitionMembership) RemoveLearner(nodeId string) {
	role, ok := p.Peers[nodeId]
	logging.Assert(ok, "can't find node %s", nodeId)
	logging.Assert(
		role == pb.ReplicaRole_kLearner,
		"remove %s:%s is not allowed",
		nodeId,
		role.String(),
	)
	delete(p.Peers, nodeId)
	delete(p.RestoreVersion, nodeId)
	p.MembershipVersion++
}

func (p *PartitionMembership) RemoveNode(nodeId string) {
	_, ok := p.Peers[nodeId]
	logging.Assert(ok, "can't find node %s", nodeId)
	delete(p.Peers, nodeId)
	delete(p.RestoreVersion, nodeId)
	p.MembershipVersion++
}

func (p *PartitionMembership) DivideRoles() (pri []string, sec []string, learner []string) {
	for node, role := range p.Peers {
		switch role {
		case pb.ReplicaRole_kPrimary:
			pri = append(pri, node)
		case pb.ReplicaRole_kSecondary:
			sec = append(sec, node)
		case pb.ReplicaRole_kLearner:
			learner = append(learner, node)
		}
	}
	return
}

func (p *PartitionMembership) CountServing(
	n *node_mgr.NodeStats,
	hubs []*pb.ReplicaHub,
) (map[string]int, int) {
	validHubs := map[string]bool{}
	for _, hub := range hubs {
		validHubs[hub.Name] = true
	}
	output := map[string]int{}
	total := 0
	for node, role := range p.Peers {
		info := n.MustGetNodeInfo(node)
		_, ok := validHubs[info.Hub]
		if ok && role.IsServing() {
			output[info.Hub]++
			total++
		}
	}
	return output, total
}

func (p *PartitionMembership) DivideByHubs(
	n *node_mgr.NodeStats,
	hubs []*pb.ReplicaHub,
) (inHub map[string][]string, outOfHub []string) {
	inHub = make(map[string][]string)
	for _, hub := range hubs {
		inHub[hub.Name] = []string{}
	}

	for node := range p.Peers {
		nodeInfo := n.MustGetNodeInfo(node)
		hub, ok := inHub[nodeInfo.Hub]
		if ok {
			inHub[nodeInfo.Hub] = append(hub, node)
		} else {
			logging.Info(
				"node %s with hub %s not belonging to valid hubs, which means that the hub is removed",
				nodeInfo.LogStr(),
				nodeInfo.Hub,
			)
			outOfHub = append(outOfHub, node)
		}
	}

	return
}
