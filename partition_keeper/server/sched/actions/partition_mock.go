package actions

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
)

type PartitionMock struct {
	membership *table_model.PartitionMembership
}

func (p *PartitionMock) AddLearner(node string) ActionResult {
	p.membership.AddLearner(node, 0)
	return RunActionOk
}

func (p *PartitionMock) RemoveLearner(node string) {
	p.membership.RemoveLearner(node)
}

func (p *PartitionMock) RemoveNode(node string) {
	p.membership.RemoveNode(node)
}

func (p *PartitionMock) Transform(node string, role pb.ReplicaRole) {
	p.membership.Transform(node, role)
}

func (p *PartitionMock) PromoteLearner(node string) ActionResult {
	p.membership.Transform(node, pb.ReplicaRole_kSecondary)
	return RunActionOk
}

func (p *PartitionMock) AtomicSwitchPrimary(from, to string) ActionResult {
	p.membership.Transform(from, pb.ReplicaRole_kSecondary)
	p.membership.Transform(to, pb.ReplicaRole_kPrimary)
	return RunActionOk
}

func (p *PartitionMock) CreateMembers(members *table_model.PartitionMembership) {
	members.CloneTo(p.membership)
}
