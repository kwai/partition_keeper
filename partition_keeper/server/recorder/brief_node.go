package recorder

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type BriefNode struct {
	NodeId string
	Record map[pb.ReplicaRole]int
	Weight map[pb.ReplicaRole]int
}

func NewBriefNode(node string) NodeRecorder {
	return &BriefNode{
		NodeId: node,
		Record: make(map[pb.ReplicaRole]int),
		Weight: make(map[pb.ReplicaRole]int),
	}
}

func (b *BriefNode) Clone() NodeRecorder {
	output := NewBriefNode(b.NodeId).(*BriefNode)
	utils.GobClone(&output.Record, &b.Record)
	utils.GobClone(&output.Weight, &b.Weight)
	return output
}

func (b *BriefNode) Add(_, _ int32, role pb.ReplicaRole, weight int) {
	b.Record[role]++
	b.Weight[role] += weight
}

func (b *BriefNode) Remove(_, _ int32, role pb.ReplicaRole, weight int) {
	b.Record[role]--
	b.Weight[role] -= weight
}

func (b *BriefNode) Transform(t, p int32, from, to pb.ReplicaRole, weight int) {
	b.Remove(t, p, from, weight)
	b.Add(t, p, to, weight)
}

func (b *BriefNode) Count(role pb.ReplicaRole) int {
	return b.Record[role]
}

func (b *BriefNode) CountAll() (ans int) {
	ans = 0
	for _, v := range b.Record {
		ans += v
	}
	return
}

func (b *BriefNode) WeightedCount(role pb.ReplicaRole) int {
	return b.Weight[role]
}

func (b *BriefNode) WeightedCountAll() (ans int) {
	ans = 0
	for _, v := range b.Weight {
		ans += v
	}
	return
}
