package recorder

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type NodePartitions struct {
	NodeId                 string
	Primaries              map[utils.TblPartID]int
	PrimariesTotalWeight   int
	Secondaries            map[utils.TblPartID]int
	SecondariesTotalWeight int
	Learners               map[utils.TblPartID]int
	LearnersTotalWeight    int
}

func NewNodePartitions(node string) NodeRecorder {
	return &NodePartitions{
		NodeId:                 node,
		Primaries:              make(map[utils.TblPartID]int),
		PrimariesTotalWeight:   0,
		Secondaries:            make(map[utils.TblPartID]int),
		SecondariesTotalWeight: 0,
		Learners:               make(map[utils.TblPartID]int),
		LearnersTotalWeight:    0,
	}
}

func (t *NodePartitions) Clone() NodeRecorder {
	output := NewNodePartitions(t.NodeId).(*NodePartitions)
	utils.GobClone(&output.Primaries, &t.Primaries)
	utils.GobClone(&output.Secondaries, &t.Secondaries)
	utils.GobClone(&output.Learners, &t.Learners)
	output.PrimariesTotalWeight = t.PrimariesTotalWeight
	output.SecondariesTotalWeight = t.SecondariesTotalWeight
	output.LearnersTotalWeight = t.LearnersTotalWeight
	return output
}

func (t *NodePartitions) Add(tableId, partId int32, role pb.ReplicaRole, weight int) {
	id := utils.MakeTblPartID(tableId, partId)
	switch role {
	case pb.ReplicaRole_kPrimary:
		t.Primaries[id] = weight
		t.PrimariesTotalWeight += weight
	case pb.ReplicaRole_kSecondary:
		t.Secondaries[id] = weight
		t.SecondariesTotalWeight += weight
	case pb.ReplicaRole_kLearner:
		t.Learners[id] = weight
		t.LearnersTotalWeight += weight
	}
}

func (t *NodePartitions) verifyWeight(id utils.TblPartID, role pb.ReplicaRole, weight int) {
	var container map[utils.TblPartID]int = nil
	switch role {
	case pb.ReplicaRole_kPrimary:
		container = t.Primaries
	case pb.ReplicaRole_kSecondary:
		container = t.Secondaries
	case pb.ReplicaRole_kLearner:
		container = t.Learners
	}
	if val, ok := container[id]; ok {
		logging.Assert(
			val == weight,
			"%s: %s weight %d not same as recorded: %d",
			id.String(),
			role.String(),
			weight,
			val,
		)
	}
}

func (t *NodePartitions) Remove(tableId, partId int32, role pb.ReplicaRole, weight int) {
	id := utils.MakeTblPartID(tableId, partId)
	t.verifyWeight(id, role, weight)
	switch role {
	case pb.ReplicaRole_kPrimary:
		delete(t.Primaries, id)
		t.PrimariesTotalWeight -= weight
	case pb.ReplicaRole_kSecondary:
		delete(t.Secondaries, id)
		t.SecondariesTotalWeight -= weight
	case pb.ReplicaRole_kLearner:
		delete(t.Learners, id)
		t.LearnersTotalWeight -= weight
	}
}

func (t *NodePartitions) Transform(tableId, partId int32, from, to pb.ReplicaRole, weight int) {
	t.Remove(tableId, partId, from, weight)
	t.Add(tableId, partId, to, weight)
}

func (t *NodePartitions) Count(role pb.ReplicaRole) int {
	switch role {
	case pb.ReplicaRole_kPrimary:
		return len(t.Primaries)
	case pb.ReplicaRole_kSecondary:
		return len(t.Secondaries)
	case pb.ReplicaRole_kLearner:
		return len(t.Learners)
	}
	return 0
}

func (t *NodePartitions) CountAll() int {
	return len(t.Primaries) + len(t.Secondaries) + len(t.Learners)
}

func (t *NodePartitions) WeightedCount(role pb.ReplicaRole) int {
	switch role {
	case pb.ReplicaRole_kPrimary:
		return t.PrimariesTotalWeight
	case pb.ReplicaRole_kSecondary:
		return t.SecondariesTotalWeight
	case pb.ReplicaRole_kLearner:
		return t.LearnersTotalWeight
	}
	return 0
}

func (t *NodePartitions) WeightedCountAll() int {
	return t.PrimariesTotalWeight + t.SecondariesTotalWeight + t.LearnersTotalWeight
}
