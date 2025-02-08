package actions

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
)

type TableModelMock struct {
	*pb.Table
	Partitions        []table_model.PartitionMembership
	EstimatedReplicas map[string]int
}

func NewTableModelMock(info *pb.Table) *TableModelMock {
	output := &TableModelMock{
		Table:      info,
		Partitions: make([]table_model.PartitionMembership, info.PartsCount),
	}
	for i := range output.Partitions {
		output.Partitions[i].Peers = make(map[string]pb.ReplicaRole)
		output.Partitions[i].RestoreVersion = make(map[string]int64)
	}
	return output
}

func (t *TableModelMock) GetPartWeight(partId int32) int {
	if t.Partitions[partId].SplitVersion == t.SplitVersion {
		return 1
	}
	if partId < t.PartsCount/2 {
		return 2
	}
	return 0
}

func (t *TableModelMock) PartSchedulable(partId int32) bool {
	if t.Partitions[partId].SplitVersion == t.SplitVersion {
		return true
	}
	return partId < t.PartsCount/2
}

func (t *TableModelMock) GetInfo() *pb.Table {
	return t.Table
}

func (t *TableModelMock) GetMembership(pid int32) *table_model.PartitionMembership {
	return &(t.Partitions[pid])
}

func (t *TableModelMock) GetEstimatedReplicas() map[string]int {
	return t.EstimatedReplicas
}

func (t *TableModelMock) GetEstimatedReplicasOnNode(nid string) int {
	return t.EstimatedReplicas[nid]
}

func (t *TableModelMock) Record(nr recorder.NodesRecorder) {
	for i := int32(0); i < t.PartsCount; i++ {
		for node, role := range t.Partitions[i].Peers {
			nr.Add(node, t.TableId, i, role, t.GetPartWeight(i))
		}
	}
}

func (t *TableModelMock) GetActionAcceptor(pid int32) *PartitionMock {
	return &PartitionMock{
		membership: t.GetMembership(pid),
	}
}
