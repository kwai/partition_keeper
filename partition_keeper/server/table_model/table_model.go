package table_model

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
)

type TableModel interface {
	GetInfo() *pb.Table
	GetMembership(partId int32) *PartitionMembership
	GetPartWeight(partId int32) int
	PartSchedulable(partId int32) bool
	GetEstimatedReplicas() map[string]int
	GetEstimatedReplicasOnNode(nodeId string) int
	Record(nr recorder.NodesRecorder)
}
