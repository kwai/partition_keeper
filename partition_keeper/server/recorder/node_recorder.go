package recorder

import "github.com/kuaishou/open_partition_keeper/partition_keeper/pb"

type NodeRecorder interface {
	Add(tableId, partId int32, role pb.ReplicaRole, weight int)
	Remove(tableId, partId int32, role pb.ReplicaRole, weight int)
	Transform(tableId, partId int32, from, to pb.ReplicaRole, weight int)
	Count(role pb.ReplicaRole) int
	CountAll() int
	WeightedCount(role pb.ReplicaRole) int
	WeightedCountAll() int
	Clone() NodeRecorder
}
