package recorder

import "github.com/kuaishou/open_partition_keeper/partition_keeper/pb"

type NodesRecorder interface {
	Add(node string, tbl, part int32, r pb.ReplicaRole, weight int)
	GetNode(n string) NodeRecorder
	Clone() NodesRecorder
}

type BuildRecorder func(node string) NodeRecorder
