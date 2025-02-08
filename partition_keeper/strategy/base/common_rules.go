package base

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

func StdTransformLearner(self *pb.PartitionReplica) pb.ReplicaRole {
	if self.Role != pb.ReplicaRole_kLearner {
		return self.Role
	}
	if !utils.ContainsKey(self.StatisticsInfo, pb.StdReplicaStat_unavailable.String()) {
		if self.ReadyToPromote {
			return pb.ReplicaRole_kSecondary
		}
	}
	return pb.ReplicaRole_kLearner
}

func StdDowngrade(self *pb.PartitionReplica) pb.ReplicaRole {
	if self.Role == pb.ReplicaRole_kLearner {
		return self.Role
	}
	if utils.ContainsKey(self.StatisticsInfo, pb.StdReplicaStat_unavailable.String()) {
		return pb.ReplicaRole_kLearner
	}
	return self.Role
}

func StdTransform(self *pb.PartitionReplica) pb.ReplicaRole {
	switch self.Role {
	case pb.ReplicaRole_kLearner:
		return StdTransformLearner(self)
	default:
		return StdDowngrade(self)
	}
}
