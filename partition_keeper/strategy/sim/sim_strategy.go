package sim

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type SimStrategy struct {
	base.DummyStrategy
}

func (e *SimStrategy) GetType() pb.ServiceType {
	return pb.ServiceType_colossusdb_sim_server
}

func Create() base.StrategyBase {
	return &SimStrategy{}
}

func (f *SimStrategy) CreateCheckpointByDefault() bool {
	return true
}

func (f *SimStrategy) SupportSplit() bool {
	return false
}

func (r *SimStrategy) Transform(
	tpid utils.TblPartID,
	self *pb.PartitionReplica,
	others []*pb.PartitionReplica,
	parents []*pb.PartitionReplica,
	opts *base.TableRunningOptions,
) pb.ReplicaRole {
	value, ok := self.StatisticsInfo["schema_changing"]
	if !ok {
		return base.StdTransform(self)
	}
	if value == "true" && self.Role != pb.ReplicaRole_kLearner {
		return pb.ReplicaRole_kLearner
	}
	if value == "false" && self.Role == pb.ReplicaRole_kLearner {
		return pb.ReplicaRole_kSecondary
	}
	return self.Role
}
