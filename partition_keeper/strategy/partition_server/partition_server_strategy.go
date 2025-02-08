package partition_server

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
)

type partitionServerStrategy struct {
	base.DummyStrategy
}

func (e *partitionServerStrategy) GetType() pb.ServiceType {
	return pb.ServiceType_colossusdb_partition_server
}

func Create() base.StrategyBase {
	return &partitionServerStrategy{}
}

func (e *partitionServerStrategy) CreateCheckpointByDefault() bool {
	return false
}

func (e *partitionServerStrategy) SupportSplit() bool {
	return false
}

func (r *partitionServerStrategy) DisallowServiceAccessOnlyNearestNodes(
	ksn string,
	serviceName string,
	tableName string,
	regions map[string]bool,
) {
	third_party.DisallowServiceAccessOnlyNearestNodes(ksn, serviceName, tableName, regions)
}
