package feature_server

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
)

type featureServerStrategy struct {
	base.DummyStrategy
}

func (e *featureServerStrategy) GetType() pb.ServiceType {
	return pb.ServiceType_colossusdb_feature_server
}

func (f *featureServerStrategy) CreateCheckpointByDefault() bool {
	return false
}

func (f *featureServerStrategy) SupportSplit() bool {
	return false
}

func Create() base.StrategyBase {
	return &featureServerStrategy{}
}
