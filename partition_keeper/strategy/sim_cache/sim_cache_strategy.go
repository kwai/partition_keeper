package sim_cache

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
)

type SimCacheStrategy struct {
	base.DummyStrategy
}

func (e *SimCacheStrategy) GetType() pb.ServiceType {
	return pb.ServiceType_colossusdb_sim_cache_server
}

func Create() base.StrategyBase {
	return &SimCacheStrategy{}
}

func (f *SimCacheStrategy) CreateCheckpointByDefault() bool {
	return false
}

func (f *SimCacheStrategy) SupportSplit() bool {
	return false
}
