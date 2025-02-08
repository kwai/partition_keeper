package kgnn

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
)

type KgnnStrategy struct {
	base.DummyStrategy
}

func (e *KgnnStrategy) GetType() pb.ServiceType {
	return pb.ServiceType_colossusdb_kgnn
}

func Create() base.StrategyBase {
	return &KgnnStrategy{}
}

func (f *KgnnStrategy) CreateCheckpointByDefault() bool {
	return true
}

func (f *KgnnStrategy) SupportSplit() bool {
	return false
}
