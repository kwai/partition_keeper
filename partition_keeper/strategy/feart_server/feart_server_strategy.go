package feart_server

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
)

type feartServerStrategy struct {
	base.DummyStrategy
}

func (e *feartServerStrategy) GetType() pb.ServiceType {
	return pb.ServiceType_colossusdb_feart_server
}

func (e *feartServerStrategy) SupportSplit() bool {
	return false
}

func Create() base.StrategyBase {
	return &feartServerStrategy{}
}
