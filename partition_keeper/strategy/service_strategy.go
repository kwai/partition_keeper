package strategy

import (
	"fmt"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/clotho"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/embedding_server"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/feart_server"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/feature_server"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/kgnn"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/partition_server"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/rodis"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/sim"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/sim_cache"
)

type ServiceStrategy = base.StrategyBase
type StrategyCreator func() ServiceStrategy

func NewServiceStrategy(svcType pb.ServiceType) (ServiceStrategy, error) {
	if creator, ok := strategyFactory[svcType]; ok {
		return creator(), nil
	}
	return nil, fmt.Errorf("can't recognize service type: %v", svcType)
}

func MustNewServiceStrategy(svcType pb.ServiceType) ServiceStrategy {
	st, err := NewServiceStrategy(svcType)
	logging.Assert(err == nil, "")
	return st
}

func RegisterServiceStrategy(svcType pb.ServiceType, creator StrategyCreator) {
	strategyFactory[svcType] = creator
}

var (
	strategyFactory = map[pb.ServiceType]StrategyCreator{}
)

func init() {
	strategyFactory[pb.ServiceType_colossusdb_dummy] = base.CreateDummy
	strategyFactory[pb.ServiceType_colossusdb_embedding_server] = embedding_server.Create
	strategyFactory[pb.ServiceType_colossusdb_feart_server] = feart_server.Create
	strategyFactory[pb.ServiceType_colossusdb_rodis] = rodis.Create
	strategyFactory[pb.ServiceType_colossusdb_feature_server] = feature_server.Create
	strategyFactory[pb.ServiceType_colossusdb_partition_server] = partition_server.Create
	strategyFactory[pb.ServiceType_colossusdb_sim_cache_server] = sim_cache.Create
	strategyFactory[pb.ServiceType_colossusdb_kgnn] = kgnn.Create
	strategyFactory[pb.ServiceType_colossusdb_sim_server] = sim.Create
	strategyFactory[pb.ServiceType_colossusdb_clotho] = clotho.Create
}
