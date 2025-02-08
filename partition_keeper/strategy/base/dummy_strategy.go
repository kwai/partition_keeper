package base

import (
	"strconv"
	"sync"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route/null_store"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type DummyStrategy struct {
}

func (e *DummyStrategy) GetType() pb.ServiceType {
	return pb.ServiceType_colossusdb_dummy
}

func (e *DummyStrategy) CreateCheckpointByDefault() bool {
	return true
}

func (e *DummyStrategy) SupportSplit() bool {
	return true
}

func (e *DummyStrategy) CheckSwitchPrimary(
	tpid utils.TblPartID,
	from *pb.PartitionReplica,
	to *pb.PartitionReplica,
) bool {
	return true
}

func (e *DummyStrategy) NotifySwitchPrimary(
	tpid utils.TblPartID,
	wg *sync.WaitGroup,
	address *utils.RpcNode,
	nodesConn *rpc.ConnPool,
) {
}

func (e *DummyStrategy) MakeStoreOpts(
	regions map[string]bool, service, table, jsonArgs string,
) ([]*route.StoreOption, error) {
	output := []*route.StoreOption{}
	for rg := range regions {
		opt := &route.StoreOption{
			Media: null_store.MediaType,
			StoreBaseOption: base.StoreBaseOption{
				Url:     rg,
				Service: service,
				Table:   table,
			},
		}
		output = append(output, opt)
	}
	return output, nil
}

func (e *DummyStrategy) Transform(
	tpid utils.TblPartID,
	self *pb.PartitionReplica,
	siblings []*pb.PartitionReplica,
	parents []*pb.PartitionReplica,
	opts *TableRunningOptions,
) pb.ReplicaRole {
	return StdTransform(self)
}

func (e *DummyStrategy) ValidateCreateTableArgs(table *pb.Table) error {
	return ValidateMsgQueueShards(int(table.PartsCount), table.JsonArgs)
}

func (e *DummyStrategy) GetSchedulerInfo(table *pb.Table) (string, int) {
	return GetScheduler(int(table.PartsCount), table.JsonArgs)
}

func (r *DummyStrategy) GetTableResource(jsonArgs string) utils.HardwareUnit {
	return nil
}

func (r *DummyStrategy) GetServerResource(
	info map[string]string,
	nodeId string,
) (utils.HardwareUnit, error) {
	output := utils.HardwareUnit{}
	addHardwareUnit := func(key, value string) {
		intVal, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return
		}
		output[key] = intVal
	}
	for key, value := range info {
		switch key {
		case pb.StdStat_cpu_percent.String():
			addHardwareUnit(utils.KSAT_CPU, value)
		case pb.StdStat_diskband_bytes.String():
			addHardwareUnit(utils.DISK_BAND, value)
		case pb.StdStat_netband_bytes.String():
			addHardwareUnit(utils.NET_BAND, value)
		case pb.StdStat_memcap_bytes.String():
			addHardwareUnit(utils.MEM_CAP, value)
		case pb.StdStat_diskcap_bytes.String():
			addHardwareUnit(utils.DISK_CAP, value)
		}
	}
	if len(output) == 0 {
		return nil, nil
	} else {
		return output, nil
	}
}

func (r *DummyStrategy) GetReplicaResource(
	pr *pb.PartitionReplica,
) (utils.HardwareUnit, error) {
	return nil, nil
}

func (r *DummyStrategy) DisallowServiceAccessOnlyNearestNodes(
	ksn string,
	serviceName string,
	tableName string,
	regions map[string]bool,
) {
	third_party.DisallowServiceAccessOnlyNearestNodes(ksn, serviceName, tableName, regions)
}

func CreateDummy() StrategyBase {
	return &DummyStrategy{}
}
