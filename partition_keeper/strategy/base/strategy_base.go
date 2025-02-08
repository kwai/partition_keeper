package base

import (
	"sync"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type TableRunningOptions struct {
	IsRestoring bool
}

type StrategyBase interface {
	GetType() pb.ServiceType
	CreateCheckpointByDefault() bool
	SupportSplit() bool
	// return the expected target role
	// if no transform is necessary, please return current role
	// if the node must be removed, please return pb.ReplicaRole_kInvalid
	//
	// if parents is nil, this partition is an independent partition. otherwise
	// it's a child
	Transform(
		tpid utils.TblPartID,
		self *pb.PartitionReplica,
		siblings []*pb.PartitionReplica,
		parents []*pb.PartitionReplica,
		opts *TableRunningOptions,
	) pb.ReplicaRole
	MakeStoreOpts(
		regions map[string]bool,
		service, table, jsonArgs string,
	) ([]*route.StoreOption, error)
	ValidateCreateTableArgs(proto *pb.Table) error
	GetSchedulerInfo(proto *pb.Table) (string, int)
	// for same kind of resource, server/table/replica should
	// same key and value with same unit
	GetTableResource(jsonArgs string) utils.HardwareUnit
	GetServerResource(info map[string]string, nodeId string) (utils.HardwareUnit, error)
	GetReplicaResource(pr *pb.PartitionReplica) (utils.HardwareUnit, error)
	CheckSwitchPrimary(
		tpid utils.TblPartID,
		from *pb.PartitionReplica,
		to *pb.PartitionReplica,
	) bool
	NotifySwitchPrimary(
		tpid utils.TblPartID,
		wg *sync.WaitGroup,
		address *utils.RpcNode,
		nodesConn *rpc.ConnPool,
	)
	DisallowServiceAccessOnlyNearestNodes(
		ksn string,
		serviceName string,
		tableName string,
		regions map[string]bool,
	)
}
