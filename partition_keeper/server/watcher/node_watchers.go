package watcher

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type NodeWatcher interface {
	Name() string
	UpdateParameters(parameters interface{})
	GetParameters() interface{}
	Clone() NodeWatcher
	StatSatisfied(
		logName string,
		nodes *node_mgr.NodeStats,
	) (map[pb.AdminNodeOp][]*utils.RpcNode, bool)
	Deserialize(data []byte)
	Serialize() []byte
}

type createNodeWatcher func() NodeWatcher

var (
	watcherFactory = map[string]createNodeWatcher{}
)

func NewWatcher(name string) NodeWatcher {
	return watcherFactory[name]()
}
