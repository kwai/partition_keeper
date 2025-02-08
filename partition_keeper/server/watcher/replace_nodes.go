package watcher

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
)

const (
	ReplaceNodesWatcherName = "replace_nodes"
)

type ReplaceNodesWatcher struct {
	req *pb.ReplaceNodesRequest
}

func NewReplaceNodesWatcher() NodeWatcher {
	return &ReplaceNodesWatcher{}
}

func (r *ReplaceNodesWatcher) Name() string {
	return ReplaceNodesWatcherName
}

func (r *ReplaceNodesWatcher) StatSatisfied(
	logName string,
	nodes *node_mgr.NodeStats,
) (map[pb.AdminNodeOp][]*utils.RpcNode, bool) {
	ops := map[pb.AdminNodeOp][]*utils.RpcNode{}
	addNodeOp := func(node *utils.RpcNode, op pb.AdminNodeOp) {
		ops[op] = append(ops[op], node)
	}

	for i := range r.req.SrcNodes {
		srcNode, dstNode := utils.FromPb(r.req.SrcNodes[i]), utils.FromPb(r.req.DstNodes[i])
		dstInfo := nodes.GetNodeInfoByAddr(dstNode, true)
		if dstInfo == nil {
			logging.Info("%s: haven't found node %s yet", logName, dstNode.String())
			return nil, false
		}
		srcInfo := nodes.GetNodeInfoByAddr(srcNode, true)
		if srcInfo != nil && srcInfo.Op != pb.AdminNodeOp_kOffline {
			logging.Assert(srcInfo.Hub == dstInfo.Hub, "")
			logging.Info(
				"%s: detect new node %s so try to offline %s",
				logName,
				dstNode.String(),
				srcNode.String(),
			)
			addNodeOp(srcNode, pb.AdminNodeOp_kOffline)
		}
	}

	if len(ops) == 0 {
		logging.Info("%s: replace nodes watcher %s has already satisfied", logName, r.req)
		return nil, true
	} else {
		return ops, false
	}
}

func (r *ReplaceNodesWatcher) UpdateParameters(parameters interface{}) {
	req := parameters.(*pb.ReplaceNodesRequest)
	r.req = proto.Clone(req).(*pb.ReplaceNodesRequest)
}

func (r *ReplaceNodesWatcher) GetParameters() interface{} {
	return r.req
}

func (r *ReplaceNodesWatcher) Clone() NodeWatcher {
	output := &ReplaceNodesWatcher{
		req: proto.Clone(r.req).(*pb.ReplaceNodesRequest),
	}
	return output
}

func (r *ReplaceNodesWatcher) Serialize() []byte {
	return utils.MarshalJsonOrDie(r.req)
}

func (r *ReplaceNodesWatcher) Deserialize(data []byte) {
	r.req = &pb.ReplaceNodesRequest{}
	utils.UnmarshalJsonOrDie(data, r.req)
}

func init() {
	watcherFactory[ReplaceNodesWatcherName] = NewReplaceNodesWatcher
}
