package watcher

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
)

const (
	AzSizeWatcherName = "az_size"
)

type AzSizeWatcher struct {
	req *pb.ExpandAzsRequest
}

func NewAzSizeWatcher() NodeWatcher {
	return &AzSizeWatcher{}
}

func (h *AzSizeWatcher) Name() string {
	return AzSizeWatcherName
}

func (h *AzSizeWatcher) StatSatisfied(
	logName string,
	nodes *node_mgr.NodeStats,
) (map[pb.AdminNodeOp][]*utils.RpcNode, bool) {
	hubSize := nodes.GetHubSize()
	azHubs := nodes.GetHubMap().DivideByAz()

	for _, opt := range h.req.AzOptions {
		hubs, ok := azHubs[opt.Az]
		if !ok {
			logging.Warning("%s: can't find hub for az %s, maybe removed", logName, opt.Az)
			continue
		}
		currentSize := 0
		for _, hub := range hubs {
			currentSize += hubSize[hub]
		}
		if currentSize < int(opt.NewSize) {
			logging.Info(
				"%s: az %s current size %d vs %d, not satisfied",
				logName,
				opt.Az,
				currentSize,
				opt.NewSize,
			)
			return nil, false
		}
	}
	logging.Info("%s: az size watcher for %v has satisfied", logName, h.req)
	return nil, true
}

func (h *AzSizeWatcher) Deserialize(data []byte) {
	h.req = &pb.ExpandAzsRequest{}
	utils.UnmarshalJsonOrDie(data, h.req)
}

func (h *AzSizeWatcher) Serialize() []byte {
	return utils.MarshalJsonOrDie(h.req)
}

func (h *AzSizeWatcher) UpdateParameters(parameters interface{}) {
	req := parameters.(*pb.ExpandAzsRequest)
	h.req = proto.Clone(req).(*pb.ExpandAzsRequest)
}

func (h *AzSizeWatcher) GetParameters() interface{} {
	return h.req
}

func (h *AzSizeWatcher) Clone() NodeWatcher {
	output := &AzSizeWatcher{
		req: proto.Clone(h.req).(*pb.ExpandAzsRequest),
	}
	return output
}

func init() {
	watcherFactory[AzSizeWatcherName] = NewAzSizeWatcher
}
