package node_mgr

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/est"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
)

var (
	flagCollectIntervalSecs = flag.Int64(
		"collect_interval_secs",
		5,
		"collect interval secs",
	)
)

type NodeCollector struct {
	serviceName string
	namespace   string
	pool        *rpc.ConnPool
	nodes       *NodeStats

	mu                  sync.Mutex
	collectIntervalSecs int64
	nodeInProcessing    map[string]bool
	dispatchFacts       chan *pb.GetReplicasResponse
	triggerCollect      chan bool
	quit                chan bool
	alert               *NodeAlert
}

type collectOpts func(*NodeCollector)

func WithCollectIntervalSecs(i int64) collectOpts {
	return func(nc *NodeCollector) {
		nc.collectIntervalSecs = i
	}
}

func NewNodeCollector(
	serviceName, namespace string,
	builder rpc.ConnPoolBuilder,
	opts ...collectOpts,
) *NodeCollector {
	ans := &NodeCollector{
		serviceName:         serviceName,
		namespace:           namespace,
		pool:                builder.Build(),
		nodes:               nil,
		collectIntervalSecs: *flagCollectIntervalSecs,
		alert:               NewNodeAlert(serviceName),
		nodeInProcessing:    make(map[string]bool),
		dispatchFacts:       make(chan *pb.GetReplicasResponse, 1000),
		triggerCollect:      make(chan bool),
		quit:                make(chan bool),
	}
	for _, opt := range opts {
		opt(ans)
	}
	return ans
}

func (n *NodeCollector) Start(nodes *NodeStats) <-chan *pb.GetReplicasResponse {
	logging.Info("%s: start collecting", n.serviceName)
	n.nodes = nodes
	go n.runCollecting()
	return n.dispatchFacts
}

func (n *NodeCollector) Stop() {
	logging.Info("%s: notify collecting to quit", n.serviceName)
	n.quit <- true
	logging.Info("%s: finish notifying collecting to quit", n.serviceName)
}

func (n *NodeCollector) NotifyNodeCollectingHandled(nodeId string) {
	n.finishProcessingNode(nodeId)
}

func (n *NodeCollector) TriggerCollect() {
	n.triggerCollect <- true
}

func (n *NodeCollector) markProcessingNode(nodeId string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	status, ok := n.nodeInProcessing[nodeId]
	if ok && status {
		return false
	}
	n.nodeInProcessing[nodeId] = true
	return true
}

func (n *NodeCollector) finishProcessingNode(nodeId string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.nodeInProcessing, nodeId)
}

func (n *NodeCollector) nodeHasSchedulePlan(nodeId string) bool {
	n.nodes.serviceLock.LockRead()
	defer n.nodes.serviceLock.UnlockRead()
	info := n.nodes.GetNodeInfo(nodeId)
	if info == nil {
		return false
	}
	return info.Score != est.INVALID_SCORE
}

func (n *NodeCollector) fillNodeEstimatedReplicas(nid string, req *pb.GetReplicasRequest) {
	if !n.nodeHasSchedulePlan(nid) {
		logging.Info("%s: %s has no schedule plan yet", n.serviceName, nid)
		return
	}
	req.FromPartitionKeeper = true
	req.EstimatedReplicas = map[int32]int32{}
	n.nodes.estReplicas.Get(nid, func(nodeCount map[int32]int) {
		if len(nodeCount) == 0 {
			return
		}
		for tid, count := range nodeCount {
			req.EstimatedReplicas[tid] = int32(count)
		}
	})
}

func (n *NodeCollector) collectNodeInfo(nid string, info *NodePing) {
	n.pool.Run(info.Address, func(psClient interface{}) error {
		client := psClient.(pb.PartitionServiceClient)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		start := time.Now()
		req := &pb.GetReplicasRequest{
			AuthKey: n.namespace,
		}
		n.fillNodeEstimatedReplicas(nid, req)

		resp, err := client.GetReplicas(ctx, req)
		if err != nil {
			logging.Warning(
				"%s: request to %v got rpc error: %s",
				n.serviceName,
				info.Address,
				err.Error(),
			)
			n.finishProcessingNode(nid)
			return err
		}
		elapse := time.Since(start).Microseconds()
		third_party.PerfLog1(
			"reco",
			"reco.colossusdb.get_replicas.cost",
			n.serviceName,
			info.Address.String(),
			uint64(elapse),
		)
		if resp.ServerInfo == nil {
			resp.ServerInfo = &pb.ServerInfo{
				NodeId: nid,
			}
		} else {
			resp.ServerInfo.NodeId = nid
		}

		logging.Verbose(1, "%s: collect %d replicas from %s",
			n.serviceName, len(resp.Infos), info.Address.String())

		n.dispatchFacts <- resp
		return nil
	})
}

func (n *NodeCollector) collectNodesInfo() {
	nodesSnapshot := n.nodes.GetNodeLivenessMap(true)

	aliveNodes := 0
	deadCount := 0
	deadNodes := ""
	for id, report := range nodesSnapshot {
		if !report.IsAlive {
			deadCount++
			if deadNodes == "" {
				deadNodes = fmt.Sprintf("%s:%d", report.Address.NodeName, report.Address.Port)
			} else {
				deadNodes = fmt.Sprintf("%s,%s:%d", deadNodes, report.Address.NodeName, report.Address.Port)
			}
			logging.Info("%s: skip collecting dead nodes %s", n.serviceName, id)
			continue
		}
		aliveNodes++
		if !n.markProcessingNode(id) {
			logging.Info(
				"%s: node %s status is still in handling, skip this round",
				n.serviceName,
				id,
			)
			continue
		}
		go n.collectNodeInfo(id, report)
	}

	third_party.PerfLog1(
		"reco",
		"reco.colossusdb.alive_nodes",
		third_party.GetKsnNameByServiceName(n.serviceName),
		n.serviceName,
		uint64(aliveNodes))
	n.alert.deadNodesAlert(deadCount, deadNodes)
}

func (n *NodeCollector) runCollecting() {
	logging.Info("%s: start collecting loop", n.serviceName)
	ticker := time.NewTicker(time.Second * time.Duration(n.collectIntervalSecs))
	for {
		select {
		case <-ticker.C:
			n.collectNodesInfo()
		case <-n.triggerCollect:
			n.collectNodesInfo()
		case <-n.quit:
			ticker.Stop()
			logging.Info("%s: stop collecting loop", n.serviceName)
			return
		}
	}
}
