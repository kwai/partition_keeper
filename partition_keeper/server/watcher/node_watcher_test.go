package watcher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

type nodeWatcherTestEnv struct {
	zkStore   metastore.MetaStore
	zkPrefix  string
	nodeStats *node_mgr.NodeStats
	lock      *utils.LooseLock
}

func setupNodeWatcherTestEnv(
	t *testing.T,
	hubs []*pb.ReplicaHub,
	perHubNodes []int,
) *nodeWatcherTestEnv {
	output := &nodeWatcherTestEnv{}

	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	output.zkPrefix = utils.SpliceZkRootPath("/test/pk/watcher")

	output.zkStore = metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"}, time.Second*10, acl, scheme, auth,
	)

	assert.Assert(t, output.zkStore.RecursiveDelete(context.Background(), output.zkPrefix))
	assert.Assert(t, output.zkStore.RecursiveCreate(context.Background(), output.zkPrefix+"/nodes"))
	assert.Assert(t, output.zkStore.RecursiveCreate(context.Background(), output.zkPrefix+"/hints"))

	lock := utils.NewLooseLock()
	nodes := node_mgr.NewNodeStats(
		"test",
		lock,
		output.zkPrefix+"/nodes",
		output.zkPrefix+"/hints",
		output.zkStore,
	).WithSkipHintCheck(true)
	nodes.LoadFromZookeeper(utils.MapHubs(hubs), false)

	hints := map[string]*pb.NodeHints{}
	pings := map[string]*node_mgr.NodePing{}

	for k, hub := range hubs {
		for i := 0; i < perHubNodes[k]; i++ {
			rpcNode := &utils.RpcNode{
				NodeName: fmt.Sprintf("127.0.0.%d", k),
				Port:     int32(i),
			}
			hints[rpcNode.String()] = &pb.NodeHints{Hub: hub.Name}
			pings[fmt.Sprintf("node_%s_%d", hub.Name, i)] = &node_mgr.NodePing{
				IsAlive:   true,
				Az:        hub.Az,
				Address:   rpcNode,
				ProcessId: "1",
				BizPort:   int32(i + 1000),
			}
		}
	}

	lock.LockWrite()
	defer lock.UnlockWrite()

	nodes.AddHints(hints, true)
	nodes.UpdateStats(pings)
	output.nodeStats = nodes
	output.lock = lock

	return output
}

func (n *nodeWatcherTestEnv) teardown() {
	n.zkStore.Close()
}
