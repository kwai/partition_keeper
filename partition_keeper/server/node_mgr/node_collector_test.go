package node_mgr

import (
	"context"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"gotest.tools/assert"
)

type testNodeCollectorEnv struct {
	zkPath           string
	zkStore          metastore.MetaStore
	localPoolBuilder *rpc.LocalPSClientPoolBuilder
	nodeStats        *NodeStats
	nc               *NodeCollector
	collected        <-chan *pb.GetReplicasResponse
	quitConsumer     chan bool
}

func setupTestNodeCollectorEnv(b *rpc.RpcMockBehaviors) *testNodeCollectorEnv {
	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	out := &testNodeCollectorEnv{
		zkPath: utils.SpliceZkRootPath("/test/nc"),
		zkStore: metastore.CreateZookeeperStore(
			[]string{"127.0.0.1:2181"},
			time.Second*10,
			acl,
			scheme,
			auth,
		),
		localPoolBuilder: rpc.NewLocalPSClientPoolBuilder().
			WithBehaviors(b).
			WithInitialTargets([]string{"127.0.0.1:1001"}),
		quitConsumer: make(chan bool),
	}

	lock := utils.NewLooseLock()
	out.zkStore.RecursiveDelete(context.Background(), out.zkPath)
	out.zkStore.RecursiveCreate(context.Background(), out.zkPath+"/nodes")
	out.zkStore.RecursiveCreate(context.Background(), out.zkPath+"/hints")

	out.nodeStats = NewNodeStats(
		"test",
		lock,
		out.zkPath+"/nodes",
		out.zkPath+"/hints",
		out.zkStore,
	)

	hubHandle := utils.MapHubs([]*pb.ReplicaHub{
		{
			Name: "yz1", Az: "YZ",
		},
	})
	out.nodeStats.LoadFromZookeeper(hubHandle, false)
	lock.LockWrite()
	out.nodeStats.UpdateStats(map[string]*NodePing{
		"node1": {true, "YZ", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
	})
	lock.UnlockWrite()

	out.nc = NewNodeCollector("test", "test", out.localPoolBuilder, WithCollectIntervalSecs(100000))
	out.collected = out.nc.Start(out.nodeStats)
	go out.consume()
	return out
}

func (te *testNodeCollectorEnv) consume() {
	for {
		select {
		case data := <-te.collected:
			te.nc.NotifyNodeCollectingHandled(data.ServerInfo.NodeId)
		case <-te.quitConsumer:
			return
		}
	}
}

func (te *testNodeCollectorEnv) teardown() {
	te.nc.Stop()
	te.quitConsumer <- true
	te.zkStore.RecursiveDelete(context.Background(), te.zkPath)
	te.zkStore.Close()
}

func TestEstimatedReplicasSentToNodes(t *testing.T) {
	te := setupTestNodeCollectorEnv(&rpc.RpcMockBehaviors{})
	te.nodeStats.GetEstReplicas().Update(int32(1), map[string]int{
		"node1": 16,
	})
	defer te.teardown()

	te.nc.TriggerCollect()
	utils.WaitCondition(t, func(log bool) bool {
		ans := te.localPoolBuilder.GetClient("127.0.0.1:1001").LastGetReplicasReq()
		if ans == nil {
			return false
		}
		assert.Equal(t, ans.FromPartitionKeeper, false)
		assert.Assert(t, ans.EstimatedReplicas == nil)
		return true
	}, 10)

	te.localPoolBuilder.GetClient("127.0.0.1:1001").Reset()
	te.nodeStats.GetNodeInfo("node1").Score = 1000

	te.nc.TriggerCollect()
	utils.WaitCondition(t, func(log bool) bool {
		ans := te.localPoolBuilder.GetClient("127.0.0.1:1001").LastGetReplicasReq()
		if ans == nil {
			return false
		}
		assert.Equal(t, ans.FromPartitionKeeper, true)
		assert.DeepEqual(t, ans.EstimatedReplicas, map[int32]int32{1: 16})
		return true
	}, 10)
}

func TestCollectNodeInfoCostLong(t *testing.T) {
	te := setupTestNodeCollectorEnv(&rpc.RpcMockBehaviors{
		Delay: time.Second * 2,
	})
	defer te.teardown()

	te.nc.TriggerCollect()
	time.Sleep(time.Millisecond * 500)
	assert.Assert(t, !te.nc.markProcessingNode("node1"))
	te.nc.TriggerCollect()

	utils.WaitCondition(t, func(log bool) bool {
		ans := te.nc.markProcessingNode("node1")
		if !ans && log {
			logging.Info("mark node1 as processing failed")
		}
		return ans
	}, 50)

	assert.Equal(t, te.localPoolBuilder.GetClient("127.0.0.1:1001").GetMetric("get_replicas"), 1)
}

func TestCollectNodeInfoFailed(t *testing.T) {
	te := setupTestNodeCollectorEnv(&rpc.RpcMockBehaviors{
		RpcFail: true,
	})
	defer te.teardown()

	te.nc.TriggerCollect()
	utils.WaitCondition(t, func(log bool) bool {
		ans := te.localPoolBuilder.GetClient("127.0.0.1:1001").GetMetric("get_replicas")
		return ans > 0
	}, 10)

	assert.Equal(t, te.nc.markProcessingNode("node"), true)
}
