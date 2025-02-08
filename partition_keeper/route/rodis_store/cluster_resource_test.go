package rodis_store

import (
	"context"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"gotest.tools/assert"
)

func TestClusterResourceKeyWork(t *testing.T) {
	prefix1 := utils.SpliceZkRootPath("/ks2/rodis/test_cluster1")
	prefix2 := utils.SpliceZkRootPath("/ks2/rodis/test_cluster2")

	hosts1 := []string{"127.0.0.1:1001", "127.0.0.1:1002"}
	hosts2 := []string{"127.0.0.1:1002", "127.0.0.1:1001"}

	cluster1 := pool.getClusterResource(prefix1, hosts1)
	cluster2 := pool.getClusterResource(prefix1, hosts2)
	cluster3 := pool.getClusterResource(prefix2, hosts1)

	assert.Equal(t, cluster1, cluster2)
	assert.Assert(t, cluster1 != cluster3)
}

func TestClusterResourceUpdateSynced(t *testing.T) {
	zk := metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"},
		time.Second*10,
		zk.WorldACL(zk.PermAll),
		"digest",
		[]byte("keeper:admin"),
	)
	zk.RecursiveDelete(context.Background(), utils.SpliceZkRootPath("/test/pk/rodis"))
	zk.RecursiveCreate(context.Background(), utils.SpliceZkRootPath("/test/pk/rodis"))
	zk.Close()

	route := &pb.RouteEntries{
		TableInfo: &pb.TableInfo{
			HashMethod: "crc32",
		},
		ReplicaHubs: []*pb.ReplicaHub{
			{Name: "gz1_1", Az: "ZW"},
			{Name: "yz1", Az: "ZW"},
			{Name: "zw1", Az: "ZW"},
		},
		Servers: []*pb.ServerLocation{
			{
				Host:  "dev-huyifan03-02.dev.kwaidc.com",
				Port:  1001,
				HubId: 0,
				Alive: true,
				Op:    pb.AdminNodeOp_kNoop,
			},
			{
				Host:  "dev-huyifan03-02.dev.kwaidc.com",
				Port:  1002,
				HubId: 1,
				Alive: true,
				Op:    pb.AdminNodeOp_kNoop,
			},
			{
				Host:  "dev-huyifan03-02.dev.kwaidc.com",
				Port:  1003,
				HubId: 2,
				Alive: true,
				Op:    pb.AdminNodeOp_kNoop,
			},
		},
		Partitions: []*pb.PartitionLocation{
			{
				Replicas: []*pb.ReplicaLocation{
					{Role: pb.ReplicaRole_kPrimary, ServerIndex: 0},
					{Role: pb.ReplicaRole_kSecondary, ServerIndex: 1},
					{Role: pb.ReplicaRole_kLearner, ServerIndex: 2},
				},
				Version: 10,
			},
		},
	}

	clusterRes := newClusterResource(
		utils.SpliceZkRootPath("/test/pk/rodis/test_cluster"),
		[]string{"127.0.0.1:2181"},
	)
	clusterRes.initialize()
	clusterRes.update(route)

	clusterRes2 := newClusterResource(
		utils.SpliceZkRootPath("/test/pk/rodis/test_cluster"),
		[]string{"127.0.0.1:2181"},
	)
	clusterRes2.initialize()

	assert.Equal(t, clusterRes.frame.zkPath, clusterRes2.frame.zkPath)
	assert.DeepEqual(t, clusterRes.frame.frame2Az, clusterRes2.frame.frame2Az)
	//assert.DeepEqual(t, clusterRes.frame.az2Idc, clusterRes2.frame.az2Idc)

	assert.Equal(t, len(clusterRes.nodes), len(clusterRes2.nodes))
	for node, meta := range clusterRes.nodes {
		n2 := clusterRes2.nodes[node]
		assert.Assert(t, n2 != nil)
		assert.Equal(t, meta.hasZkEntry, n2.hasZkEntry)
		assert.Equal(t, meta.zkPath, n2.zkPath)
		assert.Equal(t, meta.status, n2.status)
		assert.Equal(t, meta.frame, n2.frame)
		assert.Equal(t, meta.idc, n2.idc)
	}
}
