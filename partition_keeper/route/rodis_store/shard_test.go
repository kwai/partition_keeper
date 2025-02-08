package rodis_store

import (
	"context"
	"testing"

	"github.com/go-zookeeper/zk"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"gotest.tools/assert"
)

type testShardEnv struct {
	store      *metastore.ZookeeperStore
	parentPath string
	partId     int
}

func setupTestShardEnv(t *testing.T) *testShardEnv {
	output := &testShardEnv{
		store: metastore.GetGlobalPool().
			Get([]string{"127.0.0.1:2181"}, zk.WorldACL(zk.PermAll), "digest", []byte("keeper:admin")),
		parentPath: utils.SpliceZkRootPath("/test/pk/rodis_store/shards"),
		partId:     0,
	}

	assert.Assert(t, output.store.RecursiveDelete(context.Background(), output.parentPath))
	assert.Assert(t, output.store.RecursiveCreate(context.Background(), output.parentPath))
	return output
}

func (te *testShardEnv) teardown() {
	te.store.RecursiveDelete(context.Background(), te.parentPath)
}

func (te *testShardEnv) testZkSynced(t *testing.T, shard *rodisShardMeta) {
	zkShard := newRodisShardMeta(te.parentPath, te.partId)
	zkShard.loadFromZk(te.store)

	assert.Equal(t, zkShard.version, shard.version)
	assert.Equal(t, zkShard.master, shard.master)
	assert.Equal(t, zkShard.masterSeqNum, shard.masterSeqNum)
	assert.DeepEqual(t, zkShard.replicas, shard.replicas)
}

func TestShardReadOldRodis(t *testing.T) {
	te := setupTestShardEnv(t)
	defer te.teardown()

	shard := newRodisShardMeta(te.parentPath, te.partId)

	routeEntry := &pb.RouteEntries{
		TableInfo: &pb.TableInfo{
			HashMethod: "crc32",
		},
		ReplicaHubs: []*pb.ReplicaHub{
			{Name: "gz1_1", Az: "GZ1"},
			{Name: "yz1", Az: "YZ"},
			{Name: "zw1", Az: "ZW"},
		},
		Servers: []*pb.ServerLocation{
			{Host: "127.0.0.1", Port: 1001, HubId: 0, Alive: true, Op: pb.AdminNodeOp_kNoop},
			{Host: "127.0.0.2", Port: 1001, HubId: 1, Alive: true, Op: pb.AdminNodeOp_kNoop},
			{Host: "127.0.0.3", Port: 1001, HubId: 2, Alive: true, Op: pb.AdminNodeOp_kNoop},
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

	logging.Info("add new shard")
	shard.update(te.store, routeEntry, 0)

	logging.Info("emulate rodis that don't have a version")
	te.store.Set(context.Background(), shard.zkPath, []byte{})

	shard.version = 1
	te.testZkSynced(t, shard)
}

func TestShardAddDeleteUpdate(t *testing.T) {
	te := setupTestShardEnv(t)
	defer te.teardown()

	shard := newRodisShardMeta(te.parentPath, te.partId)

	routeEntry := &pb.RouteEntries{
		TableInfo: &pb.TableInfo{
			HashMethod: "crc32",
		},
		ReplicaHubs: []*pb.ReplicaHub{
			{Name: "gz1_1", Az: "GZ1"},
			{Name: "yz1", Az: "YZ"},
			{Name: "zw1", Az: "ZW"},
		},
		Servers: []*pb.ServerLocation{
			{Host: "127.0.0.1", Port: 1001, HubId: 0, Alive: true, Op: pb.AdminNodeOp_kNoop},
			{Host: "127.0.0.2", Port: 1001, HubId: 1, Alive: true, Op: pb.AdminNodeOp_kNoop},
			{Host: "127.0.0.3", Port: 1001, HubId: 2, Alive: true, Op: pb.AdminNodeOp_kNoop},
		},
		Partitions: []*pb.PartitionLocation{
			{
				Replicas: []*pb.ReplicaLocation{
					{Role: pb.ReplicaRole_kSecondary, ServerIndex: 0},
					{Role: pb.ReplicaRole_kSecondary, ServerIndex: 1},
					{Role: pb.ReplicaRole_kLearner, ServerIndex: 2},
				},
				Version: 10,
			},
		},
	}

	logging.Info("add new shard")
	shard.update(te.store, routeEntry, 0)
	assert.Equal(t, shard.hasZkEntry, true)
	assert.Equal(t, shard.version, int64(10))
	assert.Equal(t, shard.master, "")
	assert.Equal(t, shard.masterSeqNum, int64(0))
	assert.Equal(t, len(shard.replicas), 3)
	assert.DeepEqual(t, shard.replicas, map[string]rodisReplicaStatus{
		routeEntry.Servers[0].ToHostPort(): rodisReplicaServing,
		routeEntry.Servers[1].ToHostPort(): rodisReplicaServing,
		routeEntry.Servers[2].ToHostPort(): rodisReplicaPreparing,
	})

	te.testZkSynced(t, shard)

	logging.Info("update shard and add primary")
	routeEntry.Partitions[0].Replicas[0].Role = pb.ReplicaRole_kPrimary
	routeEntry.Partitions[0].Replicas[0].Info = map[string]string{"seq": "12345"}
	routeEntry.Partitions[0].Version = 11

	shard.update(te.store, routeEntry, 0)
	assert.Equal(t, shard.version, int64(11))
	assert.Equal(t, shard.master, routeEntry.Servers[0].ToHostPort())
	assert.Equal(t, shard.masterSeqNum, int64(12345))
	assert.Equal(t, len(shard.replicas), 3)
	assert.DeepEqual(t, shard.replicas, map[string]rodisReplicaStatus{
		routeEntry.Servers[0].ToHostPort(): rodisReplicaServing,
		routeEntry.Servers[1].ToHostPort(): rodisReplicaServing,
		routeEntry.Servers[2].ToHostPort(): rodisReplicaPreparing,
	})
	te.testZkSynced(t, shard)

	logging.Info("version same will only update sequence number")
	routeEntry.Partitions[0].Replicas = routeEntry.Partitions[0].Replicas[0:1]
	routeEntry.Partitions[0].Replicas[0].Info = map[string]string{"seq": "22345"}
	routeEntry.Partitions[0].Version = 11

	shard.update(te.store, routeEntry, 0)
	assert.Equal(t, shard.version, int64(11))
	assert.Equal(t, shard.master, routeEntry.Servers[0].ToHostPort())
	assert.Equal(t, shard.masterSeqNum, int64(22345))
	assert.Equal(t, len(shard.replicas), 3)
	assert.DeepEqual(t, shard.replicas, map[string]rodisReplicaStatus{
		routeEntry.Servers[0].ToHostPort(): rodisReplicaServing,
		routeEntry.Servers[1].ToHostPort(): rodisReplicaServing,
		routeEntry.Servers[2].ToHostPort(): rodisReplicaPreparing,
	})
	te.testZkSynced(t, shard)

	logging.Info("update shard")
	routeEntry.Partitions[0].Replicas = routeEntry.Partitions[0].Replicas[0:1]
	routeEntry.Partitions[0].Version = 12
	routeEntry.Partitions[0].Replicas[0].Role = pb.ReplicaRole_kLearner

	shard.update(te.store, routeEntry, 0)
	assert.Equal(t, shard.version, int64(12))
	assert.Equal(t, shard.master, "")
	assert.Equal(t, shard.masterSeqNum, int64(22345))
	assert.Equal(t, len(shard.replicas), 1)
	assert.DeepEqual(t, shard.replicas, map[string]rodisReplicaStatus{
		routeEntry.Servers[0].ToHostPort(): rodisReplicaPreparing,
	})

	te.testZkSynced(t, shard)

	logging.Info("version smaller won't be updated")
	routeEntry.Partitions[0].Version = 11
	routeEntry.Partitions[0].Replicas[0].Role = pb.ReplicaRole_kPrimary

	shard.update(te.store, routeEntry, 0)
	assert.Equal(t, shard.version, int64(12))
	assert.Equal(t, shard.master, "")
	assert.Equal(t, shard.masterSeqNum, int64(22345))
	assert.Equal(t, len(shard.replicas), 1)
	assert.DeepEqual(t, shard.replicas, map[string]rodisReplicaStatus{
		routeEntry.Servers[0].ToHostPort(): rodisReplicaPreparing,
	})
}
