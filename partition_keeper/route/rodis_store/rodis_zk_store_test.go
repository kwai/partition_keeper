package rodis_store

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
)

func sortEntryItem(route *pb.RouteEntries) {
	sort.Slice(route.ReplicaHubs, func(i, j int) bool {
		return route.ReplicaHubs[i].Name < route.ReplicaHubs[j].Name
	})
	sort.Slice(route.Servers, func(i, j int) bool {
		l, r := route.Servers[i], route.Servers[j]
		if l.Host != r.Host {
			return l.Host < r.Host
		}
		return l.Port < r.Port
	})
	for _, part := range route.Partitions {
		sort.Slice(part.Replicas, func(i, j int) bool {
			l, r := part.Replicas[i], part.Replicas[j]
			return l.ServerIndex < r.ServerIndex
		})
	}
}

func checkEntriesEqual(t *testing.T, route1, route2 *pb.RouteEntries) {
	sortEntryItem(route1)
	sortEntryItem(route2)

	assert.Assert(t, proto.Equal(route1, route2))
}

func cleanZookeeperDir() {
	zk := metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"},
		time.Second*10,
		zk.WorldACL(zk.PermAll),
		"",
		[]byte(""),
	)
	zk.RecursiveDelete(context.Background(), utils.SpliceZkRootPath("/test/pk/rodis"))
	zk.Close()
}

func TestRodisZkStore(t *testing.T) {
	cleanZookeeperDir()
	hn, _ := os.Hostname()
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
				Host:  hn,
				Port:  1001,
				HubId: 0,
				Alive: true,
				Op:    pb.AdminNodeOp_kNoop,
			},
			{
				Host:  hn,
				Port:  1002,
				HubId: 1,
				Alive: true,
				Op:    pb.AdminNodeOp_kNoop,
			},
			{
				Host:  hn,
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

	args := &base.StoreBaseOption{
		Url:     "127.0.0.1:2181",
		Args:    utils.SpliceZkRootPath("/test/pk/rodis/test_cluster"),
		Service: "test_cluster",
		Table:   "test_table",
	}
	zkStore := NewRodisZkStore(args)
	logging.Info("put to empty domain")
	err := zkStore.Put(route)
	assert.Assert(t, err == nil)

	current, _ := zkStore.dump()
	checkEntriesEqual(t, route, current)

	route.ReplicaHubs = append(route.ReplicaHubs, &pb.ReplicaHub{Name: "zw2", Az: "ZW"})
	route.Servers = append(route.Servers, &pb.ServerLocation{
		Host: hn, Port: 1004, HubId: 1, Alive: true, Op: pb.AdminNodeOp_kNoop,
	})
	route.Partitions[0].Replicas[0].Role = pb.ReplicaRole_kLearner
	route.Partitions[0].Replicas[2].Role = pb.ReplicaRole_kSecondary
	route.Partitions[0].Version += 2

	logging.Info("put to non empty domain")
	err = zkStore.Put(route)
	assert.Assert(t, err == nil)

	current, _ = zkStore.dump()
	checkEntriesEqual(t, route, current)

	logging.Info("load non empty domain")

	zkStore2 := NewRodisZkStore(args)
	current2, _ := zkStore2.Get()
	checkEntriesEqual(t, current, current2)

	logging.Info("put to loaded domain")
	route.ReplicaHubs = route.ReplicaHubs[0 : len(route.ReplicaHubs)-1]
	route.Servers = route.Servers[0 : len(route.Servers)-1]
	route.Partitions[0].Replicas[1].Role = pb.ReplicaRole_kPrimary
	route.Partitions[0].Version++

	err = zkStore2.Put(route)
	assert.Assert(t, err == nil)
	current2, _ = zkStore2.dump()
	checkEntriesEqual(t, route, current2)

	logging.Info("delete domain")
	domainsDir := fmt.Sprintf("%s/%s", zkStore2.cluster.domainsDir, zkStore2.domainName)
	_, exists, succ := zkStore2.cluster.store.Get(context.Background(), domainsDir)
	assert.Equal(t, succ && exists, true)
	_, exists, succ = zkStore2.cluster.store.Get(context.Background(), zkStore2.shardsDir)
	assert.Equal(t, succ && exists, true)

	err = zkStore2.Del()
	assert.Assert(t, err == nil)
	_, exists, succ = zkStore2.cluster.store.Get(context.Background(), domainsDir)
	assert.Equal(t, succ && !exists, true)
	_, exists, succ = zkStore2.cluster.store.Get(context.Background(), zkStore2.shardsDir)
	assert.Equal(t, succ && !exists, true)

	logging.Info("delete empty domain")
	args = &base.StoreBaseOption{
		Url:     "127.0.0.1:2181",
		Args:    utils.SpliceZkRootPath("/test/pk/rodis/test_del_cluster"),
		Service: "test_del_cluster",
		Table:   "test_del_table",
	}

	zkStore_del := NewRodisZkStore(args)
	err = zkStore_del.Del()
	assert.Assert(t, err == nil)
	cleanZookeeperDir()
}
