package rodis_store

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"gotest.tools/assert"
)

func checkNodeZkSynced(t *testing.T, store *metastore.ZookeeperStore, node *rodisNodeMeta) {
	parent := path.Dir(node.zkPath)
	name := path.Base(node.zkPath)

	newNode := newRodisNodeMeta(parent, name)
	newNode.loadFromZk(store)
	assert.Assert(t, newNode.hasZkEntry)

	assert.Equal(t, node.frame, newNode.frame)
	assert.Equal(t, node.idc, newNode.idc)
	assert.Equal(t, node.status, newNode.status)
}

func TestUpdateNode(t *testing.T) {
	store := metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"},
		time.Second*10,
		zk.WorldACL(zk.PermAll),
		"digest",
		[]byte("keeper:admin"),
	)
	defer store.Close()

	parentPath := utils.SpliceZkRootPath("/test/pk/rodis/node_test/nodes")
	succ := store.RecursiveDelete(context.Background(), parentPath)
	assert.Assert(t, succ)
	succ = store.RecursiveCreate(context.Background(), parentPath)
	assert.Assert(t, succ)

	node := newRodisNodeMeta(parentPath, "127.0.0.1:1001")

	server := &pb.ServerLocation{
		Host:  "127.0.0.1",
		Port:  1001,
		HubId: 0,
		Alive: true,
		Op:    pb.AdminNodeOp_kNoop,
	}
	hub := &pb.ReplicaHub{Name: "yz", Az: "YZ"}
	node.update(store, server, hub, "bj")
	assert.Equal(t, node.status, rodisNodeEnable)
	assert.Equal(t, node.frame, "yz")
	assert.Equal(t, node.idc, "bj")
	checkNodeZkSynced(t, store, node)

	server.Op = pb.AdminNodeOp_kRestart
	server.Alive = false
	hub.Name = "zw"
	node.update(store, server, hub, "bj")
	assert.Equal(t, node.status, rodisNodeTempDown)
	assert.Equal(t, node.frame, "zw")
	assert.Equal(t, node.idc, "bj")
	checkNodeZkSynced(t, store, node)

	server.Op = pb.AdminNodeOp_kOffline
	server.Alive = false
	hub.Name = "gz1"
	node.update(store, server, hub, "HN1")
	assert.Equal(t, node.status, rodisNodePreDisable)
	assert.Equal(t, node.frame, "gz1")
	assert.Equal(t, node.idc, "HN1")
	checkNodeZkSynced(t, store, node)

	server.Op = pb.AdminNodeOp_kNoop
	server.Alive = false
	node.update(store, server, hub, "HN1")
	assert.Equal(t, node.status, rodisNodeError)
	assert.Equal(t, node.frame, "gz1")
	assert.Equal(t, node.idc, "HN1")
	checkNodeZkSynced(t, store, node)

	node.remove(store)
	_, exists, succ := store.Get(context.Background(), node.zkPath)
	assert.Assert(t, succ)
	assert.Assert(t, !exists)
}

func TestTranslateNodeStatusToServerOp(t *testing.T) {
	server := &pb.ServerLocation{
		Host:  "127.0.0.1",
		Port:  1001,
		HubId: 0,
		Alive: false,
		Op:    pb.AdminNodeOp_kOffline,
	}

	rodisNodeEnable.translateTo(server)
	assert.Equal(t, server.Alive, true)
	assert.Equal(t, server.Op, pb.AdminNodeOp_kNoop)

	server.Alive = true
	server.Op = pb.AdminNodeOp_kOffline
	rodisNodeError.translateTo(server)
	assert.Equal(t, server.Alive, false)
	assert.Equal(t, server.Op, pb.AdminNodeOp_kNoop)

	server.Alive = false
	server.Op = pb.AdminNodeOp_kNoop
	rodisNodeTempDown.translateTo(server)
	assert.Equal(t, server.Alive, true)
	assert.Equal(t, server.Op, pb.AdminNodeOp_kRestart)

	server.Alive = false
	server.Op = pb.AdminNodeOp_kNoop
	rodisNodePreDisable.translateTo(server)
	assert.Equal(t, server.Alive, true)
	assert.Equal(t, server.Op, pb.AdminNodeOp_kOffline)

	server.Alive = false
	server.Op = pb.AdminNodeOp_kNoop
	rodisNodeDisable.translateTo(server)
	assert.Equal(t, server.Alive, true)
	assert.Equal(t, server.Op, pb.AdminNodeOp_kOffline)
}
