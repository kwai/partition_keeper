package metastore

import (
	"context"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

func getACLandAuthForZK() ([]zk.ACL, string, []byte) {
	acls := zk.WorldACL(zk.PermRead)
	digest := zk.DigestACL(zk.PermAll, "keeper", "admin")
	acls = append(acls, digest...)
	return acls, "digest", []byte("keeper:admin")
}

func TestWriteBatch(t *testing.T) {
	acl, scheme, auth := getACLandAuthForZK()
	zkStore := CreateZookeeperStore([]string{"127.0.0.1:2181"}, time.Second*10, acl, scheme, auth)
	defer zkStore.Close()

	logging.Info("prepare")
	testZkRoot := utils.SpliceZkRootPath("/test/metastore/write_batch")
	ans := zkStore.RecursiveDelete(context.Background(), testZkRoot)
	assert.Assert(t, ans)

	ans = zkStore.RecursiveCreate(context.Background(), testZkRoot)
	assert.Assert(t, ans)

	logging.Info("first create 2 nodes")
	createNode1 := &CreateOp{
		Path: testZkRoot + "/node1",
		Data: []byte("node1"),
	}
	createNode2 := &CreateOp{
		Path: testZkRoot + "/node2",
		Data: []byte("node2"),
	}

	ans = zkStore.WriteBatch(context.Background(), createNode1, createNode2)
	assert.Assert(t, ans)

	logging.Info("test create succeed")
	data, exists, succ := zkStore.Get(context.Background(), createNode1.Path)
	assert.Assert(t, succ && exists)
	assert.DeepEqual(t, data, createNode1.Data)

	data, exists, succ = zkStore.Get(context.Background(), createNode2.Path)
	assert.Assert(t, succ && exists)
	assert.DeepEqual(t, data, createNode2.Data)

	logging.Info("test write batch failed as can't create all")
	createNode3 := &CreateOp{
		Path: testZkRoot + "/node3",
		Data: []byte("node3"),
	}

	ans = zkStore.WriteBatch(context.Background(), createNode2, createNode3)
	assert.Equal(t, ans, false)

	_, exists, succ = zkStore.Get(context.Background(), createNode3.Path)
	assert.Assert(t, succ)
	assert.Equal(t, exists, false)

	logging.Info("test create, update & delete in a batch")
	deleteNode1 := &DeleteOp{
		Path: createNode1.Path,
	}
	setNode2 := &SetOp{
		Path: createNode2.Path,
		Data: []byte("node2 new value"),
	}

	ans = zkStore.WriteBatch(context.Background(), deleteNode1, setNode2, createNode3)
	assert.Assert(t, ans)

	_, exists, succ = zkStore.Get(context.Background(), createNode1.Path)
	assert.Assert(t, succ)
	assert.Equal(t, exists, false)

	data, exists, succ = zkStore.Get(context.Background(), createNode2.Path)
	assert.Assert(t, succ && exists)
	assert.DeepEqual(t, data, setNode2.Data)

	data, exists, succ = zkStore.Get(context.Background(), createNode3.Path)
	assert.Assert(t, succ && exists)
	assert.DeepEqual(t, data, createNode3.Data)

	logging.Info("test writebatch retry")
	ans = zkStore.WriteBatch(context.Background(), deleteNode1, setNode2, createNode3)
	assert.Equal(t, ans, false)

	_, exists, succ = zkStore.Get(context.Background(), createNode1.Path)
	assert.Assert(t, succ)
	assert.Equal(t, exists, false)

	data, exists, succ = zkStore.Get(context.Background(), createNode2.Path)
	assert.Assert(t, succ && exists)
	assert.DeepEqual(t, data, setNode2.Data)

	data, exists, succ = zkStore.Get(context.Background(), createNode3.Path)
	assert.Assert(t, succ && exists)
	assert.DeepEqual(t, data, createNode3.Data)
}
