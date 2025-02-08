package tables_mgr

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/grpc"
	"gotest.tools/assert"
)

func rpcGetId(port int) (*pb.GetNextIdResponse, error) {
	conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := pb.NewTablesManagerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	req := &pb.GetNextIdRequest{}
	return client.GetNextId(ctx, req)
}

func rpcAllocateId(port int) (*pb.AllocateIdResponse, error) {
	conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := pb.NewTablesManagerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	req := &pb.AllocateIdRequest{}
	return client.AllocateId(ctx, req)
}

func httpGetId(port int) (*pb.GetNextIdResponse, error) {
	url := fmt.Sprintf("http://127.0.0.1:%d/v1/get_next_id", port)

	resp := &pb.GetNextIdResponse{}
	err := utils.HttpGet(url, nil, map[string]string{}, resp, utils.DefaultCheckResp)
	return resp, err
}

func httpAllocateId(port int) (*pb.AllocateIdResponse, error) {
	url := fmt.Sprintf("http://127.0.0.1:%d/v1/allocate_id", port)

	req := &pb.AllocateIdRequest{}
	resp := &pb.AllocateIdResponse{}
	err := utils.HttpPostJson(url, nil, nil, req, resp)
	return resp, err
}

func waitUntilFollowerStart(t *testing.T, port int) {
	for i := 0; i < 20; i++ {
		getIdResp, err := rpcGetId(port)
		if err != nil {
			time.Sleep(time.Millisecond * 50)
		} else {
			assert.Equal(t, getIdResp.Status.Code, int32(pb.TablesManagerError_kNotLeader))
			break
		}
	}

	allocateResp, err := rpcAllocateId(port)
	assert.NilError(t, err)
	assert.Equal(t, allocateResp.Status.Code, int32(pb.TablesManagerError_kNotLeader))
}

func TestTablesManagerServer(t *testing.T) {
	testZkPath := utils.SpliceZkRootPath("/test/tbl_manager")
	lockPath := fmt.Sprintf("%s/%s", testZkPath, kLockZNode)
	propPath := fmt.Sprintf("%s/%s", testZkPath, kPropsZNode)
	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	zkStore := metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"},
		time.Second*10,
		acl,
		scheme,
		auth,
	)
	defer zkStore.Close()

	ans := zkStore.RecursiveDelete(context.Background(), testZkPath)
	assert.Assert(t, ans)

	ans = zkStore.RecursiveCreate(context.Background(), lockPath)
	assert.Assert(t, ans)

	distLock := zk.NewLock(zkStore.GetRawSession(), lockPath, acl)
	err := distLock.Lock()
	assert.NilError(t, err)

	logging.Info("table manager can't get the leader lock")
	server := NewServer(
		"colossusdbTableManagerTest",
		10300,
		10310,
		[]string{"127.0.0.1:2181"},
		testZkPath,
	)
	assert.Assert(t, server != nil)
	go server.Start()

	waitUntilFollowerStart(t, 10300)
	_, exists, succ := zkStore.Get(context.Background(), propPath)
	assert.Assert(t, succ)
	assert.Assert(t, !exists)

	logging.Info("first lock owner released, server will acquire leader")
	err = distLock.Unlock()
	assert.NilError(t, err)

	electLeaderOk := false
	for i := 0; i < 100; i++ {
		getIdResp, err := rpcGetId(10300)
		assert.NilError(t, err)
		if getIdResp.Status.Code == int32(pb.TablesManagerError_kNotLeader) {
			time.Sleep(time.Millisecond * 50)
			logging.Info("wait 10101 to elect leader")
		} else {
			electLeaderOk = true
			assert.Equal(t, getIdResp.Id, int32(kFirstTableId))
			break
		}
	}
	assert.Equal(t, electLeaderOk, true)

	for i := 0; i < 10; i++ {
		allocateResp, err := rpcAllocateId(10300)
		assert.NilError(t, err)
		assert.Equal(t, allocateResp.Status.Code, int32(pb.TablesManagerError_kOk))
		assert.Equal(t, allocateResp.Id, int32(i+kFirstTableId))
	}

	logging.Info("then release old server, create new server, which will load props")
	go server.Stop()

	server2 := NewServer(
		"colossusdbTableManagerTest",
		10500,
		10510,
		[]string{"127.0.0.1:2181"},
		testZkPath,
	)
	go server2.Start()

	electLeaderOk = false
	for i := 0; i < 100; i++ {
		getIdResp, err := httpGetId(10510)
		if err != nil {
			logging.Info("got error: %s", err.Error())
			time.Sleep(time.Millisecond * 100)
		} else if getIdResp.Status.Code == int32(pb.TablesManagerError_kNotLeader) {
			time.Sleep(time.Millisecond * 50)
			logging.Info("wait 20111 to elect leader")
		} else {
			electLeaderOk = true
			assert.Equal(t, getIdResp.Id, int32(kFirstTableId+10))
			break
		}
	}
	assert.Equal(t, electLeaderOk, true)

	allocatedIdChan := make(chan int32, 10)
	for i := 0; i < 10; i++ {
		go func() {
			allocateResp, err := httpAllocateId(10510)
			assert.NilError(t, err)
			assert.Equal(t, allocateResp.Status.Code, int32(pb.TablesManagerError_kOk))
			allocatedIdChan <- allocateResp.Id
		}()
	}
	allocatedIds := make([]int, 10)
	for i := 0; i < 10; i++ {
		allocatedIds[i] = int(<-allocatedIdChan)
	}
	sort.Ints(allocatedIds)
	for i := 0; i < 10; i++ {
		assert.Equal(t, allocatedIds[i], kFirstTableId+10+i)
	}
	server2.Stop()
}
