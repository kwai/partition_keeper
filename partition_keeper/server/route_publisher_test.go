package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/delay_execute"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route"
	route_base "github.com/kuaishou/open_partition_keeper/partition_keeper/route/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	_ "github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/checkpoint"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	strategy_base "github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"gotest.tools/assert"
	"gotest.tools/assert/cmp"
)

var (
	errorIll = errors.New("mocked ill")
)

type mockRouteKessStore struct {
	mu        sync.Mutex
	putValues []*pb.RouteEntries
	ill       bool
}

func (m *mockRouteKessStore) MakeIll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ill = true
}

func (m *mockRouteKessStore) UnMakeIll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ill = false
}

func (m *mockRouteKessStore) Put(value *pb.RouteEntries) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ill {
		return errorIll
	}
	m.putValues = append(m.putValues, value)
	return nil
}

func (m *mockRouteKessStore) Del() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ill {
		return errorIll
	}

	l := len(m.putValues)
	if l == 0 {
		return nil
	}
	m.putValues = m.putValues[0 : l-1]
	return nil
}

func (m *mockRouteKessStore) Get() (*pb.RouteEntries, error) {
	return nil, errors.New("not implemented")
}

func init() {
	route_base.RegisterStoreCreator(
		"mock",
		func(args *route_base.StoreBaseOption) route_base.StoreBase {
			logging.Info("create store mock")
			return &mockRouteKessStore{}
		},
	)
}

type mockedServiceStrategy struct {
	strategy_base.DummyStrategy
}

func (m *mockedServiceStrategy) MakeStoreOpts(
	region map[string]bool,
	svc, tbl, args string,
) ([]*route.StoreOption, error) {
	return nil, nil
}

func (m *mockedServiceStrategy) CreateCheckpointByDefault() bool {
	return false
}

type publisherTestEnv struct {
	zkPath  string
	zkStore metastore.MetaStore
	lp      *rpc.LocalPSClientPoolBuilder
	hubs    []*pb.ReplicaHub
	table   *TableStats
}

func setupPublishTestEnv(t *testing.T, usePaz bool) *publisherTestEnv {
	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	out := &publisherTestEnv{}
	out.zkPath = utils.SpliceZkRootPath("/test/pk/test_publish")
	out.zkStore = metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"},
		time.Second*10,
		acl,
		scheme,
		auth,
	)

	succ := out.zkStore.RecursiveDelete(context.Background(), out.zkPath)
	assert.Assert(t, succ)

	out.lp = rpc.NewLocalPSClientPoolBuilder()

	lock := utils.NewLooseLock()
	out.hubs = []*pb.ReplicaHub{
		{Name: "yz1", Az: "yz1"},
		{Name: "yz2", Az: "yz2"},
		{Name: "yz3", Az: "yz3"},
	}
	if usePaz {
		out.hubs = []*pb.ReplicaHub{
			{Name: "az1", Az: "HB1AZ1"},
			{Name: "az2", Az: "HB1AZ2"},
			{Name: "az3", Az: "HB1AZ3"},
		}
	}

	succ = out.zkStore.RecursiveCreate(context.Background(), out.zkPath+"/nodes")
	assert.Assert(t, succ)
	succ = out.zkStore.RecursiveCreate(context.Background(), out.zkPath+"/hints")
	assert.Assert(t, succ)
	nodes := node_mgr.NewNodeStats(
		"test",
		lock,
		out.zkPath+"/nodes",
		out.zkPath+"/hints",
		out.zkStore,
	)
	nodes.LoadFromZookeeper(utils.MapHubs(out.hubs), false)

	nodePings := map[string]*node_mgr.NodePing{
		"node1": {
			IsAlive:   true,
			Az:        "yz1",
			Address:   utils.FromHostPort("127.0.0.1:1001"),
			ProcessId: "12341",
			BizPort:   2001,
		},
		"node2": {
			IsAlive:   true,
			Az:        "yz2",
			Address:   utils.FromHostPort("127.0.0.2:1001"),
			ProcessId: "12342",
			BizPort:   2001,
		},
		"node3": {
			IsAlive:   true,
			Az:        "yz3",
			Address:   utils.FromHostPort("127.0.0.3:1001"),
			ProcessId: "12343",
			BizPort:   2001,
		},
	}
	if usePaz {
		nodePings = map[string]*node_mgr.NodePing{
			"node1": {
				IsAlive:   true,
				Az:        "HB1AZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
			"node2": {
				IsAlive:   true,
				Az:        "HB1AZ2",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "12342",
				BizPort:   2001,
			},
			"node3": {
				IsAlive:   true,
				Az:        "HB1AZ3",
				Address:   utils.FromHostPort("127.0.0.3:1001"),
				ProcessId: "12343",
				BizPort:   2001,
			},
		}
	}
	lock.LockWrite()
	nodes.UpdateStats(nodePings)
	lock.UnlockWrite()

	succ = out.zkStore.RecursiveCreate(context.Background(), out.zkPath+"/tables")
	assert.Assert(t, succ)
	delayedExecutorManager := delay_execute.NewDelayedExecutorManager(
		out.zkStore,
		out.zkPath+"/"+kDelayedExecutorNode,
	)

	delayedExecutorManager.InitFromZookeeper()

	out.table = NewTableStats(
		"test",
		"test",
		usePaz,
		lock,
		nodes,
		out.hubs,
		&mockedServiceStrategy{},
		out.lp.Build(),
		out.zkStore,
		delayedExecutorManager,
		out.zkPath+"/tables",
		nil,
	)

	tableDescription := &pb.Table{
		TableId:    1,
		TableName:  "test",
		HashMethod: "crc32",
		PartsCount: 8,
		JsonArgs:   `{"a": "b"}`,
		KconfPath:  "reco.rodisFea.partitionKeeperHDFSTest",
	}

	out.table.InitializeNew(tableDescription, "")
	return out
}

func (te *publisherTestEnv) teardown() {
	te.table.Stop(true)
	te.table.delayedExecutorManager.Stop()
	te.zkStore.RecursiveDelete(context.Background(), te.zkPath)
	te.zkStore.Close()
}

func (te *publisherTestEnv) updateFactsFromMembers() {
	for i := int32(0); i < te.table.PartsCount; i++ {
		part := &(te.table.currParts[i])
		for node, role := range part.members.Peers {
			part.facts[node] = &replicaFact{
				membershipVersion: part.members.MembershipVersion,
				partSplitVersion:  part.members.SplitVersion,
				PartitionReplica: &pb.PartitionReplica{
					Role:                role,
					ReadyToPromote:      true,
					Node:                te.table.nodes.MustGetNodeInfo(node).Address.ToPb(),
					RestoreVersion:      part.members.RestoreVersion[node],
					StatisticsInfo:      map[string]string{"a": "b"},
					SplitCleanupVersion: part.members.SplitVersion,
				},
			}
		}
	}
}

func (te *publisherTestEnv) prepareNormalTable() {
	plans := map[int32]*actions.PartitionActions{}

	for i := int32(0); i < te.table.PartsCount; i++ {
		pa := &actions.PartitionActions{}
		pa.AddAction(&actions.AddLearnerAction{Node: "node1"})
		pa.AddAction(&actions.TransformAction{Node: "node1", ToRole: pb.ReplicaRole_kSecondary})
		pa.AddAction(&actions.TransformAction{Node: "node1", ToRole: pb.ReplicaRole_kPrimary})

		pa.AddAction(&actions.AddLearnerAction{Node: "node2"})
		pa.AddAction(&actions.TransformAction{Node: "node2", ToRole: pb.ReplicaRole_kSecondary})

		pa.AddAction(&actions.AddLearnerAction{Node: "node3"})
		pa.AddAction(&actions.TransformAction{Node: "node3", ToRole: pb.ReplicaRole_kSecondary})

		plans[i] = pa
	}

	te.table.serviceLock.LockWrite()
	defer te.table.serviceLock.UnlockWrite()
	te.table.ApplyPlan(plans)
	time.Sleep(time.Millisecond * 50)

	for ans := te.table.scheduleAsPlan(); len(ans) > 0; ans = te.table.scheduleAsPlan() {
		time.Sleep(time.Millisecond * 50)
	}

	// prepare node statistics info
	te.updateFactsFromMembers()
}

func (te *publisherTestEnv) getNodeInfoByBizPort(
	nodeName string,
	bizPort int32,
) *node_mgr.NodeInfo {
	for _, info := range te.table.nodes.AllNodes() {
		if info.Address.NodeName == nodeName && info.BizPort == bizPort {
			return info
		}
	}
	return nil
}

func TestStopNotClean(t *testing.T) {
	usePazs := []bool{false, true}
	for _, usePaz := range usePazs {
		te := setupPublishTestEnv(t, usePaz)
		te.prepareNormalTable()
		defer te.teardown()

		storeOpts := []*route.StoreOption{
			{
				Media: "mock",
			},
		}
		publisher := NewRoutePublisher(
			te.table,
			storeOpts,
			WithMaxIntervalSecs(1000000),
			WithMinIntervalSecs(1),
		)
		publisher.Start()
		kessStore := publisher.routeStore.GetStore(storeOpts[0]).(*mockRouteKessStore)

		publisher.NotifyChange()
		time.Sleep(time.Millisecond * 100)
		publisher.Stop(false)

		kessStore.mu.Lock()
		defer kessStore.mu.Unlock()
		assert.Assert(t, cmp.Len(kessStore.putValues, 1))
	}

}

func TestPublishRouteAndStopClean(t *testing.T) {
	usePazs := []bool{false, true}
	for _, usePaz := range usePazs {
		te := setupPublishTestEnv(t, usePaz)
		te.prepareNormalTable()
		defer te.teardown()

		storeOpts := []*route.StoreOption{
			{
				Media: "mock",
			},
		}
		publisher := NewRoutePublisher(
			te.table,
			storeOpts,
			WithMaxIntervalSecs(1000000),
			WithMinIntervalSecs(1),
		)
		publisher.Start()
		kessStore := publisher.routeStore.GetStore(storeOpts[0]).(*mockRouteKessStore)

		publisher.NotifyChange()
		time.Sleep(time.Millisecond * 100)
		publisher.NotifyChange()
		time.Sleep(time.Second)
		publisher.NotifyChange()
		publisher.NotifyChange()

		assert.Assert(t, cmp.Len(kessStore.putValues, 2))

		routeEntries := kessStore.putValues[0]

		assert.Equal(t, routeEntries.TableInfo.HashMethod, "crc32")
		assert.Equal(t, len(routeEntries.ReplicaHubs), len(te.hubs))
		for i, hub := range te.hubs {
			assert.Equal(t, hub.Name, routeEntries.ReplicaHubs[i].Name)
			assert.Equal(t, hub.Az, routeEntries.ReplicaHubs[i].Az)
		}

		assert.Equal(t, len(routeEntries.Servers), len(te.table.nodes.AllNodes()))
		for _, serverLoc := range routeEntries.Servers {
			assert.Equal(t, serverLoc.Host, serverLoc.Ip)
			nodeInfo := te.getNodeInfoByBizPort(serverLoc.Host, serverLoc.Port)
			assert.Assert(t, nodeInfo != nil)
			assert.Equal(t, routeEntries.ReplicaHubs[serverLoc.HubId].Name, nodeInfo.Hub)
		}

		assert.Equal(t, len(routeEntries.Partitions), len(te.table.currParts))
		for i, part := range routeEntries.Partitions {
			tableMembership := &(te.table.currParts[i].members)
			assert.Equal(t, part.Version, tableMembership.MembershipVersion)
			assert.Equal(t, len(part.Replicas), len(tableMembership.Peers))

			for _, rep := range part.Replicas {
				assert.DeepEqual(t, rep.Info, map[string]string{"a": "b"})
				server := routeEntries.Servers[rep.ServerIndex]
				nodeInfo := te.getNodeInfoByBizPort(server.Host, server.Port)
				assert.Assert(t, nodeInfo != nil)
				role, ok := tableMembership.Peers[nodeInfo.Id]
				assert.Equal(t, ok, true)
				assert.Equal(t, role, rep.Role)
			}
		}

		publisher.Stop(true)
		assert.Assert(t, cmp.Len(kessStore.putValues, 1))
	}
}

func TestRoutePublishedWhenSplit(t *testing.T) {
	usePazs := []bool{false, true}
	for _, usePaz := range usePazs {
		te := setupPublishTestEnv(t, usePaz)
		te.prepareNormalTable()
		defer te.teardown()

		storeOpts := []*route.StoreOption{
			{
				Media: "mock",
			},
		}
		publisher := NewRoutePublisher(
			te.table,
			storeOpts,
			WithMaxIntervalSecs(1000000),
			WithMinIntervalSecs(1),
		)
		publisher.Start()
		kessStore := publisher.routeStore.GetStore(storeOpts[0]).(*mockRouteKessStore)

		logging.Info("first table split version is 0, and 0 is published")
		publisher.NotifyChange()
		time.Sleep(time.Second)
		assert.Equal(t, len(kessStore.putValues), 1)
		assert.Equal(t, kessStore.putValues[0].TableInfo.SplitVersion, int32(0))

		logging.Info("then trigger table to split, and make partition 0 to split finish")
		ans := te.table.StartSplit(&pb.SplitTableRequest{
			NewSplitVersion: 1,
			Options: &pb.SplitTableOptions{
				MaxConcurrentParts: 1,
				DelaySeconds:       1,
			},
		})
		assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

		for {
			te.table.serviceLock.LockWrite()
			te.table.Schedule(
				true,
				&pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 1000},
			)
			te.updateFactsFromMembers()
			te.table.serviceLock.UnlockWrite()
			if te.table.currParts[0].members.SplitVersion == 1 {
				break
			}
		}

		publisher.NotifyChange()
		time.Sleep(time.Millisecond * 100)
		assert.Equal(t, len(kessStore.putValues), 2)
		assert.Equal(t, kessStore.putValues[1].TableInfo.SplitVersion, int32(1))
		assert.Equal(t, len(kessStore.putValues[1].Partitions), 16)
		assert.Equal(t, kessStore.putValues[1].Partitions[0].SplitVersion, int32(1))
		assert.Equal(t, kessStore.putValues[1].Partitions[1].SplitVersion, int32(0))
	}
}

func TestRouteRemoveHubs(t *testing.T) {
	usePazs := []bool{false, true}
	for _, usePaz := range usePazs {
		te := setupPublishTestEnv(t, usePaz)
		te.prepareNormalTable()
		defer te.teardown()

		storeOpts := []*route.StoreOption{
			{
				Media: "mock",
			},
		}
		publisher := NewRoutePublisher(
			te.table,
			storeOpts,
			WithMaxIntervalSecs(1000000),
			WithMinIntervalSecs(1),
		)
		publisher.Start()
		kessStore := publisher.routeStore.GetStore(storeOpts[0]).(*mockRouteKessStore)

		te.table.serviceLock.LockWrite()
		te.hubs = te.hubs[1:]
		te.table.nodes.UpdateHubs(utils.MapHubs(te.hubs), false)
		te.table.UpdateHubs(te.hubs)
		te.table.serviceLock.UnlockWrite()

		publisher.NotifyChange()
		publisher.Stop(false)

		assert.Assert(t, cmp.Len(kessStore.putValues, 1))

		routeEntries := kessStore.putValues[0]

		assert.Equal(t, routeEntries.TableInfo.HashMethod, "crc32")
		assert.Equal(t, len(routeEntries.ReplicaHubs), len(te.hubs))
		for i, hub := range te.hubs {
			assert.Equal(t, hub.Name, routeEntries.ReplicaHubs[i].Name)
			assert.Equal(t, hub.Az, routeEntries.ReplicaHubs[i].Az)
		}

		assert.Equal(t, len(routeEntries.Servers), 2)
		assert.Equal(t, routeEntries.Servers[0].Host, "127.0.0.2")
		assert.Equal(t, routeEntries.Servers[0].Ip, "127.0.0.2")
		assert.Equal(t, routeEntries.Servers[1].Host, "127.0.0.3")
		assert.Equal(t, routeEntries.Servers[1].Ip, "127.0.0.3")

		for _, serverLoc := range routeEntries.Servers {
			nodeInfo := te.getNodeInfoByBizPort(serverLoc.Host, serverLoc.Port)
			assert.Assert(t, nodeInfo != nil)
			assert.Equal(t, routeEntries.ReplicaHubs[serverLoc.HubId].Name, nodeInfo.Hub)
		}

		assert.Equal(t, len(routeEntries.Partitions), len(te.table.currParts))
		for i, part := range routeEntries.Partitions {
			tableMembership := &(te.table.currParts[i].members)
			assert.Equal(t, part.Version, tableMembership.MembershipVersion)
			assert.Equal(t, len(part.Replicas), len(tableMembership.Peers)-1)

			for _, rep := range part.Replicas {
				server := routeEntries.Servers[rep.ServerIndex]
				nodeInfo := te.getNodeInfoByBizPort(server.Host, server.Port)
				assert.Assert(t, nodeInfo != nil)
				role, ok := tableMembership.Peers[nodeInfo.Id]
				assert.Equal(t, ok, true)
				assert.Equal(t, role, rep.Role)
			}
		}
	}
}
func TestRetryPublishRoute(t *testing.T) {
	usePazs := []bool{false, true}
	for _, usePaz := range usePazs {
		te := setupPublishTestEnv(t, usePaz)
		te.prepareNormalTable()
		defer te.teardown()

		storeOpts := []*route.StoreOption{
			{
				Media: "mock",
			},
		}
		publisher := NewRoutePublisher(
			te.table,
			storeOpts,
			WithMaxIntervalSecs(1000000),
			WithMinIntervalSecs(1),
		)
		publisher.Start()
		kessStore := publisher.routeStore.GetStore(storeOpts[0]).(*mockRouteKessStore)

		publisher.NotifyChange()
		time.Sleep(time.Millisecond * 100)
		assert.Assert(t, cmp.Len(kessStore.putValues, 1))

		// Put retry
		kessStore.MakeIll()
		publisher.NotifyChange()
		for i := 0; i < 4; i++ {
			time.Sleep(time.Second * 2)
			assert.Assert(t, cmp.Len(kessStore.putValues, 1))
		}
		kessStore.UnMakeIll()
		time.Sleep(time.Second * 3)
		assert.Assert(t, cmp.Len(kessStore.putValues, 2))

		// Del retry
		kessStore.MakeIll()
		go func() {
			for i := 0; i < 4; i++ {
				time.Sleep(time.Second * 2)
				assert.Assert(t, cmp.Len(kessStore.putValues, 2))
			}
			kessStore.UnMakeIll()
		}()
		publisher.Stop(true)
		assert.Assert(t, cmp.Len(kessStore.putValues, 1))
	}
}
