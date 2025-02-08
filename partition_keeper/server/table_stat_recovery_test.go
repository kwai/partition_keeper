package server

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/delay_execute"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route"
	route_base "github.com/kuaishou/open_partition_keeper/partition_keeper/route/base"
	_ "github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/checkpoint"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	strategy_base "github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
)

const (
	kNonEmptyMockStore = "non_empty_mock"
)

type mockedNonEmptyRouteStore struct {
	entries *pb.RouteEntries
}

func (m *mockedNonEmptyRouteStore) Put(value *pb.RouteEntries) error {
	return errors.New("not implemented")
}

func (m *mockedNonEmptyRouteStore) Del() error {
	return nil
}

func (m *mockedNonEmptyRouteStore) Get() (*pb.RouteEntries, error) {
	return proto.Clone(m.entries).(*pb.RouteEntries), nil
}

type mockedRouteServiceStrategy struct {
	strategy_base.DummyStrategy
}

func (m *mockedRouteServiceStrategy) MakeStoreOpts(
	regions map[string]bool, service, table, jsonArgs string,
) ([]*route.StoreOption, error) {
	return []*route.StoreOption{
		{
			Media: kNonEmptyMockStore,
		},
	}, nil
}

func createRouteEntries(nodes []string, partsCount int) *pb.RouteEntries {
	entry := &pb.RouteEntries{}
	entry.TableInfo = &pb.TableInfo{HashMethod: "crc32"}
	entry.ReplicaHubs = []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
		{Name: "yz2", Az: "YZ"},
		{Name: "yz3", Az: "YZ"},
	}

	for i, node := range nodes {
		rpcNode := utils.FromHostPort(node)
		entry.Servers = append(entry.Servers, &pb.ServerLocation{
			Host:  rpcNode.NodeName,
			Port:  rpcNode.Port,
			HubId: int32(i),
			Alive: true,
			Op:    pb.AdminNodeOp_kNoop,
		})
	}

	for i := 0; i < partsCount; i++ {
		pl := &pb.PartitionLocation{
			Version: int64(rand.Intn(1000) + 1),
			Replicas: []*pb.ReplicaLocation{
				{Role: pb.ReplicaRole_kPrimary, ServerIndex: 0},
				{Role: pb.ReplicaRole_kSecondary, ServerIndex: 1},
				{Role: pb.ReplicaRole_kLearner, ServerIndex: 2},
			},
		}
		if i == partsCount-1 {
			// add a empty partition for test
			pl.Replicas = []*pb.ReplicaLocation{}
		}
		entry.Partitions = append(entry.Partitions, pl)
	}
	return entry
}

func checkPartsSyncedWithZk(t *testing.T, table *TableStats) {
	for i := range table.currParts {
		zkPath := table.getPartitionPath(int32(i))
		data, exists, succ := table.zkConn.Get(context.Background(), zkPath)
		assert.Assert(t, succ)
		assert.Assert(t, exists)

		part := table_model.PartitionMembership{}
		err := json.Unmarshal(data, &part)
		assert.NilError(t, err)
		assert.DeepEqual(t, table.currParts[i].members, part)
	}
}

func TestNormalTableRecovery(t *testing.T) {
	entry := createRouteEntries([]string{"127.0.0.1:2001", "127.0.0.2:2001", "127.0.0.3:2001"}, 16)
	getRoleInRoute := func(entry *pb.RouteEntries, part, index int) pb.ReplicaRole {
		reps := entry.Partitions[part].Replicas
		if index >= len(reps) {
			return pb.ReplicaRole_kInvalid
		}
		return reps[index].Role
	}

	route_base.RegisterStoreCreator(
		kNonEmptyMockStore,
		func(args *route_base.StoreBaseOption) route_base.StoreBase {
			logging.Info("create store mock")
			output := &mockedNonEmptyRouteStore{}
			output.entries = entry
			return output
		},
	)
	defer route_base.UnregisterStoreCreator(kNonEmptyMockStore)

	te := setupTestTableEnv(t, 4, 16, 3, &mockedRouteServiceStrategy{}, "", false, "zk")
	defer te.teardown()

	id0 := te.table.nodes.GetNodeInfoByAddr(utils.FromHostPort("127.0.0.1:1001"), true).Id
	id1 := te.table.nodes.GetNodeInfoByAddr(utils.FromHostPort("127.0.0.2:1001"), true).Id
	id2 := te.table.nodes.GetNodeInfoByAddr(utils.FromHostPort("127.0.0.3:1001"), true).Id

	tableProto := &pb.Table{
		TableId:                    1,
		TableName:                  "test",
		HashMethod:                 "crc32",
		PartsCount:                 int32(16),
		JsonArgs:                   "",
		KconfPath:                  "reco.rodisFea.partitionKeeperHDFSTest",
		RecoverPartitionsFromRoute: true,
		ScheduleGrayscale:          kScheduleGrayscaleMax + 1,
	}

	te.table.InitializeNew(tableProto, "")
	checkPartsSyncedWithZk(t, te.table)
	for i := range te.table.currParts {
		p := &(te.table.currParts[i])
		assert.Equal(t, p.members.MembershipVersion, entry.Partitions[i].Version)
		assert.Equal(t, p.members.GetMember(id0), getRoleInRoute(entry, i, 0))
		assert.Equal(t, p.members.GetMember(id1), getRoleInRoute(entry, i, 1))
		assert.Equal(t, p.members.GetMember(id2), getRoleInRoute(entry, i, 2))
	}
	assert.Equal(t, te.table.RecoverPartitionsFromRoute, false)
	assert.Equal(t, te.table.ScheduleGrayscale, int32(0))

	tblProto2 := &pb.Table{}
	data, exists, succ := te.zkStore.Get(context.Background(), te.table.zkPath)
	assert.Assert(t, exists)
	assert.Assert(t, succ)

	json.Unmarshal(data, tblProto2)
	assert.Assert(t, proto.Equal(te.table.Table, tblProto2))
}

func TestRecoveryHasDeadNode(t *testing.T) {
	entry := createRouteEntries([]string{"127.0.0.1:2001", "127.0.0.2:2001", "127.0.0.3:2001"}, 16)

	route_base.RegisterStoreCreator(
		kNonEmptyMockStore,
		func(args *route_base.StoreBaseOption) route_base.StoreBase {
			logging.Info("create store mock")
			output := &mockedNonEmptyRouteStore{}
			output.entries = entry
			return output
		},
	)
	defer route_base.UnregisterStoreCreator(kNonEmptyMockStore)

	te := setupTestTableEnv(t, 4, 16, 3, &mockedRouteServiceStrategy{}, "", false, "zk")
	defer te.teardown()

	te.table.serviceLock.LockWrite()
	node0 := te.table.nodes.GetNodeInfoByAddr(utils.FromHostPort("127.0.0.1:1001"), true)
	currentPing := node0.NodePing
	currentPing.IsAlive = false
	pings := map[string]*node_mgr.NodePing{node0.Id: &currentPing}
	te.table.nodes.UpdateStats(pings)
	te.table.serviceLock.UnlockWrite()

	id1 := te.table.nodes.GetNodeInfoByAddr(utils.FromHostPort("127.0.0.2:1001"), true).Id
	id2 := te.table.nodes.GetNodeInfoByAddr(utils.FromHostPort("127.0.0.3:1001"), true).Id

	tableProto := &pb.Table{
		TableId:                    1,
		TableName:                  "test",
		HashMethod:                 "crc32",
		PartsCount:                 int32(16),
		JsonArgs:                   "",
		KconfPath:                  "reco.rodisFea.partitionKeeperHDFSTest",
		RecoverPartitionsFromRoute: true,
		ScheduleGrayscale:          kScheduleGrayscaleMax + 1,
	}
	te.table.InitializeNew(tableProto, "")
	checkPartsSyncedWithZk(t, te.table)
	for i := range te.table.currParts {
		p := &(te.table.currParts[i])
		if i != 15 {
			assert.Equal(t, p.members.MembershipVersion, entry.Partitions[i].Version+1)
			assert.Equal(t, p.members.GetMember(node0.Id), pb.ReplicaRole_kLearner)
			assert.Equal(t, p.members.GetMember(id1), pb.ReplicaRole_kSecondary)
			assert.Equal(t, p.members.GetMember(id2), pb.ReplicaRole_kLearner)
		} else {
			assert.Equal(t, p.members.MembershipVersion, entry.Partitions[i].Version)
			assert.Equal(t, len(p.members.Peers), 0)
		}
	}
	assert.Equal(t, te.table.RecoverPartitionsFromRoute, false)
	assert.Equal(t, te.table.ScheduleGrayscale, int32(0))

	tblProto2 := &pb.Table{}
	data, exists, succ := te.zkStore.Get(context.Background(), te.table.zkPath)
	assert.Assert(t, exists)
	assert.Assert(t, succ)

	json.Unmarshal(data, tblProto2)
	assert.Assert(t, proto.Equal(te.table.Table, tblProto2))
}

func TestRecoveryHasUnrecognizedNode(t *testing.T) {
	entry := createRouteEntries([]string{"127.0.0.1:2001", "127.0.0.2:2001", "127.0.0.4:2001"}, 16)
	route_base.RegisterStoreCreator(
		kNonEmptyMockStore,
		func(args *route_base.StoreBaseOption) route_base.StoreBase {
			logging.Info("create store mock")
			output := &mockedNonEmptyRouteStore{}
			output.entries = entry
			return output
		},
	)
	defer route_base.UnregisterStoreCreator(kNonEmptyMockStore)

	te := setupTestTableEnv(t, 3, 16, 3, &mockedRouteServiceStrategy{}, "", false, "zk")
	defer te.teardown()

	id0 := te.table.nodes.GetNodeInfoByAddr(utils.FromHostPort("127.0.0.1:1001"), true).Id
	id1 := te.table.nodes.GetNodeInfoByAddr(utils.FromHostPort("127.0.0.2:1001"), true).Id
	tableProto := &pb.Table{
		TableId:                    1,
		TableName:                  "test",
		HashMethod:                 "crc32",
		PartsCount:                 int32(16),
		JsonArgs:                   "",
		KconfPath:                  "reco.rodisFea.partitionKeeperHDFSTest",
		RecoverPartitionsFromRoute: true,
		ScheduleGrayscale:          kScheduleGrayscaleMax + 1,
	}
	te.table.InitializeNew(tableProto, "")
	checkPartsSyncedWithZk(t, te.table)
	for i := range te.table.currParts {
		p := &(te.table.currParts[i])
		if i != 15 {
			assert.Equal(t, p.members.MembershipVersion, entry.Partitions[i].Version+1)
			assert.Equal(t, len(p.members.Peers), 2)
			assert.Equal(t, p.members.GetMember(id0), pb.ReplicaRole_kPrimary)
			assert.Equal(t, p.members.GetMember(id1), pb.ReplicaRole_kSecondary)
		} else {
			assert.Equal(t, p.members.MembershipVersion, entry.Partitions[i].Version)
			assert.Equal(t, len(p.members.Peers), 0)
		}
	}
	assert.Equal(t, te.table.RecoverPartitionsFromRoute, false)
	assert.Equal(t, te.table.ScheduleGrayscale, int32(0))

	tblProto2 := &pb.Table{}
	data, exists, succ := te.zkStore.Get(context.Background(), te.table.zkPath)
	assert.Assert(t, exists)
	assert.Assert(t, succ)

	json.Unmarshal(data, tblProto2)
	assert.Assert(t, proto.Equal(te.table.Table, tblProto2))
}

func TestContinueHalfRecovery(t *testing.T) {
	entry := createRouteEntries([]string{"127.0.0.1:2001", "127.0.0.2:2001", "127.0.0.3:2001"}, 16)
	route_base.RegisterStoreCreator(
		kNonEmptyMockStore,
		func(args *route_base.StoreBaseOption) route_base.StoreBase {
			logging.Info("create store mock")
			output := &mockedNonEmptyRouteStore{}
			output.entries = entry
			return output
		},
	)
	defer route_base.UnregisterStoreCreator(kNonEmptyMockStore)

	te := setupTestTableEnv(t, 4, 16, 3, &mockedRouteServiceStrategy{}, "", false, "zk")
	defer te.teardown()

	id0 := te.table.nodes.GetNodeInfoByAddr(utils.FromHostPort("127.0.0.1:1001"), true).Id
	id1 := te.table.nodes.GetNodeInfoByAddr(utils.FromHostPort("127.0.0.2:1001"), true).Id
	id2 := te.table.nodes.GetNodeInfoByAddr(utils.FromHostPort("127.0.0.3:1001"), true).Id

	tableProto := &pb.Table{
		TableId:                    1,
		TableName:                  "test",
		HashMethod:                 "crc32",
		PartsCount:                 int32(16),
		JsonArgs:                   "",
		KconfPath:                  "reco.rodisFea.partitionKeeperHDFSTest",
		RecoverPartitionsFromRoute: true,
		ScheduleGrayscale:          kScheduleGrayscaleMax + 1,
	}

	logging.Info("first do recover")
	te.table.InitializeNew(tableProto, "")
	te.table.Stop(false)

	logging.Info("remove some nodes and make table in recovering state")
	succ := te.zkStore.Delete(context.Background(), te.table.getPartitionPath(0))
	assert.Assert(t, succ)

	tableProto.RecoverPartitionsFromRoute = true
	data, err := json.Marshal(tableProto)
	assert.NilError(t, err)
	succ = te.zkStore.Set(context.Background(), te.table.zkPath, data)
	assert.Assert(t, succ)

	delayedExecutorManager := delay_execute.NewDelayedExecutorManager(
		te.zkStore,
		utils.SpliceZkRootPath("/test/table_recovery_test/delayed_executor"),
	)
	delayedExecutorManager.InitFromZookeeper()

	tableStat2 := NewTableStats(
		te.table.serviceName,
		te.table.namespace,
		false,
		utils.NewLooseLock(),
		te.table.nodes,
		te.table.hubs,
		te.table.svcStrategy,
		te.table.nodesConn,
		te.table.zkConn,
		delayedExecutorManager,
		te.table.parentZkPath,
		nil,
	)
	tableStat2.LoadFromZookeeper(tableProto.TableName)
	defer tableStat2.Stop(false)

	for i := range tableStat2.currParts {
		p := &(tableStat2.currParts[i])
		assert.Equal(t, p.members.MembershipVersion, entry.Partitions[i].Version)
		if i != 15 {
			assert.Equal(t, p.members.GetMember(id0), pb.ReplicaRole_kPrimary)
			assert.Equal(t, p.members.GetMember(id1), pb.ReplicaRole_kSecondary)
			assert.Equal(t, p.members.GetMember(id2), pb.ReplicaRole_kLearner)
		} else {
			assert.Equal(t, len(p.members.Peers), 0)
		}
	}
	assert.Equal(t, tableStat2.RecoverPartitionsFromRoute, false)
	assert.Equal(t, te.table.ScheduleGrayscale, int32(0))

	tblProto2 := &pb.Table{}
	data, exists, succ := te.zkStore.Get(context.Background(), te.table.zkPath)
	assert.Assert(t, exists)
	assert.Assert(t, succ)

	json.Unmarshal(data, tblProto2)
	assert.Assert(t, proto.Equal(tblProto2, tableStat2.Table))

	_, exists, succ = te.zkStore.Get(context.Background(), tableStat2.getPartitionPath(0))
	assert.Assert(t, succ)
	assert.Assert(t, exists)
}
