package server

import (
	"context"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/delay_execute"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/sd"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/checkpoint"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/est"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/watcher"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/embedding_server"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/rodis"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
	"gotest.tools/assert/cmp"
)

var (
	serviceStatTestZkPrefix = utils.SpliceZkRootPath("/test/pk/services")
)

type mockedMakeStoreOptsDummy struct {
	base.DummyStrategy
}

func (m *mockedMakeStoreOptsDummy) MakeStoreOpts(
	regions map[string]bool,
	svc, table, args string,
) ([]*route.StoreOption, error) {
	return nil, nil
}

type mockedMakeStoreOptsEmbedding struct {
	embedding_server.EmbeddingServerStrategy
}

func (m *mockedMakeStoreOptsEmbedding) MakeStoreOpts(
	regions map[string]bool,
	svc, table, args string,
) ([]*route.StoreOption, error) {
	return nil, nil
}

type mockedMakeStoreOptsRodis struct {
	rodis.RodisStrategy
}

func (m *mockedMakeStoreOptsRodis) MakeStoreOpts(
	regions map[string]bool,
	svc, table, args string,
) ([]*route.StoreOption, error) {
	return nil, nil
}

func init() {
	strategy.RegisterServiceStrategy(
		pb.ServiceType_colossusdb_dummy,
		func() strategy.ServiceStrategy {
			return &mockedMakeStoreOptsDummy{}
		},
	)
	strategy.RegisterServiceStrategy(
		pb.ServiceType_colossusdb_embedding_server,
		func() strategy.ServiceStrategy {
			return &mockedMakeStoreOptsEmbedding{}
		},
	)
	strategy.RegisterServiceStrategy(
		pb.ServiceType_colossusdb_rodis,
		func() strategy.ServiceStrategy {
			return &mockedMakeStoreOptsRodis{}
		},
	)
}

func bootstrapService(
	t *testing.T,
	opts *pb.CreateServiceRequest,
	serviceName, tableName string, tableParts int32,
	hubs []*pb.ReplicaHub,
	hints map[string]*pb.NodeHints,
	metaStoreType string,
) metastore.MetaStore {
	var metaStore metastore.MetaStore
	if metaStoreType == "zk" {
		acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
		metaStore = metastore.CreateZookeeperStore(
			[]string{"127.0.0.1:2181"},
			time.Second*10,
			acl,
			scheme,
			auth,
		)
	} else {
		metaStore = metastore.NewMockMetaStore()
	}

	ans := metaStore.RecursiveDelete(context.Background(), serviceStatTestZkPrefix)
	assert.Assert(t, ans)

	ans = metaStore.RecursiveCreate(context.Background(), serviceStatTestZkPrefix)
	assert.Assert(t, ans)

	tablesManager := NewTablesManager(metaStore, serviceStatTestZkPrefix+"/tables")
	tablesManager.InitFromZookeeper()

	delayedExecutorManager := delay_execute.NewDelayedExecutorManager(
		metaStore,
		serviceStatTestZkPrefix+"/"+kDelayedExecutorNode,
	)
	delayedExecutorManager.InitFromZookeeper()
	defer delayedExecutorManager.Stop()

	sd := sd.NewServiceDiscovery(
		sd.SdTypeDirectUrl,
		"test",
		sd.DirectSdGivenUrl("http://127.0.0.1:4444"),
	)
	kd := node_mgr.NewNodeDetector(
		serviceName,
		node_mgr.WithServiceDiscovery(sd),
		node_mgr.WithPollKessIntervalSecs(100000),
	)

	checker := NewServiceChecker(serviceName, true)
	service := NewServiceStat(
		"test",
		"test",
		serviceStatTestZkPrefix,
		metaStore,
		tablesManager,
		delayedExecutorManager,
		make(chan<- string),
		WithRpcPoolBuilder(&rpc.LocalPSClientPoolBuilder{}),
		WithDetector(kd),
		WithChecker(checker),
	)
	defer service.stop()

	createServiceReq := proto.Clone(opts).(*pb.CreateServiceRequest)
	createServiceReq.NodesHubs = hubs

	err := service.InitializeNew(createServiceReq)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))

	if hints != nil {
		service.nodes.WithSkipHintCheck(true)
		err := service.GiveHints(hints, true)
		assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	}

	if tableName != "" {
		tablePb := &pb.Table{
			TableId:           0,
			TableName:         tableName,
			HashMethod:        "crc32",
			PartsCount:        tableParts,
			JsonArgs:          `{"a": "b", "c": "d", "btq_prefix": "dummy"}`,
			KconfPath:         "reco.rodisFea.partitionKeeperHDFSTest",
			ScheduleGrayscale: kScheduleGrayscaleMax + 1,
		}

		status := service.AddTable(tablePb, "", "")
		assert.Equal(t, status.Code, int32(pb.AdminError_kOk), "error: %v", status.Message)
	}
	return metaStore
}

func findReplica(resp *pb.QueryNodeInfoResponse, tableName string, pid int32) *pb.ReplicaInfo {
	for _, r := range resp.Replicas {
		if r.TableName == tableName && r.PartitionId == pid {
			return r
		}
	}
	return nil
}

func checkQueryNode(
	t *testing.T,
	sv *ServiceStat,
	rpcNode *utils.RpcNode,
	b *rpc.LocalPSClientPoolBuilder,
	otherTables int,
	log bool,
) bool {
	ipPort := rpcNode.String()
	req := &pb.QueryNodeInfoRequest{
		NodeName:  rpcNode.NodeName,
		Port:      rpcNode.Port,
		TableName: "",
		OnlyBrief: false,
		MatchPort: true,
	}
	resp := &pb.QueryNodeInfoResponse{}
	sv.QueryNodeInfo(req, resp)
	if resp.Status.Code != int32(pb.AdminError_kOk) {
		logging.InfoIf(log, "query %s, got %v", ipPort, resp.Status.Code)
		return false
	}
	if resp.Brief.Node.NodeName != rpcNode.NodeName {
		logging.InfoIf(log, "query %s, got %v", ipPort, resp.Brief.Node.NodeName)
		return false
	}
	if resp.Brief.Node.Port != rpcNode.Port {
		logging.InfoIf(log, "query %s, got %v", ipPort, resp.Brief.Node.Port)
		return false
	}
	client := b.LocalClients[ipPort]
	if client == nil {
		logging.InfoIf(log, "query %s, got %v", ipPort, client)
		return false
	}
	getResp, err := client.GetReplicas(context.Background(), &pb.GetReplicasRequest{})
	assert.NilError(t, err)

	if len(getResp.Infos) != len(resp.Replicas)+otherTables {
		logging.InfoIf(
			log,
			"query %s, got %v, expect: %v",
			ipPort,
			len(getResp.Infos),
			len(resp.Replicas),
		)
		return false
	}

	otherTableReplicasOnClient := 0
	for i, r := range getResp.Infos {
		assert.Equal(t, getResp.Results[i].Code, int32(pb.PartitionError_kOK))
		assert.Assert(t, pb.RemoveOtherPeers(r.PeerInfo, utils.FromHostPort(ipPort).ToPb()))
		if _, ok := sv.tables[r.TableId]; !ok {
			otherTableReplicasOnClient++
			continue
		}
		tableName := sv.tables[r.TableId].TableName
		reportedNode := findReplica(resp, tableName, r.PartitionId)
		if reportedNode == nil {
			logging.InfoIf(
				log,
				"query %s, expect has %s:%d, but got none",
				ipPort,
				tableName,
				r.PartitionId,
			)
			return false
		}
		if reportedNode.Role != r.PeerInfo.Peers[0].Role {
			logging.InfoIf(
				log,
				"query %s, expect %s:%d role %v vs %v",
				ipPort,
				tableName,
				r.PartitionId,
				reportedNode.Role,
				r.PeerInfo.Peers[0].Role,
			)
			return false
		}
	}

	return otherTableReplicasOnClient == otherTables
}

func checkQueryNodes(
	t *testing.T,
	sv *ServiceStat,
	nodes []*utils.RpcNode,
	b *rpc.LocalPSClientPoolBuilder,
	otherTables []int,
) {
	for i := 0; i < 3; i++ {
		utils.WaitCondition(t, func(log bool) bool {
			return checkQueryNode(t, sv, nodes[i], b, otherTables[i], log)
		}, 10)
	}
}

type testServiceEnvSetupOptions struct {
	openSched     bool
	createTable   bool
	metaStoreType string
	tableParts    int32
}

type testServiceEnv struct {
	t             *testing.T
	mockKess      *sd.DiscoveryMockServer
	lpb           *rpc.LocalPSClientPoolBuilder
	metaStore     metastore.MetaStore
	service       *ServiceStat
	kessDetector  *node_mgr.KessBasedNodeDetector
	nodeCollector *node_mgr.NodeCollector
}

// service name and hubs are ignored in opts
func setupTestServiceEnv(
	t *testing.T,
	initialNodes []*node_mgr.NodeInfo,
	opts *pb.CreateServiceRequest,
	envOpts *testServiceEnvSetupOptions,
) *testServiceEnv {
	// first initialize zookeeper
	namespace := "test"
	serviceName := "test"
	tableName := "test"
	if !envOpts.createTable {
		tableName = ""
	}

	hubmap := utils.NewHubHandle()
	for _, info := range initialNodes {
		hubmap.AddHub(&pb.ReplicaHub{Name: info.Hub, Az: info.Az})
	}
	hubs := hubmap.ListHubs()

	hints := map[string]*pb.NodeHints{}
	for _, info := range initialNodes {
		hints[info.Address.String()] = &pb.NodeHints{Hub: info.Hub}
	}
	metaStore := bootstrapService(
		t,
		opts,
		serviceName,
		tableName,
		envOpts.tableParts,
		hubs,
		hints,
		envOpts.metaStoreType,
	)
	outEnv := &testServiceEnv{
		t:         t,
		metaStore: metaStore,
	}

	// then start mocked kess server & kess node detector
	kessPort := 4444
	mockKess := sd.NewDiscoveryMockServer()
	mockKess.Start(kessPort)
	for _, nd := range initialNodes {
		node_mgr.MockedKessUpdateNodePing(mockKess, nd.Id, &(nd.NodePing))
	}
	outEnv.mockKess = mockKess

	outEnv.lpb = rpc.NewLocalPSClientPoolBuilder()

	sd := sd.NewServiceDiscovery(
		sd.SdTypeDirectUrl,
		"test",
		sd.DirectSdGivenUrl("http://127.0.0.1:4444"),
	)
	outEnv.kessDetector = node_mgr.NewNodeDetector(
		serviceName,
		node_mgr.WithServiceDiscovery(sd),
		node_mgr.WithPollKessIntervalSecs(100000),
	)
	outEnv.nodeCollector = node_mgr.NewNodeCollector(
		"test",
		"test",
		outEnv.lpb,
		node_mgr.WithCollectIntervalSecs(100000),
	)

	*delay_execute.FlagScheduleExecuteDuration = time.Second * 2
	delayedExecutorManager := delay_execute.NewDelayedExecutorManager(
		outEnv.metaStore,
		serviceStatTestZkPrefix+"/"+kDelayedExecutorNode,
	)
	delayedExecutorManager.InitFromZookeeper()

	tablesManager := NewTablesManager(outEnv.metaStore, serviceStatTestZkPrefix+"/tables")
	tablesManager.InitFromZookeeper()

	outEnv.service = NewServiceStat(
		serviceName,
		namespace,
		serviceStatTestZkPrefix,
		outEnv.metaStore,
		tablesManager,
		delayedExecutorManager,
		make(chan<- string),
		WithRpcPoolBuilder(outEnv.lpb),
		WithDetector(outEnv.kessDetector),
		WithCollector(outEnv.nodeCollector),
		WithScheduleIntervalSeconds(10000000),
	)

	outEnv.service.LoadFromZookeeper()

	if envOpts.openSched {
		result := outEnv.service.SwitchSchedulerStatus(true)
		assert.Equal(t, result.Code, int32(pb.AdminError_kOk))
		outEnv.TriggerDetectAndWaitFinish()
		outEnv.service.lock.LockWrite()
		assert.Assert(t, outEnv.service.checkScores())
		outEnv.service.lock.UnlockWrite()
	}

	return outEnv
}

func (te *testServiceEnv) reloadUpdatedMembership() {
	te.service.stop()
	te.service.LoadFromZookeeper()

	tbl := te.service.tableNames["test"]
	logging.Info("first reconcile facts to nodes")
	te.TriggerScheduleAndWaitFinish()
	assert.Assert(te.t, cmp.Len(tbl.plans, 0))

	logging.Info("then sync stats")
	te.TriggerCollectAndWaitFinish()
	assert.Assert(te.t, cmp.Len(tbl.plans, 0))
}

func (te *testServiceEnv) initGivenMembership(p *table_model.PartitionMembership) {
	logging.Info("start to assign given membership for each replica")
	tbl := te.service.tableNames["test"]
	for i := range tbl.currParts {
		tbl.currParts[i].initialize(tbl, int32(i))
		tbl.currParts[i].newMembers.MembershipVersion = p.MembershipVersion
		tbl.currParts[i].newMembers.Peers = p.Peers
		tbl.currParts[i].durableNewMembership()
	}
	te.reloadUpdatedMembership()
}

func (te *testServiceEnv) makeTestTableNormal() {
	p := table_model.PartitionMembership{
		MembershipVersion: 7,
		Peers: map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kPrimary,
			"node2": pb.ReplicaRole_kSecondary,
			"node3": pb.ReplicaRole_kSecondary,
		},
	}
	te.initGivenMembership(&p)
}

func (te *testServiceEnv) TriggerScheduleAndWaitFinish() {
	schedCount := te.service.GetScheduleLoopCount()
	te.service.TriggerSchedule()
	te.service.WaitScheduleLoopBeyond(schedCount)
	time.Sleep(time.Millisecond * 100)
}

func (te *testServiceEnv) TriggerCollectAndWaitFinish() {
	schedCount := te.service.GetScheduleLoopCount()
	te.nodeCollector.TriggerCollect()
	te.service.WaitScheduleLoopBeyond(schedCount)
}

func (te *testServiceEnv) TriggerDetectAndWaitFinish() {
	schedCount := te.service.GetScheduleLoopCount()
	te.kessDetector.TriggerDetection()
	te.service.WaitScheduleLoopBeyond(schedCount)
}

func (te *testServiceEnv) TriggerScheduleAndCollect() {
	te.TriggerScheduleAndWaitFinish()
	assert.Assert(te.t, !te.service.tableNames["test"].StatsConsistency(false))
	te.TriggerCollectAndWaitFinish()
	assert.Assert(te.t, te.service.tableNames["test"].StatsConsistency(false))
}

func (te *testServiceEnv) stop() {
	*delay_execute.FlagScheduleExecuteDuration = time.Second * 60
	te.service.stop()
	ans := te.metaStore.RecursiveDelete(context.Background(), serviceStatTestZkPrefix)
	assert.Assert(te.t, ans)
	te.service.tablesManager.Stop()
	te.service.delayedExecutorManager.Stop()
	te.metaStore.Close()
	te.mockKess.Stop()
}

func TestFailureDomainArgsEffective(t *testing.T) {
	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	zkStore := metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"},
		time.Second*10,
		acl,
		scheme,
		auth,
	)
	defer zkStore.Close()

	ans := zkStore.RecursiveDelete(context.Background(), serviceStatTestZkPrefix)
	assert.Assert(t, ans)

	ans = zkStore.RecursiveCreate(context.Background(), serviceStatTestZkPrefix)
	assert.Assert(t, ans)

	tablesManager := NewTablesManager(zkStore, serviceStatTestZkPrefix+"/tables")
	tablesManager.InitFromZookeeper()

	delayedExecutorManager := delay_execute.NewDelayedExecutorManager(
		zkStore,
		serviceStatTestZkPrefix+"/"+kDelayedExecutorNode,
	)
	delayedExecutorManager.InitFromZookeeper()
	defer delayedExecutorManager.Stop()

	discovery := sd.NewServiceDiscovery(
		sd.SdTypeDirectUrl,
		"test",
		sd.DirectSdGivenUrl("http://127.0.0.1:4444"),
	)
	kd := node_mgr.NewNodeDetector(
		"test_service",
		node_mgr.WithServiceDiscovery(discovery),
		node_mgr.WithPollKessIntervalSecs(100000),
	)

	checker := NewServiceChecker("test_service", true)
	service := NewServiceStat(
		"test_service",
		"test",
		serviceStatTestZkPrefix,
		zkStore,
		tablesManager,
		delayedExecutorManager,
		make(chan<- string),
		WithRpcPoolBuilder(&rpc.LocalPSClientPoolBuilder{}),
		WithDetector(kd),
		WithChecker(checker),
	)
	defer service.stop()

	createServiceReq := &pb.CreateServiceRequest{
		NodesHubs: []*pb.ReplicaHub{
			{Name: "TEST_1", Az: "TEST"},
			{Name: "TEST_2", Az: "TEST"},
		},
		ServiceType:       pb.ServiceType_colossusdb_dummy,
		FailureDomainType: pb.NodeFailureDomainType_PROCESS,
	}
	err := service.InitializeNew(createServiceReq)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))

	nodes := map[string]*node_mgr.NodePing{
		"node1": {
			IsAlive:   true,
			Az:        "TEST",
			Address:   utils.FromHostPort("127.0.0.1:1001"),
			ProcessId: "1001",
			BizPort:   2001,
		},
		"node2": {
			IsAlive:   true,
			Az:        "TEST",
			Address:   utils.FromHostPort("127.0.0.1:1002"),
			ProcessId: "1002",
			BizPort:   2002,
		},
	}

	kessPort := 4444
	mockKess := sd.NewDiscoveryMockServer()
	mockKess.Start(kessPort)
	defer mockKess.Stop()

	time.Sleep(time.Millisecond * 100)
	for id, ping := range nodes {
		node_mgr.MockedKessUpdateNodePing(mockKess, id, ping)
	}

	count := service.GetScheduleLoopCount()
	service.detector.TriggerDetection()
	service.WaitScheduleLoopBeyond(count)

	info1 := service.nodes.GetNodeInfo("node1")
	info2 := service.nodes.GetNodeInfo("node2")
	assert.Assert(t, info1.Hub != info2.Hub)
}

func TestScheduleFromEmptyCluster(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
	}

	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	logging.Info("listNodes will list all detected nodes")
	listNodesResp := &pb.ListNodesResponse{}
	for i := 0; i < 10; i++ {
		te.service.ListNodes(&pb.ListNodesRequest{}, listNodesResp)
		assert.Equal(t, listNodesResp.Status.Code, int32(pb.AdminError_kOk))
		if len(listNodesResp.Nodes) < 3 {
			time.Sleep(time.Millisecond * 100)
		} else {
			break
		}
	}

	assert.Assert(t, cmp.Len(listNodesResp.Nodes, 3))
	sort.Slice(listNodesResp.Nodes, func(i, j int) bool {
		return listNodesResp.Nodes[i].Node.Compare(listNodesResp.Nodes[j].Node) < 0
	})

	for i, node := range listNodesResp.Nodes {
		assert.Equal(t, node.Node.ToHostPort(), nodes[i].Address.String())
		assert.Equal(t, node.NodeUniqId, nodes[i].Id)
		assert.Assert(t, node.IsAlive)
		assert.Equal(t, node.Op, pb.AdminNodeOp_kNoop)
		assert.Equal(t, node.PrimaryCount, int32(0))
		assert.Equal(t, node.SecondaryCount, int32(0))
		assert.Equal(t, node.LearnerCount, int32(0))
	}

	logging.Info("start first round of schedule, will assign learner on nodes")
	te.TriggerScheduleAndWaitFinish()

	logging.Info("as we haven't collect data from nodes, so reschedule will trigger reconcileFacts")
	te.TriggerScheduleAndWaitFinish()

	psClients := te.lpb.LocalClients
	gotDuplicateReq := false
	for _, client := range psClients {
		if client.DuplicateReq {
			gotDuplicateReq = true
		}
	}
	assert.Assert(t, gotDuplicateReq)

	logging.Info("added replicas will have all the necessary table info")
	tbl := te.service.tableNames["test"]
	someReplicasAdd := 0
	for _, client := range psClients {
		for _, rep := range client.KeptReplicas() {
			someReplicasAdd++
			assert.Equal(t, rep.Part.PartitionNum, tbl.PartsCount)
			assert.Equal(t, rep.Part.TableId, tbl.TableId)
			assert.Equal(t, rep.Part.TableJsonArgs, tbl.JsonArgs)
			assert.Equal(t, rep.Part.TableKconfPath, tbl.KconfPath)
			assert.Equal(t, rep.Part.TableName, tbl.TableName)
		}
	}
	assert.Assert(t, someReplicasAdd > 0)

	logging.Info("then start a round of collection")
	te.TriggerCollectAndWaitFinish()
	checkQueryNodes(
		t,
		te.service,
		[]*utils.RpcNode{nodes[0].Address, nodes[1].Address, nodes[2].Address},
		te.lpb,
		[]int{0, 0, 0},
	)
	gotRemoveReplicaReq := 0
	for _, client := range psClients {
		gotRemoveReplicaReq += client.GetMetric("remove_replica")
	}
	assert.Equal(t, gotRemoveReplicaReq, 0)

	logging.Info("given a fake replica & issue another round of collection")
	psClients[nodes[0].Address.String()].AddReplica(context.Background(),
		&pb.AddReplicaRequest{
			Part: &pb.PartitionInfo{
				PartitionId: 45,
				TableId:     tbl.TableId,
				TableName:   "test",
			},
			PeerInfo: &pb.PartitionPeerInfo{
				MembershipVersion: 1,
				Peers: []*pb.PartitionReplica{
					{
						Role: pb.ReplicaRole_kLearner,
						Node: nodes[0].Address.ToPb(),
					},
				},
			},
		})

	psClients[nodes[0].Address.String()].AddReplica(context.Background(),
		&pb.AddReplicaRequest{
			Part: &pb.PartitionInfo{
				PartitionId: 45,
				TableId:     2,
				TableName:   "test",
			},
			PeerInfo: &pb.PartitionPeerInfo{
				MembershipVersion: 1,
				Peers: []*pb.PartitionReplica{
					{
						Role: pb.ReplicaRole_kLearner,
						Node: nodes[0].Address.ToPb(),
					},
				},
			},
		})

	te.TriggerCollectAndWaitFinish()

	checkQueryNodes(
		t,
		te.service,
		[]*utils.RpcNode{nodes[0].Address, nodes[1].Address, nodes[2].Address},
		te.lpb,
		[]int{1, 0, 0},
	)
	assert.Equal(t, psClients[nodes[0].Address.String()].GetMetric("remove_replica"), 1)
}

func TestNormalClusterTriggerBalance(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
		{
			Id:  "node4",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1004"),
				ProcessId: "12343",
				BizPort:   2004,
			},
		},
		{
			Id:  "node5",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1005"),
				ProcessId: "12344",
				BizPort:   2005,
			},
		},
		{
			Id:  "node6",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1006"),
				ProcessId: "12345",
				BizPort:   2006,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	te.makeTestTableNormal()

	tbl := te.service.tableNames["test"]

	logging.Info("then balanced is issued")
	te.TriggerScheduleAndWaitFinish()
	assert.Assert(t, len(tbl.plans) > 0)
}

func TestSchedulerDisabled(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{false, true, "zk", 32})
	defer te.stop()

	tbl := te.service.tableNames["test"]
	te.TriggerDetectAndWaitFinish()
	te.service.lock.LockRead()
	assert.Equal(t, len(te.service.nodes.AllNodes()), 3)
	te.service.lock.UnlockRead()

	logging.Info("then disable scheduler, so no action is triggered")
	ans := te.service.SwitchSchedulerStatus(false)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	te.TriggerScheduleAndWaitFinish()
	for i := 0; i < len(tbl.currParts); i++ {
		assert.Equal(t, tbl.currParts[i].members.MembershipVersion, int64(0))
	}
	logging.Info("then we enable scheduler, some nodes will be assigned as learner")
	ans = te.service.SwitchSchedulerStatus(true)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	te.TriggerScheduleAndCollect()
	for i := 0; i < len(tbl.currParts); i++ {
		assert.Equal(t, tbl.currParts[i].members.MembershipVersion, int64(1))
	}
}

func TestKessPollerDisabled(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{false, true, "zk", 32})
	defer te.stop()

	ans := te.service.SwitchKessPollerStatus(false)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	te.kessDetector.TriggerDetection()
	time.Sleep(time.Millisecond * 1000)
	te.service.lock.LockRead()
	assert.Equal(t, len(te.service.nodes.AllNodes()), 0)
	te.service.lock.UnlockRead()

	ans = te.service.SwitchKessPollerStatus(true)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	te.kessDetector.TriggerDetection()
	time.Sleep(time.Millisecond * 1000)
	te.service.lock.LockRead()
	assert.Equal(t, len(te.service.nodes.AllNodes()), 3)
	te.service.lock.UnlockRead()
}

func TestScheduleOptions(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{false, true, "zk", 32})
	defer te.stop()

	queryServiceResp := &pb.QueryServiceResponse{}
	te.service.QueryService(queryServiceResp)
	assert.Equal(t, queryServiceResp.SchedOpts.EnablePrimaryScheduler, true)
	assert.Equal(t, queryServiceResp.SchedOpts.MaxSchedRatio, int32(10))
	assert.Equal(t, queryServiceResp.SchedOpts.Estimator, est.DEFAULT_ESTIMATOR)
	assert.Equal(t, queryServiceResp.SchedOpts.ForceRescoreNodes, false)
	assert.Equal(t, queryServiceResp.SchedOpts.EnableSplitBalancer, false)
	assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerAddReplicaFirst, false)
	assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerMaxOverflowReplicas, int32(0))
	assert.Equal(t, queryServiceResp.SchedOpts.MaxLearningPartsPerNode, int32(0))

	logging.Info("update invalid options")
	invalidOpts := []*pb.ScheduleOptions{
		{
			EnablePrimaryScheduler:          false,
			MaxSchedRatio:                   0,
			Estimator:                       est.DEFAULT_ESTIMATOR,
			ForceRescoreNodes:               true,
			EnableSplitBalancer:             true,
			HashArrangerAddReplicaFirst:     true,
			HashArrangerMaxOverflowReplicas: 1,
			MaxLearningPartsPerNode:         10,
		},
		{
			EnablePrimaryScheduler:          false,
			MaxSchedRatio:                   1001,
			Estimator:                       est.DEFAULT_ESTIMATOR,
			ForceRescoreNodes:               true,
			EnableSplitBalancer:             true,
			HashArrangerAddReplicaFirst:     true,
			HashArrangerMaxOverflowReplicas: 1,
			MaxLearningPartsPerNode:         10,
		},
		{
			EnablePrimaryScheduler:          false,
			MaxSchedRatio:                   50,
			Estimator:                       "invalid",
			ForceRescoreNodes:               true,
			EnableSplitBalancer:             true,
			HashArrangerAddReplicaFirst:     true,
			HashArrangerMaxOverflowReplicas: 1,
			MaxLearningPartsPerNode:         10,
		},
		{
			EnablePrimaryScheduler:          false,
			MaxSchedRatio:                   50,
			Estimator:                       est.DEFAULT_ESTIMATOR,
			ForceRescoreNodes:               true,
			EnableSplitBalancer:             true,
			HashArrangerAddReplicaFirst:     true,
			HashArrangerMaxOverflowReplicas: -1,
			MaxLearningPartsPerNode:         10,
		},
	}
	for _, opt := range invalidOpts {
		ans := te.service.UpdateScheduleOptions(
			opt,
			[]string{
				"enable_primary_scheduler",
				"max_sched_ratio",
				"estimator",
				"force_rescore_nodes",
				"enable_split_balancer",
				"hash_arranger_add_replica_first",
				"hash_arranger_max_overflow_replicas",
				"max_learning_parts_per_node",
			},
		)
		assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
		te.service.QueryService(queryServiceResp)
		assert.Equal(t, queryServiceResp.SchedOpts.EnablePrimaryScheduler, true)
		assert.Equal(t, queryServiceResp.SchedOpts.MaxSchedRatio, int32(10))
		assert.Equal(t, queryServiceResp.SchedOpts.Estimator, est.DEFAULT_ESTIMATOR)
		assert.Equal(t, queryServiceResp.SchedOpts.ForceRescoreNodes, false)
		assert.Equal(t, queryServiceResp.SchedOpts.EnableSplitBalancer, false)
		assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerAddReplicaFirst, false)
		assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerMaxOverflowReplicas, int32(0))
		assert.Equal(t, queryServiceResp.SchedOpts.MaxLearningPartsPerNode, int32(0))
	}

	logging.Info("update some options")
	ans := te.service.UpdateScheduleOptions(
		&pb.ScheduleOptions{
			EnablePrimaryScheduler: false,
			MaxSchedRatio:          200,
			Estimator:              "invalid_est",
			ForceRescoreNodes:      true,
			EnableSplitBalancer:    true,
		},
		[]string{"enable_primary_scheduler", "max_sched_ratio", "enable_split_balancer"},
	)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	te.service.QueryService(queryServiceResp)
	assert.Equal(t, queryServiceResp.SchedOpts.EnablePrimaryScheduler, false)
	assert.Equal(t, queryServiceResp.SchedOpts.MaxSchedRatio, int32(200))
	assert.Equal(t, queryServiceResp.SchedOpts.EnableSplitBalancer, true)
	assert.Equal(t, queryServiceResp.SchedOpts.Estimator, est.DEFAULT_ESTIMATOR)
	assert.Equal(t, queryServiceResp.SchedOpts.ForceRescoreNodes, false)
	assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerAddReplicaFirst, false)
	assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerMaxOverflowReplicas, int32(0))
	assert.Equal(t, queryServiceResp.SchedOpts.MaxLearningPartsPerNode, int32(0))

	data, exists, ok := te.service.zkConn.Get(
		context.Background(),
		te.service.zkScheduleOptionsPath,
	)
	assert.Assert(t, exists && ok)
	schedOpts := &pb.ScheduleOptions{}
	utils.UnmarshalJsonOrDie(data, schedOpts)
	assert.Assert(t, proto.Equal(schedOpts, queryServiceResp.SchedOpts))

	logging.Info("update estimator")
	ans = te.service.UpdateScheduleOptions(
		&pb.ScheduleOptions{
			EnablePrimaryScheduler: true,
			MaxSchedRatio:          500,
			Estimator:              est.DISK_CAP_ESTIMATOR,
			ForceRescoreNodes:      false,
			EnableSplitBalancer:    false,
		},
		[]string{"estimator", "force_rescore_nodes"},
	)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	te.service.QueryService(queryServiceResp)
	assert.Equal(t, queryServiceResp.SchedOpts.EnablePrimaryScheduler, false)
	assert.Equal(t, queryServiceResp.SchedOpts.MaxSchedRatio, int32(200))
	assert.Equal(t, queryServiceResp.SchedOpts.EnableSplitBalancer, true)
	assert.Equal(t, queryServiceResp.SchedOpts.Estimator, est.DISK_CAP_ESTIMATOR)
	assert.Equal(t, queryServiceResp.SchedOpts.ForceRescoreNodes, true)
	assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerAddReplicaFirst, false)
	assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerMaxOverflowReplicas, int32(0))
	data, exists, ok = te.service.zkConn.Get(
		context.Background(),
		te.service.zkScheduleOptionsPath,
	)
	assert.Assert(t, exists && ok)
	schedOpts = &pb.ScheduleOptions{}
	utils.UnmarshalJsonOrDie(data, schedOpts)
	assert.Assert(t, proto.Equal(schedOpts, queryServiceResp.SchedOpts))

	logging.Info("update force_rescore_nodes")
	ans = te.service.UpdateScheduleOptions(
		&pb.ScheduleOptions{
			EnablePrimaryScheduler: true,
			MaxSchedRatio:          500,
			Estimator:              est.DEFAULT_ESTIMATOR,
			ForceRescoreNodes:      false,
			EnableSplitBalancer:    false,
		},
		[]string{"force_rescore_nodes"},
	)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	te.service.QueryService(queryServiceResp)
	assert.Equal(t, queryServiceResp.SchedOpts.EnablePrimaryScheduler, false)
	assert.Equal(t, queryServiceResp.SchedOpts.MaxSchedRatio, int32(200))
	assert.Equal(t, queryServiceResp.SchedOpts.EnableSplitBalancer, true)
	assert.Equal(t, queryServiceResp.SchedOpts.Estimator, est.DISK_CAP_ESTIMATOR)
	assert.Equal(t, queryServiceResp.SchedOpts.ForceRescoreNodes, false)
	assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerAddReplicaFirst, false)
	assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerMaxOverflowReplicas, int32(0))
	data, exists, ok = te.service.zkConn.Get(
		context.Background(),
		te.service.zkScheduleOptionsPath,
	)
	assert.Assert(t, exists && ok)
	schedOpts = &pb.ScheduleOptions{}
	utils.UnmarshalJsonOrDie(data, schedOpts)
	assert.Assert(t, proto.Equal(schedOpts, queryServiceResp.SchedOpts))

	logging.Info("update hash_arranger_add_replica_first")
	ans = te.service.UpdateScheduleOptions(
		&pb.ScheduleOptions{
			EnablePrimaryScheduler:          true,
			MaxSchedRatio:                   500,
			Estimator:                       est.DEFAULT_ESTIMATOR,
			ForceRescoreNodes:               false,
			EnableSplitBalancer:             false,
			HashArrangerAddReplicaFirst:     true,
			HashArrangerMaxOverflowReplicas: 1,
		},
		[]string{"hash_arranger_add_replica_first"},
	)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	te.service.QueryService(queryServiceResp)
	assert.Equal(t, queryServiceResp.SchedOpts.EnablePrimaryScheduler, false)
	assert.Equal(t, queryServiceResp.SchedOpts.MaxSchedRatio, int32(200))
	assert.Equal(t, queryServiceResp.SchedOpts.EnableSplitBalancer, true)
	assert.Equal(t, queryServiceResp.SchedOpts.Estimator, est.DISK_CAP_ESTIMATOR)
	assert.Equal(t, queryServiceResp.SchedOpts.ForceRescoreNodes, false)
	assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerAddReplicaFirst, true)
	assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerMaxOverflowReplicas, int32(0))
	data, exists, ok = te.service.zkConn.Get(
		context.Background(),
		te.service.zkScheduleOptionsPath,
	)
	assert.Assert(t, exists && ok)
	schedOpts = &pb.ScheduleOptions{}
	utils.UnmarshalJsonOrDie(data, schedOpts)
	assert.Assert(t, proto.Equal(schedOpts, queryServiceResp.SchedOpts))

	logging.Info("update hash_arranger_max_overflow_replicas")
	ans = te.service.UpdateScheduleOptions(
		&pb.ScheduleOptions{
			EnablePrimaryScheduler:          true,
			MaxSchedRatio:                   500,
			Estimator:                       est.DEFAULT_ESTIMATOR,
			ForceRescoreNodes:               false,
			EnableSplitBalancer:             false,
			HashArrangerAddReplicaFirst:     false,
			HashArrangerMaxOverflowReplicas: 1,
		},
		[]string{"hash_arranger_max_overflow_replicas"},
	)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	te.service.QueryService(queryServiceResp)
	assert.Equal(t, queryServiceResp.SchedOpts.EnablePrimaryScheduler, false)
	assert.Equal(t, queryServiceResp.SchedOpts.MaxSchedRatio, int32(200))
	assert.Equal(t, queryServiceResp.SchedOpts.EnableSplitBalancer, true)
	assert.Equal(t, queryServiceResp.SchedOpts.Estimator, est.DISK_CAP_ESTIMATOR)
	assert.Equal(t, queryServiceResp.SchedOpts.ForceRescoreNodes, false)
	assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerAddReplicaFirst, true)
	assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerMaxOverflowReplicas, int32(1))
	data, exists, ok = te.service.zkConn.Get(
		context.Background(),
		te.service.zkScheduleOptionsPath,
	)
	assert.Assert(t, exists && ok)
	schedOpts = &pb.ScheduleOptions{}
	utils.UnmarshalJsonOrDie(data, schedOpts)
	assert.Assert(t, proto.Equal(schedOpts, queryServiceResp.SchedOpts))

	logging.Info("update max_learning_parts_per_node")
	ans = te.service.UpdateScheduleOptions(
		&pb.ScheduleOptions{
			EnablePrimaryScheduler:          true,
			MaxSchedRatio:                   500,
			Estimator:                       est.DEFAULT_ESTIMATOR,
			ForceRescoreNodes:               false,
			EnableSplitBalancer:             false,
			HashArrangerAddReplicaFirst:     false,
			HashArrangerMaxOverflowReplicas: 10,
			MaxLearningPartsPerNode:         20,
		},
		[]string{"max_learning_parts_per_node"},
	)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	te.service.QueryService(queryServiceResp)
	assert.Equal(t, queryServiceResp.SchedOpts.EnablePrimaryScheduler, false)
	assert.Equal(t, queryServiceResp.SchedOpts.MaxSchedRatio, int32(200))
	assert.Equal(t, queryServiceResp.SchedOpts.EnableSplitBalancer, true)
	assert.Equal(t, queryServiceResp.SchedOpts.Estimator, est.DISK_CAP_ESTIMATOR)
	assert.Equal(t, queryServiceResp.SchedOpts.ForceRescoreNodes, false)
	assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerAddReplicaFirst, true)
	assert.Equal(t, queryServiceResp.SchedOpts.HashArrangerMaxOverflowReplicas, int32(1))
	assert.Equal(t, queryServiceResp.SchedOpts.MaxLearningPartsPerNode, int32(20))
	data, exists, ok = te.service.zkConn.Get(
		context.Background(),
		te.service.zkScheduleOptionsPath,
	)
	assert.Assert(t, exists && ok)
	schedOpts = &pb.ScheduleOptions{}
	utils.UnmarshalJsonOrDie(data, schedOpts)
	assert.Assert(t, proto.Equal(schedOpts, queryServiceResp.SchedOpts))
}

func TestUpdateNodeScoreEstimator(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
	}

	logging.Info(
		"first service create with default estimator, so scores are equal and scheduler works properly",
	)
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_rodis}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	assert.Equal(t, te.service.nodes.MustGetNodeInfo("node1").Score, est.ScoreType(100))
	assert.Equal(t, te.service.nodes.MustGetNodeInfo("node2").Score, est.ScoreType(100))
	te.TriggerScheduleAndCollect()

	table := te.service.tableNames["test"]
	rec := recorder.NewAllNodesRecorder(recorder.NewBriefNode)
	table.Record(rec)
	assert.Equal(t, table.GetEstimatedReplicasOnNode("node1"), 16)
	assert.Equal(t, table.GetEstimatedReplicasOnNode("node2"), 16)
	assert.Equal(t, rec.Count("node1", pb.ReplicaRole_kLearner), int(table.PartsCount/2))
	assert.Equal(t, rec.Count("node2", pb.ReplicaRole_kLearner), int(table.PartsCount/2))

	logging.Info("change estimator")
	err := te.service.UpdateScheduleOptions(
		&pb.ScheduleOptions{
			Estimator: est.DISK_CAP_ESTIMATOR,
		},
		[]string{"estimator"})
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))

	logging.Info("can't collect any server info, so scheduler won't work")
	te.TriggerScheduleAndWaitFinish()
	assert.Assert(te.t, te.service.tableNames["test"].StatsConsistency(false))
	rec = recorder.NewAllNodesRecorder(recorder.NewBriefNode)
	table.Record(rec)
	assert.Equal(t, rec.Count("node1", pb.ReplicaRole_kLearner), int(table.PartsCount/2))
	assert.Equal(t, rec.Count("node2", pb.ReplicaRole_kLearner), int(table.PartsCount/2))

	logging.Info("add server info")
	te.lpb.GetClient("127.0.0.1:1001").SetServerStatistics(map[string]string{
		pb.RodisServerStat_dbcap_bytes.String(): "214748364800",
	})
	te.lpb.GetClient("127.0.0.1:1002").SetServerStatistics(map[string]string{
		pb.RodisServerStat_dbcap_bytes.String(): "322122547200",
	})
	te.TriggerCollectAndWaitFinish()
	assert.Assert(t, te.service.nodes.MustGetNodeInfo("node1").GetResource() != nil)
	assert.Assert(t, te.service.nodes.MustGetNodeInfo("node2").GetResource() != nil)

	logging.Info("scheduler will run again, finally replicas won't be evenly distributed")
	for i := 0; i < 1000; i++ {
		te.TriggerScheduleAndCollect()
		rec := recorder.NewAllNodesRecorder(recorder.NewBriefNode)
		table.Record(rec)
		if rec.CountAll("node1") != table.GetEstimatedReplicasOnNode("node1") {
			continue
		}
		if rec.CountAll("node2") != table.GetEstimatedReplicasOnNode("node2") {
			continue
		}
		break
	}
	assert.Assert(t, table.GetEstimatedReplicasOnNode("node1") < 16)
	assert.Assert(t, table.GetEstimatedReplicasOnNode("node2") > 16)
}

func TestOnlyLearnerDead(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	p := table_model.PartitionMembership{
		MembershipVersion: 1,
		Peers: map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kLearner,
		},
	}

	te.initGivenMembership(&p)

	tbl := te.service.tableNames["test"]

	logging.Info("then mark a node dead")
	te.mockKess.RemoveNode("127.0.0.1:1001")
	te.TriggerDetectAndWaitFinish()

	deadNode := te.service.nodes.GetNodeInfoByAddrs([]*utils.RpcNode{nodes[0].Address}, true)[0]
	assert.Assert(t, deadNode != nil)
	assert.Assert(t, !deadNode.IsAlive)

	for i := range tbl.currParts {
		part := &(tbl.currParts[i])
		assert.Equal(t, part.members.MembershipVersion, int64(1))
		assert.Equal(t, part.facts["node1"].membershipVersion, int64(1))
		assert.Equal(t, part.facts["node1"].Role, pb.ReplicaRole_kLearner)
		assert.Equal(t, part.facts["node1"].ReadyToPromote, false)
	}
}

func TestLotsNodeDead(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
		{
			Id:  "node4",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1004"),
				ProcessId: "12343",
				BizPort:   2004,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	p := table_model.PartitionMembership{
		MembershipVersion: 5,
		Peers: map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kLearner,
			"node2": pb.ReplicaRole_kSecondary,
			"node3": pb.ReplicaRole_kSecondary,
		},
	}

	te.initGivenMembership(&p)

	tbl := te.service.tableNames["test"]

	logging.Info("then mark 3 nodes dead")
	te.mockKess.RemoveNode("127.0.0.1:1001")
	te.mockKess.RemoveNode("127.0.0.1:1002")
	te.mockKess.RemoveNode("127.0.0.1:1003")
	te.TriggerDetectAndWaitFinish()

	deadNodes := te.service.nodes.GetNodeInfoByAddrs(
		[]*utils.RpcNode{nodes[0].Address, nodes[1].Address, nodes[2].Address},
		true,
	)
	for i := 0; i < 3; i++ {
		assert.Assert(t, deadNodes[i] != nil)
		assert.Assert(t, !deadNodes[i].IsAlive)
	}

	for i := range tbl.currParts {
		part := &(tbl.currParts[i])
		assert.Equal(t, part.members.MembershipVersion, int64(7))
		assert.Equal(t, part.facts["node1"].membershipVersion, int64(7))
		assert.Equal(t, part.facts["node1"].Role, pb.ReplicaRole_kLearner)
		assert.Equal(t, part.facts["node1"].ReadyToPromote, false)

		assert.Equal(t, part.facts["node2"].membershipVersion, int64(7))
		assert.Equal(t, part.facts["node2"].Role, pb.ReplicaRole_kLearner)
		assert.Equal(t, part.facts["node2"].ReadyToPromote, false)

		assert.Equal(t, part.facts["node3"].membershipVersion, int64(7))
		assert.Equal(t, part.facts["node3"].ReadyToPromote, false)
		assert.Equal(t, part.facts["node3"].Role, pb.ReplicaRole_kLearner)
	}
}

func TestNodeDead(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
		{
			Id:  "node4",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1004"),
				ProcessId: "12343",
				BizPort:   2004,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	te.makeTestTableNormal()
	tbl := te.service.tableNames["test"]

	logging.Info("then mark a node dead")
	te.mockKess.RemoveNode("127.0.0.1:1001")
	te.TriggerDetectAndWaitFinish()

	deadNode := te.service.nodes.GetNodeInfoByAddrs([]*utils.RpcNode{nodes[0].Address}, true)[0]
	assert.Assert(t, deadNode != nil)
	assert.Assert(t, !deadNode.IsAlive)

	for i := range tbl.currParts {
		part := &(tbl.currParts[i])
		assert.Equal(t, part.members.MembershipVersion, int64(8))
		member, ok := part.members.Peers[deadNode.Id]
		assert.Assert(t, ok)
		assert.Equal(t, member, pb.ReplicaRole_kLearner)
		assert.Equal(t, part.facts[deadNode.Id].Role, pb.ReplicaRole_kLearner)
		assert.Equal(t, part.facts[deadNode.Id].ReadyToPromote, false)

		primaries, _, _ := part.members.DivideRoles()
		assert.Assert(t, cmp.Len(primaries, 0))
	}

	logging.Info("then start a round of collection, the alive nodes will got the newest state")
	te.TriggerCollectAndWaitFinish()

	logging.Info("then start a round of schedule, then primary will transfer to another node")
	te.TriggerScheduleAndCollect()

	for i := range tbl.currParts {
		part := &(tbl.currParts[i])
		assert.Equal(t, part.members.MembershipVersion, int64(9))

		primaries, _, _ := part.members.DivideRoles()
		assert.Assert(t, cmp.Len(primaries, 1))
	}

	logging.Info(
		"then start another round of schedule, half learners from node1 will transfer to node4",
	)
	te.TriggerScheduleAndCollect()
	te.TriggerScheduleAndCollect()

	transferredCount := 0
	for i := range tbl.currParts {
		part := &(tbl.currParts[i])
		if part.members.MembershipVersion == 9 {
			assert.Equal(t, part.members.HasMember("node1"), true)
			assert.Equal(t, part.members.HasMember("node4"), false)
		} else {
			transferredCount++
			assert.Equal(t, part.members.MembershipVersion, int64(11))
			assert.Equal(t, part.members.HasMember("node1"), false)
			assert.Equal(t, part.members.HasMember("node4"), true)
		}
	}
	assert.Equal(t, transferredCount, 16)

	logging.Info("then mark the dead node alive again")
	nodes[0].ProcessId = "22340"
	node_mgr.MockedKessUpdateNodePing(te.mockKess, nodes[0].Id, &(nodes[0].NodePing))
	te.TriggerDetectAndWaitFinish()

	deadNode = te.service.nodes.GetNodeInfoByAddrs([]*utils.RpcNode{nodes[0].Address}, true)[0]
	assert.Assert(t, deadNode.IsAlive)
	assert.Equal(t, deadNode.ProcessId, "22340")

	logging.Info("then start a round of collection, the old's dead state will be synced")
	te.TriggerCollectAndWaitFinish()

	logging.Info("then start a new round of schedule, the schedule will do sync")
	for _, client := range te.lpb.LocalClients {
		client.CleanMetrics()
	}
	te.TriggerScheduleAndWaitFinish()
	assert.Assert(t, te.lpb.GetClient(nodes[0].Address.String()).GetMetric("reconfig") > 0)
	assert.Equal(t, te.lpb.GetClient(nodes[1].Address.String()).GetMetric("reconfig"), 0)
	assert.Equal(t, te.lpb.GetClient(nodes[2].Address.String()).GetMetric("reconfig"), 0)

	logging.Info("then start a round of collection, node1's learner stats will be promotable")
	te.TriggerCollectAndWaitFinish()

	logging.Info(
		"then start a new round of schedule, the schedule will promote learner to secondary",
	)
	te.TriggerScheduleAndCollect()

	node1SecCount := 0
	for i := range tbl.currParts {
		part := &(tbl.currParts[i])
		member, ok := part.members.Peers[deadNode.Id]
		if ok {
			node1SecCount++
			assert.Equal(t, member, pb.ReplicaRole_kSecondary)
		}
	}
	assert.Equal(t, node1SecCount, 16)
}

func TestNodeRestarting(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	te.makeTestTableNormal()
	tbl := te.service.tableNames["test"]

	logging.Info("mark node1 need to be restarting")
	adminResp := &pb.AdminNodeResponse{}
	te.service.AdminNodes(
		[]*utils.RpcNode{utils.FromHostPort("127.0.0.1:1001")},
		true,
		pb.AdminNodeOp_kRestart,
		adminResp,
	)
	assert.Equal(t, adminResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, cmp.Len(adminResp.NodeResults, 1))
	assert.Equal(t, adminResp.NodeResults[0].Code, int32(pb.AdminError_kOk))

	logging.Info(
		"then trigger schedule, first node1 will be marked secondary, and other nodes be selected as primary",
	)
	te.TriggerScheduleAndWaitFinish()

	for i := range tbl.currParts {
		part := &(tbl.currParts[i].members)
		assert.Equal(t, part.Peers["node1"], pb.ReplicaRole_kSecondary)
		pri, _, _ := tbl.currParts[i].members.DivideRoles()
		assert.Assert(t, len(pri) > 0)
	}

	logging.Info("then trigger collection to make stats in consistency")
	assert.Assert(t, !tbl.StatsConsistency(false))
	te.TriggerCollectAndWaitFinish()
	assert.Assert(t, tbl.StatsConsistency(false))

	logging.Info("then trigger schedule again, node1 will be learner")
	te.TriggerScheduleAndCollect()
	for i := range tbl.currParts {
		part := &(tbl.currParts[i].members)
		assert.Equal(t, part.Peers["node1"], pb.ReplicaRole_kLearner)
	}

	logging.Info("then mark node1 back to normal again")
	te.service.AdminNodes(
		[]*utils.RpcNode{utils.FromHostPort("127.0.0.1:1001")},
		true,
		pb.AdminNodeOp_kNoop,
		adminResp,
	)
	assert.Equal(t, adminResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, cmp.Len(adminResp.NodeResults, 1))
	assert.Equal(t, adminResp.NodeResults[0].Code, int32(pb.AdminError_kOk))

	logging.Info("then trigger collection again, so that the learner will be marked promotable")
	te.TriggerCollectAndWaitFinish()
	assert.Assert(t, tbl.StatsConsistency(false))

	logging.Info("then start schedule, the restarting node will be mark to secondary again")
	te.TriggerScheduleAndWaitFinish()

	for i := range tbl.currParts {
		part := &(tbl.currParts[i].members)
		assert.Equal(t, part.Peers["node1"], pb.ReplicaRole_kSecondary)
	}

	logging.Info("then trigger collection to make stats in consistency")
	assert.Assert(t, !tbl.StatsConsistency(false))
	te.TriggerCollectAndWaitFinish()
	assert.Assert(t, tbl.StatsConsistency(false))
}

func TestNodeRemoving(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	te.makeTestTableNormal()
	tbl := te.service.tableNames["test"]

	logging.Info("mark node1 need to be removing")
	adminResp := &pb.AdminNodeResponse{}
	te.service.AdminNodes(
		[]*utils.RpcNode{utils.FromHostPort("127.0.0.1:1001")},
		true,
		pb.AdminNodeOp_kOffline,
		adminResp,
	)
	assert.Equal(t, adminResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, cmp.Len(adminResp.NodeResults, 1))
	assert.Equal(t, adminResp.NodeResults[0].Code, int32(pb.AdminError_kOk))

	logging.Info("issue schedule, can't find new node to transfer replicas out node1")
	for i := 0; ; i++ {
		logging.Info("schedule loop %d", i)
		te.TriggerScheduleAndWaitFinish()
		if len(tbl.plans) == 0 {
			break
		}
		te.TriggerCollectAndWaitFinish()
	}
	assert.Assert(t, tbl.StatsConsistency(false))
	for i := range tbl.currParts {
		part := &(tbl.currParts[i])
		role := part.members.GetMember("node1")
		assert.Assert(t, role == pb.ReplicaRole_kPrimary || role == pb.ReplicaRole_kSecondary)
	}

	logging.Info("add a new node")
	node4 := &node_mgr.NodeInfo{
		Id:  "node4",
		Op:  pb.AdminNodeOp_kNoop,
		Hub: "YZ1",
		NodePing: node_mgr.NodePing{
			IsAlive:   true,
			Az:        "YZ1",
			Address:   utils.FromHostPort("127.0.0.1:1004"),
			ProcessId: "12344",
			BizPort:   2004,
		},
	}
	node_mgr.MockedKessUpdateNodePing(te.mockKess, node4.Id, &(node4.NodePing))
	te.TriggerDetectAndWaitFinish()

	logging.Info("issue schedule, replicas will transfer from node1 to node4")
	for i := 0; ; i++ {
		logging.Info("schedule loop %d", i)
		te.TriggerScheduleAndWaitFinish()
		if len(tbl.plans) == 0 {
			break
		}
		te.TriggerCollectAndWaitFinish()
		for i := range tbl.currParts {
			part := &(tbl.currParts[i])
			pri, sec, _ := part.members.DivideRoles()
			assert.Assert(t, len(pri)+len(sec) >= 3)
		}
	}
	client4 := te.lpb.GetClient("127.0.0.1:1004")
	assert.Assert(t, client4.GetMetric("add_replica") > 0)
	replicas := client4.KeptReplicas()
	for _, req := range replicas {
		assert.Equal(t, req.EstimatedReplicas, tbl.PartsCount)
	}

	logging.Info("after schedule plan, replicas on node1 will be removed totally")
	for i := range tbl.currParts {
		part := &(tbl.currParts[i])
		assert.Assert(t, !part.members.HasMember("node1"))
	}

	client1 := te.lpb.GetClient("127.0.0.1:1001")
	assert.Assert(t, client1.GetMetric("remove_replica") > 0)
	replicas = client1.KeptReplicas()
	assert.Assert(t, cmp.Len(replicas, 0))
	removed := client1.RemovedReplicas()
	assert.Assert(t, cmp.Len(removed, int(tbl.PartsCount)))
	for _, req := range removed {
		assert.Equal(t, req.EstimatedReplicas, int32(0))
	}
}

func TestShadedTableRelatedOperation(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	logging.Info("add shaded table with invalid base table")
	tablePb := &pb.Table{
		TableName:         "test_shade_table",
		HashMethod:        "crc32",
		PartsCount:        32,
		JsonArgs:          `{"a": "b", "c": "d", "btq_prefix": "dummy"}`,
		KconfPath:         "reco.rodisFea.partitionKeeperHDFSTest",
		ScheduleGrayscale: kScheduleGrayscaleMax + 1,
		BaseTable:         "not_exist_base",
	}
	err := te.service.AddTable(tablePb, "", "")
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("add shaded table with different partition count with base table")
	tablePb.PartsCount = 10
	tablePb.BaseTable = "test"
	err = te.service.AddTable(tablePb, "", "")
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("add shaded table succeed")
	tablePb.PartsCount = 32
	tablePb.BaseTable = "test"
	err = te.service.AddTable(tablePb, "", "")
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))

	baseTable := te.service.tableNames["test"]
	shadedTable := te.service.tableNames[tablePb.TableName]
	assert.Assert(t, shadedTable != nil)
	assert.Equal(t, shadedTable.baseTable, baseTable)

	logging.Info("split base table not allowed")
	err = te.service.SplitTable(
		&pb.SplitTableRequest{
			ServiceName:     "test",
			TableName:       "test",
			NewSplitVersion: 1,
			Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 1},
		},
	)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("split shaded table not allowed")
	err = te.service.SplitTable(
		&pb.SplitTableRequest{
			ServiceName:     "test",
			TableName:       tablePb.TableName,
			NewSplitVersion: 1,
			Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 1},
		},
	)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("delete base table not allowed")
	err = te.service.RemoveTable(baseTable.TableName, true, 0)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("reload service will init table by order")
	te.service.stop()
	te.service.LoadFromZookeeper()
	baseTable = te.service.tableNames["test"]
	shadedTable = te.service.tableNames["test_shade_table"]
	assert.Assert(t, baseTable.baseTable == nil)
	assert.Equal(t, shadedTable.baseTable, baseTable)
}

func TestGetScheduleOrder(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	tableNames := te.service.getTableScheduleOrder(map[string]string{"a": "", "b": "", "aa": ""})
	sort.Strings(tableNames)
	assert.DeepEqual(t, tableNames, []string{"a", "aa", "b"})

	tableNames = te.service.getTableScheduleOrder(
		map[string]string{"a": "b", "b": "c", "c": "d", "d": ""},
	)
	assert.DeepEqual(t, tableNames, []string{"d", "c", "b", "a"})

	tableNames = te.service.getTableScheduleOrder(
		map[string]string{"a": "base", "b": "base", "c": "base", "base": ""},
	)
	assert.Equal(t, tableNames[0], "base")

	tableNames = te.service.getTableScheduleOrder(map[string]string{"a": "b", "b": "a"})
	sort.Strings(tableNames)
	assert.DeepEqual(t, tableNames, []string{"a", "b"})
}

func TestUpdateHubs(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	te.makeTestTableNormal()
	tbl := te.service.tableNames["test"]

	logging.Info("update a hub which doesn't exist")
	ans := te.service.UpdateHubs([]*pb.ReplicaHub{
		{Name: "yz", Az: "YZ1"},
	})
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("update hubs succeed")
	ans = te.service.UpdateHubs([]*pb.ReplicaHub{
		{Name: "YZ1", Az: "YZ1", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kPrimary}},
	})
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	expectNewHub := []*pb.ReplicaHub{
		{Name: "YZ1", Az: "YZ1", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kPrimary}},
		{Name: "YZ2", Az: "YZ2"},
		{Name: "YZ3", Az: "YZ3"},
	}
	assert.Assert(t, utils.HubsEqual(te.service.properties.Hubs, expectNewHub))
	assert.Assert(t, utils.HubsEqual(tbl.hubs, expectNewHub))

	logging.Info("then check zk is synced")
	data, exist, succ := te.metaStore.Get(context.Background(), te.service.zkPath)
	assert.Assert(t, succ)
	assert.Assert(t, exist)

	props := serviceProps{}
	props.fromJson(data)
	assert.Assert(t, utils.HubsEqual(expectNewHub, props.Hubs))
}

func TestAddHubs(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ_1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
				NodeIndex: "1.0",
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ_2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
				NodeIndex: "2.0",
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ_3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
				NodeIndex: "3.0",
			},
		},
	}
	opts := &pb.CreateServiceRequest{
		ServiceType:   pb.ServiceType_colossusdb_dummy,
		StaticIndexed: true,
	}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	te.makeTestTableNormal()
	tbl := te.service.tableNames["test"]
	nd := te.kessDetector

	oldHub := []*pb.ReplicaHub{
		{Name: "YZ_1", Az: "YZ"},
		{Name: "YZ_2", Az: "YZ"},
		{Name: "YZ_3", Az: "YZ"},
	}
	oldHubMap := utils.MapHubs(oldHub)

	logging.Info("add an existing hub")
	ans := te.service.AddHubs([]*pb.ReplicaHub{
		{Name: "YZ_1", Az: "YZ"},
		{Name: "YZ_4", Az: "YZ"},
	}, false)
	te.service.lock.LockWrite()
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Assert(t, utils.HubsEqual(oldHub, te.service.properties.Hubs))
	assert.Assert(t, utils.TreemapEqual(oldHubMap.GetMap(), te.service.properties.hubmap.GetMap()))
	te.service.lock.UnlockWrite()

	logging.Info("add hub, hub name conflict")
	ans = te.service.AddHubs([]*pb.ReplicaHub{
		{Name: "YZ_4", Az: "YZ"},
		{Name: "YZ_4", Az: "YZ"},
	}, false)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
	te.service.lock.LockWrite()
	assert.Assert(t, utils.HubsEqual(oldHub, te.service.properties.Hubs))
	assert.Assert(t, utils.TreemapEqual(oldHubMap.GetMap(), te.service.properties.hubmap.GetMap()))
	te.service.lock.UnlockWrite()

	logging.Info("add hubs try to gather not allowed")
	added := []*pb.ReplicaHub{
		{Name: "YZ_4", Az: "YZ"},
		{Name: "YZ_5", Az: "YZ"},
	}
	ans = te.service.AddHubs(added, true)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
	te.service.lock.LockWrite()
	assert.Assert(t, utils.HubsEqual(oldHub, te.service.properties.Hubs))
	assert.Assert(t, utils.TreemapEqual(oldHubMap.GetMap(), te.service.properties.hubmap.GetMap()))
	te.service.lock.UnlockWrite()

	logging.Info("add hub succeed")
	ans = te.service.AddHubs(added, false)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	newHub := append(oldHub, added...)
	newHubMap := utils.MapHubs(newHub)

	logging.Info("then table & detector hubs will be updated")
	te.service.lock.LockWrite()
	assert.Equal(t, te.service.properties.AdjustHubs, false)
	assert.Assert(t, utils.HubsEqual(newHub, te.service.properties.Hubs))
	assert.Assert(t, utils.TreemapEqual(newHubMap.GetMap(), te.service.properties.hubmap.GetMap()))

	assert.Assert(t, utils.HubsEqual(tbl.hubs, newHub))
	assert.Assert(t, utils.HubsEqual(nd.GetHubs(), newHubMap.ListHubs()))
	te.service.lock.UnlockWrite()

	logging.Info("then check zk is synced")
	data, exist, succ := te.metaStore.Get(context.Background(), te.service.zkPath)
	assert.Assert(t, succ)
	assert.Assert(t, exist)

	props := serviceProps{}
	props.fromJson(data)
	assert.Equal(t, props.AdjustHubs, false)
	assert.Assert(t, utils.HubsEqual(newHub, props.Hubs))
	assert.Assert(t, utils.TreemapEqual(newHubMap.GetMap(), props.hubmap.GetMap()))
}

func TestListAndQueryNodesHasNodeIndex(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ_1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
				NodeIndex: "1.0",
			},
		},
	}
	opts := &pb.CreateServiceRequest{
		ServiceType:   pb.ServiceType_colossusdb_dummy,
		StaticIndexed: true,
	}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	logging.Info("list nodes will have node index")
	listReq := &pb.ListNodesRequest{
		ServiceName: "test",
	}
	resp := &pb.ListNodesResponse{}
	te.service.ListNodes(listReq, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, cmp.Len(resp.Nodes, 1))
	assert.Equal(t, resp.Nodes[0].NodeIndex, "1.0")

	logging.Info("query nodes will have node index")
	queryReq := &pb.QueryNodeInfoRequest{
		NodeName:  "127.0.0.1",
		Port:      1001,
		TableName: "",
		OnlyBrief: false,
		MatchPort: true,
	}
	queryResp := &pb.QueryNodeInfoResponse{}
	te.service.QueryNodeInfo(queryReq, queryResp)
	assert.Equal(t, queryResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryResp.Brief.NodeIndex, "1.0")
}

func TestListAndQueryNodes(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "yz1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "yz1",
			NodePing: node_mgr.NodePing{
				IsAlive:   false,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kRestart,
			Hub: "yz1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.3:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node4",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "zw1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "ZW",
				Address:   utils.FromHostPort("127.0.0.4:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node5",
			Op:  pb.AdminNodeOp_kOffline,
			Hub: "zw1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "ZW",
				Address:   utils.FromHostPort("127.0.0.5:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node6",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "zw1",
			NodePing: node_mgr.NodePing{
				IsAlive:   false,
				Az:        "ZW",
				Address:   utils.FromHostPort("127.0.0.6:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
	}

	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	p := table_model.PartitionMembership{
		MembershipVersion: 7,
		Peers: map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kPrimary,
			"node4": pb.ReplicaRole_kSecondary,
		},
	}
	te.initGivenMembership(&p)

	logging.Info("test list nodes with invalid az")
	listReq := &pb.ListNodesRequest{
		ServiceName: "test",
		Az:          "GZ",
		HubName:     "",
	}
	resp := &pb.ListNodesResponse{}
	te.service.ListNodes(listReq, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("test list nodes with invalid hub name")
	resp = &pb.ListNodesResponse{}
	listReq.Az = "YZ"
	listReq.HubName = "zs1"
	te.service.ListNodes(listReq, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("list nodes will ignore hub if az is given")
	resp = &pb.ListNodesResponse{}
	listReq.Az = "YZ"
	listReq.HubName = "zw1"
	te.service.ListNodes(listReq, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, cmp.Len(resp.Nodes, 3))

	for _, brief := range resp.Nodes {
		info := te.service.nodes.GetNodeInfoByAddr(utils.FromPb(brief.Node), true)
		assert.Assert(t, info != nil)
		assert.Equal(t, brief.NodeUniqId, info.Id)
		assert.Equal(t, brief.HubName, info.Hub)
		assert.Equal(t, info.Az, "YZ")
		assert.Equal(t, info.IsAlive, brief.IsAlive)
		assert.Equal(t, info.Op, brief.Op)
		assert.Equal(t, int32(info.Score), brief.Score)
		assert.Equal(t, info.Weight, brief.Weight)
	}

	logging.Info("test list nodes with given hub")
	resp = &pb.ListNodesResponse{}
	listReq.Az = ""
	listReq.HubName = "zw1"
	te.service.ListNodes(listReq, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, cmp.Len(resp.Nodes, 3))

	for _, brief := range resp.Nodes {
		info := te.service.nodes.GetNodeInfoByAddr(utils.FromPb(brief.Node), true)
		assert.Assert(t, info != nil)
		assert.Equal(t, brief.NodeUniqId, info.Id)
		assert.Equal(t, brief.HubName, info.Hub)
		assert.Equal(t, info.Az, "ZW")
		assert.Equal(t, info.IsAlive, brief.IsAlive)
		assert.Equal(t, info.Op, brief.Op)
		assert.Equal(t, int32(info.Score), brief.Score)
		assert.Equal(t, info.Weight, brief.Weight)
	}

	logging.Info("list nodes with no filter")
	resp = &pb.ListNodesResponse{}
	listReq.Az = ""
	listReq.HubName = ""
	te.service.ListNodes(listReq, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, cmp.Len(resp.Nodes, 6))

	for _, brief := range resp.Nodes {
		info := te.service.nodes.GetNodeInfoByAddr(utils.FromPb(brief.Node), true)
		assert.Assert(t, info != nil)
		assert.Equal(t, brief.NodeUniqId, info.Id)
		assert.Equal(t, brief.HubName, info.Hub)
		assert.Equal(t, info.IsAlive, brief.IsAlive)
		assert.Equal(t, info.Op, brief.Op)
		assert.Equal(t, int32(info.Score), brief.Score)
		assert.Equal(t, info.Weight, brief.Weight)

		if info.Id == "node1" {
			assert.Equal(t, brief.PrimaryCount, int32(32))
		} else {
			assert.Equal(t, brief.PrimaryCount, int32(0))
		}

		if info.Id == "node4" {
			assert.Equal(t, brief.SecondaryCount, int32(32))
		} else {
			assert.Equal(t, brief.SecondaryCount, int32(0))
		}

		assert.Equal(t, brief.LearnerCount, int32(0))
	}

	logging.Info("test list nodes with unknown table")
	resp = &pb.ListNodesResponse{}
	listReq.Az = ""
	listReq.HubName = ""
	listReq.TableName = "not-exist_table"
	te.service.ListNodes(listReq, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, cmp.Len(resp.Nodes, 6))

	for _, brief := range resp.Nodes {
		info := te.service.nodes.GetNodeInfoByAddr(utils.FromPb(brief.Node), true)
		assert.Assert(t, info != nil)
		assert.Equal(t, brief.NodeUniqId, info.Id)
		assert.Equal(t, brief.HubName, info.Hub)
		assert.Equal(t, info.IsAlive, brief.IsAlive)
		assert.Equal(t, info.Op, brief.Op)
		assert.Equal(t, int32(info.Score), brief.Score)
		assert.Equal(t, info.Weight, brief.Weight)

		assert.Equal(t, brief.PrimaryCount, int32(0))
		assert.Equal(t, brief.SecondaryCount, int32(0))
		assert.Equal(t, brief.LearnerCount, int32(0))
	}

	logging.Info("test query node info without table filter")
	info := te.service.nodes.MustGetNodeInfo("node1")
	info.SetResource("test", utils.HardwareUnit{utils.DISK_CAP: 1023})
	queryNodeReq := &pb.QueryNodeInfoRequest{
		NodeName:  "127.0.0.1",
		Port:      1001,
		TableName: "",
		OnlyBrief: false,
		MatchPort: true,
	}
	queryNodeResp := &pb.QueryNodeInfoResponse{}
	te.service.QueryNodeInfo(queryNodeReq, queryNodeResp)
	assert.Equal(t, queryNodeResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryNodeResp.Brief.NodeUniqId, "node1")
	assert.Equal(t, queryNodeResp.Brief.Weight, info.Weight)
	assert.Equal(t, queryNodeResp.Brief.Score, int32(info.Score))
	assert.DeepEqual(t, queryNodeResp.Resource, map[string]int64(info.GetResource()))
	assert.Equal(t, queryNodeResp.Brief.PrimaryCount, int32(32))
	assert.Equal(t, queryNodeResp.Brief.SecondaryCount, int32(0))
	assert.Equal(t, queryNodeResp.Brief.LearnerCount, int32(0))
	assert.Equal(t, len(queryNodeResp.Replicas), 32)

	logging.Info("test query node info with only brief flag")
	queryNodeReq.OnlyBrief = true
	queryNodeResp = &pb.QueryNodeInfoResponse{}
	te.service.QueryNodeInfo(queryNodeReq, queryNodeResp)
	assert.Equal(t, queryNodeResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryNodeResp.Brief.NodeUniqId, "node1")
	assert.Equal(t, queryNodeResp.Brief.Weight, info.Weight)
	assert.Equal(t, queryNodeResp.Brief.Score, int32(info.Score))
	assert.DeepEqual(t, queryNodeResp.Resource, map[string]int64(info.GetResource()))
	assert.Equal(t, queryNodeResp.Brief.PrimaryCount, int32(32))
	assert.Equal(t, queryNodeResp.Brief.SecondaryCount, int32(0))
	assert.Equal(t, queryNodeResp.Brief.LearnerCount, int32(0))
	assert.Equal(t, len(queryNodeResp.Replicas), 0)

	logging.Info("test query node info with un-exist table")
	queryNodeReq.TableName = "not-exist-table"
	queryNodeReq.OnlyBrief = false
	queryNodeResp = &pb.QueryNodeInfoResponse{}
	te.service.QueryNodeInfo(queryNodeReq, queryNodeResp)
	assert.Equal(t, queryNodeResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(queryNodeResp.Replicas), 0)

	logging.Info("test query nodes info without table filter")
	info1 := te.service.nodes.MustGetNodeInfo("node1")
	info1.SetResource("test", utils.HardwareUnit{utils.DISK_CAP: 1023})
	info2 := te.service.nodes.MustGetNodeInfo("node4")
	info2.SetResource("test", utils.HardwareUnit{utils.DISK_CAP: 5555})
	queryNodesReq := &pb.QueryNodesInfoRequest{
		Nodes: []*pb.RpcNode{
			{NodeName: "127.0.0.1", Port: 1001},
			{NodeName: "127.0.0.4", Port: 1001},
		},
		TableName: "",
		OnlyBrief: false,
		MatchPort: true,
	}
	queryNodesResp := &pb.QueryNodesInfoResponse{}
	te.service.QueryNodesInfo(queryNodesReq, queryNodesResp)
	assert.Equal(t, queryNodesResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryNodesResp.Nodes[0].Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.NodeUniqId, "node1")
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.Weight, info1.Weight)
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.Score, int32(info1.Score))
	assert.DeepEqual(t, queryNodesResp.Nodes[0].Resource, map[string]int64(info1.GetResource()))
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.PrimaryCount, int32(32))
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.SecondaryCount, int32(0))
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.LearnerCount, int32(0))
	assert.Assert(t, cmp.Len(queryNodesResp.Nodes[0].Replicas, 32))
	for _, rep := range queryNodesResp.Nodes[0].Replicas {
		assert.Equal(t, rep.TableName, "test")
		assert.Equal(t, rep.Role, pb.ReplicaRole_kPrimary)
	}

	assert.Equal(t, queryNodesResp.Nodes[1].Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.NodeUniqId, "node4")
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.Weight, info2.Weight)
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.Score, int32(info2.Score))
	assert.DeepEqual(t, queryNodesResp.Nodes[1].Resource, map[string]int64(info2.GetResource()))
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.PrimaryCount, int32(0))
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.SecondaryCount, int32(32))
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.LearnerCount, int32(0))
	assert.Assert(t, cmp.Len(queryNodesResp.Nodes[1].Replicas, 32))
	for _, rep := range queryNodesResp.Nodes[1].Replicas {
		assert.Equal(t, rep.TableName, "test")
		assert.Equal(t, rep.Role, pb.ReplicaRole_kSecondary)
	}

	logging.Info("test query nodes info with only brief flag")
	queryNodesReq = &pb.QueryNodesInfoRequest{
		Nodes: []*pb.RpcNode{
			{NodeName: "127.0.0.1", Port: 1001},
			{NodeName: "127.0.0.4", Port: 1001},
		},
		TableName: "",
		OnlyBrief: true,
		MatchPort: true,
	}
	queryNodesResp = &pb.QueryNodesInfoResponse{}
	te.service.QueryNodesInfo(queryNodesReq, queryNodesResp)
	assert.Equal(t, queryNodesResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryNodesResp.Nodes[0].Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.NodeUniqId, "node1")
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.Weight, info1.Weight)
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.Score, int32(info1.Score))
	assert.DeepEqual(t, queryNodesResp.Nodes[0].Resource, map[string]int64(info1.GetResource()))
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.PrimaryCount, int32(32))
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.SecondaryCount, int32(0))
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.LearnerCount, int32(0))
	assert.Assert(t, cmp.Len(queryNodesResp.Nodes[0].Replicas, 0))

	assert.Equal(t, queryNodesResp.Nodes[1].Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.NodeUniqId, "node4")
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.Weight, info2.Weight)
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.Score, int32(info2.Score))
	assert.DeepEqual(t, queryNodesResp.Nodes[1].Resource, map[string]int64(info2.GetResource()))
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.PrimaryCount, int32(0))
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.SecondaryCount, int32(32))
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.LearnerCount, int32(0))
	assert.Assert(t, cmp.Len(queryNodesResp.Nodes[1].Replicas, 0))

	logging.Info("test query nodes info with un-exist table")
	queryNodesReq = &pb.QueryNodesInfoRequest{
		Nodes: []*pb.RpcNode{
			{NodeName: "127.0.0.1", Port: 1001},
			{NodeName: "127.0.0.4", Port: 1001},
		},
		TableName: "not-exist-table",
		OnlyBrief: false,
		MatchPort: true,
	}
	queryNodesResp = &pb.QueryNodesInfoResponse{}
	te.service.QueryNodesInfo(queryNodesReq, queryNodesResp)
	assert.Equal(t, queryNodesResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryNodesResp.Nodes[0].Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.NodeUniqId, "node1")
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.Weight, info1.Weight)
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.Score, int32(info1.Score))
	assert.DeepEqual(t, queryNodesResp.Nodes[0].Resource, map[string]int64(info1.GetResource()))
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.PrimaryCount, int32(0))
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.SecondaryCount, int32(0))
	assert.Equal(t, queryNodesResp.Nodes[0].Brief.LearnerCount, int32(0))
	assert.Assert(t, cmp.Len(queryNodesResp.Nodes[0].Replicas, 0))

	assert.Equal(t, queryNodesResp.Nodes[1].Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.NodeUniqId, "node4")
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.Weight, info2.Weight)
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.Score, int32(info2.Score))
	assert.DeepEqual(t, queryNodesResp.Nodes[1].Resource, map[string]int64(info2.GetResource()))
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.PrimaryCount, int32(0))
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.SecondaryCount, int32(0))
	assert.Equal(t, queryNodesResp.Nodes[1].Brief.LearnerCount, int32(0))
	assert.Assert(t, cmp.Len(queryNodesResp.Nodes[1].Replicas, 0))
}

func TestAddHubsRebalance(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "yz1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "yz1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "yz1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.3:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node4",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "yz2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.4:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node5",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "yz2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.5:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node6",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "yz2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.6:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
	}

	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()
	tbl := te.service.tableNames["test"]

	for i := range tbl.currParts {
		index := i % 3
		part := &(tbl.currParts[i])
		part.initialize(tbl, int32(i))
		part.newMembers.MembershipVersion = 10
		part.newMembers.Peers = map[string]pb.ReplicaRole{
			nodes[index].Id:   pb.ReplicaRole_kPrimary,
			nodes[index+3].Id: pb.ReplicaRole_kSecondary,
		}
		part.durableNewMembership()
	}
	te.reloadUpdatedMembership()

	for _, node := range nodes {
		client := te.lpb.GetClient(node.Address.String())
		assert.Equal(t, len(client.RemovedReplicas()), 0)
		for _, rep := range client.KeptReplicas() {
			assert.Equal(t, int(rep.EstimatedReplicas), tbl.GetEstimatedReplicasOnNode(node.Id))
		}
		client.CleanEstimatedReplicas()
	}

	tbl = te.service.tableNames["test"]

	logging.Info("add hub succeed")
	added := []*pb.ReplicaHub{
		{Name: "yz3", Az: "YZ"},
	}
	ans := te.service.AddHubs(added, true)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	newHub := []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
		{Name: "yz2", Az: "YZ"},
		{Name: "yz3", Az: "YZ"},
	}
	newHubMap := utils.MapHubs(newHub)

	te.service.lock.LockWrite()
	assert.Equal(t, te.service.properties.AdjustHubs, false)
	assert.Assert(t, utils.HubsEqual(newHub, te.service.properties.Hubs))
	assert.Assert(t, utils.TreemapEqual(newHubMap.GetMap(), te.service.properties.hubmap.GetMap()))
	te.service.lock.UnlockWrite()

	logging.Info("then check zk is synced")
	data, exist, succ := te.metaStore.Get(context.Background(), te.service.zkPath)
	assert.Assert(t, succ)
	assert.Assert(t, exist)

	props := serviceProps{}
	props.fromJson(data)
	assert.Equal(t, props.AdjustHubs, false)
	assert.Assert(t, utils.HubsEqual(newHub, props.Hubs))
	assert.Assert(t, utils.TreemapEqual(newHubMap.GetMap(), props.hubmap.GetMap()))

	logging.Info("then check scheduler behaviors")
	listNodeReq := &pb.ListNodesRequest{
		ServiceName: "test",
		Az:          "",
		HubName:     "yz3",
	}
	listNodesResp := &pb.ListNodesResponse{}
	te.service.ListNodes(listNodeReq, listNodesResp)
	assert.Equal(t, listNodesResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(listNodesResp.Nodes), 2)

	versions := make([]int64, tbl.PartsCount)
	for i := range versions {
		versions[i] = tbl.currParts[i].members.MembershipVersion
	}
	i := 0
	for i < 1000 {
		logging.Info("start to schedule for round %d", i)
		te.TriggerScheduleAndWaitFinish()

		versionUpdated := false
		for i := range versions {
			part := &(tbl.currParts[i])
			if versions[i] < part.members.MembershipVersion {
				versions[i] = part.members.MembershipVersion
				versionUpdated = true
			}
			assert.Assert(t, len(part.members.Peers) >= 2)
		}

		if !versionUpdated {
			break
		}

		te.TriggerCollectAndWaitFinish()
	}

	assert.Assert(t, i < 1000)
	for i := range tbl.currParts {
		part := &(tbl.currParts[i].members)
		assert.Equal(t, len(part.Peers), 3)
		hubCount := map[string]int{}
		for node := range part.Peers {
			hubCount[te.service.nodes.MustGetNodeInfo(node).Hub]++
		}
		assert.Equal(t, len(hubCount), 3)
	}

	for _, node := range nodes {
		assert.Equal(t, tbl.GetEstimatedReplicasOnNode(node.Id), 16)
		client := te.lpb.GetClient(node.Address.String())
		finalStateCount := 0
		for _, rep := range client.KeptReplicas() {
			if rep.EstimatedReplicas == 16 {
				finalStateCount++
			}
		}
		for _, rep := range client.RemovedReplicas() {
			if rep.EstimatedReplicas == 16 {
				finalStateCount++
			}
		}
		assert.Assert(t, finalStateCount > 0)
	}
}

func TestRemoveHubs(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ_1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
				NodeIndex: "1.0",
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ_2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
				NodeIndex: "2.0",
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ_3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
				NodeIndex: "3.0",
			},
		},
		{
			Id:  "node4",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ_1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1004"),
				ProcessId: "12343",
				BizPort:   2004,
				NodeIndex: "1.1",
			},
		},
		{
			Id:  "node5",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ_2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1005"),
				ProcessId: "12344",
				BizPort:   2005,
				NodeIndex: "2.1",
			},
		},
		{
			Id:  "node6",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ_3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1006"),
				ProcessId: "12345",
				BizPort:   2006,
				NodeIndex: "3.1",
			},
		},
	}
	opts := &pb.CreateServiceRequest{
		ServiceType:   pb.ServiceType_colossusdb_dummy,
		StaticIndexed: true,
	}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	te.makeTestTableNormal()
	tbl := te.service.tableNames["test"]
	nd := te.kessDetector

	oldHub := []*pb.ReplicaHub{
		{Name: "YZ_1", Az: "YZ"},
		{Name: "YZ_2", Az: "YZ"},
		{Name: "YZ_3", Az: "YZ"},
	}
	oldHubMap := utils.MapHubs(oldHub)

	logging.Info("remove an not-existing hub")
	ans := te.service.RemoveHubs([]*pb.ReplicaHub{
		{Name: "YZ_1", Az: "YZ"},
		{Name: "YZ_4", Az: "YZ"},
	}, false)
	te.service.lock.LockWrite()
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Assert(t, utils.HubsEqual(oldHub, te.service.properties.Hubs))
	assert.Assert(t, utils.TreemapEqual(oldHubMap.GetMap(), te.service.properties.hubmap.GetMap()))
	te.service.lock.UnlockWrite()

	logging.Info("remove hub, hub name conflict")
	ans = te.service.RemoveHubs([]*pb.ReplicaHub{
		{Name: "YZ_1", Az: "YZ"},
		{Name: "YZ_1", Az: "YZ"},
	}, false)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
	te.service.lock.LockWrite()
	assert.Assert(t, utils.HubsEqual(oldHub, te.service.properties.Hubs))
	assert.Assert(t, utils.TreemapEqual(oldHubMap.GetMap(), te.service.properties.hubmap.GetMap()))
	te.service.lock.UnlockWrite()

	logging.Info("remove hub try to scatter")
	removed := []*pb.ReplicaHub{
		{Name: "YZ_1", Az: "YZ"},
	}
	ans = te.service.RemoveHubs(removed, true)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
	te.service.lock.LockWrite()
	assert.Assert(t, utils.HubsEqual(oldHub, te.service.properties.Hubs))
	assert.Assert(t, utils.TreemapEqual(oldHubMap.GetMap(), te.service.properties.hubmap.GetMap()))
	te.service.lock.UnlockWrite()

	logging.Info("remove hub succeed")
	ans = te.service.RemoveHubs(removed, false)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	newHub := oldHub[1:]
	newHubMap := utils.MapHubs(newHub)

	logging.Info("then table & detector hubs will be updated, and nodes will be marked removing")
	te.service.lock.LockWrite()

	assert.Equal(t, te.service.properties.AdjustHubs, false)
	assert.Assert(t, utils.HubsEqual(newHub, te.service.properties.Hubs))
	assert.Assert(t, utils.TreemapEqual(newHubMap.GetMap(), te.service.properties.hubmap.GetMap()))

	assert.Assert(t, utils.HubsEqual(tbl.hubs, newHub))
	assert.Assert(t, utils.HubsEqual(nd.GetHubs(), newHub))

	nodes[0].Op = pb.AdminNodeOp_kOffline
	nodes[3].Op = pb.AdminNodeOp_kOffline
	for _, info := range nodes {
		n := te.service.nodes.MustGetNodeInfo(info.Id)
		assert.DeepEqual(t, n, info)
	}
	te.service.lock.UnlockWrite()

	logging.Info("then check zk is synced")
	data, exist, succ := te.metaStore.Get(context.Background(), te.service.zkPath)
	assert.Assert(t, succ)
	assert.Assert(t, exist)

	props := serviceProps{}
	props.fromJson(data)
	assert.Equal(t, props.AdjustHubs, false)
	assert.Assert(t, utils.HubsEqual(newHub, props.Hubs))
	assert.Assert(t, utils.TreemapEqual(newHubMap.GetMap(), props.hubmap.GetMap()))

	logging.Info("then nodes will be removed")

	i := 0
	for ; i < 1000; i++ {
		logging.Info("start to schedule for round %d", i)
		te.TriggerScheduleAndCollect()
		node1Req := &pb.QueryNodeInfoRequest{
			NodeName:  "127.0.0.1",
			Port:      1001,
			TableName: "",
			OnlyBrief: false,
			MatchPort: true,
		}
		node1Resp := &pb.QueryNodeInfoResponse{}
		te.service.QueryNodeInfo(node1Req, node1Resp)

		node4Req := &pb.QueryNodeInfoRequest{
			NodeName:  "127.0.0.1",
			Port:      1004,
			TableName: "",
			OnlyBrief: false,
			MatchPort: true,
		}
		node4Resp := &pb.QueryNodeInfoResponse{}
		te.service.QueryNodeInfo(node4Req, node4Resp)
		if node1Resp.Brief.ReplicaCount() == 0 && node4Resp.Brief.ReplicaCount() == 0 {
			break
		}
	}
	assert.Assert(t, i < 1000)

	logging.Info("then check max replicas count send to each node")
	client1 := te.lpb.GetClient("127.0.0.1:1001")
	assert.Assert(t, client1.GetMetric("remove_replica") > 0)
	replicas := client1.KeptReplicas()
	assert.Assert(t, cmp.Len(replicas, 0))
	removedReplicas := client1.RemovedReplicas()
	assert.Assert(t, cmp.Len(removedReplicas, int(tbl.PartsCount)))
	for _, r := range removedReplicas {
		assert.Equal(t, r.EstimatedReplicas, int32(0))
	}

	client4 := te.lpb.GetClient("127.0.0.1:1004")
	removedReplicas = client4.RemovedReplicas()
	assert.Assert(t, cmp.Len(removedReplicas, 0))
}

func TestRemoveHubsRebalance(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "yz1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "yz1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "yz2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.3:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node4",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "yz2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.4:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node5",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "yz3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.5:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
		{
			Id:  "node6",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "yz3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.6:1001"),
				ProcessId: "12341",
				BizPort:   2001,
			},
		},
	}

	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()
	tbl := te.service.tableNames["test"]

	hubNodes := te.service.nodes.DivideByHubs(true)
	hubIndex := []int{0, 0, 0}
	for i := range tbl.currParts {
		selectNodes := []string{}
		selectNodes = append(selectNodes, hubNodes["yz1"][hubIndex[0]].Id)
		hubIndex[0] = (hubIndex[0] + 1) % 2

		selectNodes = append(selectNodes, hubNodes["yz2"][hubIndex[1]].Id)
		hubIndex[1] = (hubIndex[1] + 1) % 2

		selectNodes = append(selectNodes, hubNodes["yz3"][hubIndex[2]].Id)
		hubIndex[2] = (hubIndex[2] + 1) % 2
		primaryIndex := rand.Intn(3)

		part := &(tbl.currParts[i])
		part.initialize(tbl, int32(i))
		part.newMembers.MembershipVersion = 10
		part.newMembers.Peers = make(map[string]pb.ReplicaRole)
		for index, n := range selectNodes {
			if index == primaryIndex {
				part.newMembers.Peers[n] = pb.ReplicaRole_kPrimary
			} else {
				part.newMembers.Peers[n] = pb.ReplicaRole_kSecondary
			}
		}
		part.durableNewMembership()
	}
	te.reloadUpdatedMembership()

	for _, node := range nodes {
		client := te.lpb.GetClient(node.Address.String())
		assert.Equal(t, len(client.RemovedReplicas()), 0)
		for _, rep := range client.KeptReplicas() {
			assert.Equal(t, rep.EstimatedReplicas, int32(16))
		}
		client.CleanEstimatedReplicas()
	}

	tbl = te.service.tableNames["test"]

	logging.Info("remove hub succeed")
	removed := []*pb.ReplicaHub{
		{Name: "yz3", Az: "YZ"},
	}
	ans := te.service.RemoveHubs(removed, true)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	newHub := []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
		{Name: "yz2", Az: "YZ"},
	}
	newHubMap := utils.MapHubs(newHub)

	te.service.lock.LockWrite()
	assert.Equal(t, te.service.properties.AdjustHubs, false)
	assert.Assert(t, utils.HubsEqual(newHub, te.service.properties.Hubs))
	assert.Assert(t, utils.TreemapEqual(newHubMap.GetMap(), te.service.properties.hubmap.GetMap()))
	te.service.lock.UnlockWrite()

	logging.Info("then check zk is synced")
	data, exist, succ := te.metaStore.Get(context.Background(), te.service.zkPath)
	assert.Assert(t, succ)
	assert.Assert(t, exist)

	props := serviceProps{}
	props.fromJson(data)
	assert.Equal(t, props.AdjustHubs, false)
	assert.Assert(t, utils.HubsEqual(newHub, props.Hubs))
	assert.Assert(t, utils.TreemapEqual(newHubMap.GetMap(), props.hubmap.GetMap()))

	logging.Info("then check list nodes")
	listNodeReq := &pb.ListNodesRequest{}
	listNodesResp := &pb.ListNodesResponse{}
	te.service.ListNodes(listNodeReq, listNodesResp)
	assert.Equal(t, listNodesResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(listNodesResp.Nodes), 6)
	yz1Count := 0
	yz2Count := 0
	for _, node := range listNodesResp.Nodes {
		if node.HubName == "yz1" {
			yz1Count++
		} else if node.HubName == "yz2" {
			yz2Count++
		} else {
			assert.Assert(t, false)
		}
	}
	assert.Equal(t, yz1Count, 3)
	assert.Equal(t, yz2Count, 3)

	logging.Info("then check scheduler behaviors")

	versions := make([]int64, tbl.PartsCount)
	for i := range versions {
		versions[i] = tbl.currParts[i].members.MembershipVersion
	}
	i := 0
	for i < 1000 {
		logging.Info("start to schedule for round %d", i)
		te.TriggerScheduleAndWaitFinish()

		versionUpdated := false
		for i := range versions {
			part := &(tbl.currParts[i])
			if versions[i] < part.members.MembershipVersion {
				versions[i] = part.members.MembershipVersion
				versionUpdated = true
			}
			assert.Assert(t, len(part.members.Peers) >= 2)
		}

		if !versionUpdated {
			break
		}

		te.TriggerCollectAndWaitFinish()
	}

	assert.Assert(t, i < 1000)
	for i := range tbl.currParts {
		part := &(tbl.currParts[i].members)
		assert.Equal(t, len(part.Peers), 2)
		hubCount := map[string]int{}
		for node := range part.Peers {
			hubCount[te.service.nodes.MustGetNodeInfo(node).Hub]++
		}
		assert.Equal(t, len(hubCount), 2)
	}

	for _, node := range nodes {
		client := te.lpb.GetClient(node.Address.String())
		finalStateCount := 0
		for _, rep := range client.KeptReplicas() {
			if rep.EstimatedReplicas == 10 || rep.EstimatedReplicas == 11 {
				finalStateCount++
			}
		}
		for _, rep := range client.RemovedReplicas() {
			if rep.EstimatedReplicas == 10 || rep.EstimatedReplicas == 11 {
				finalStateCount++
			}
		}
		assert.Assert(t, finalStateCount > 0)
	}
}

func TestAddNodes(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	te.makeTestTableNormal()
	tbl := te.service.tableNames["test"]

	logging.Info("add some nodes")
	nodes = []*node_mgr.NodeInfo{
		{
			Id:  "node4",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1004"),
				ProcessId: "12343",
				BizPort:   2004,
			},
		},
		{
			Id:  "node5",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1005"),
				ProcessId: "12344",
				BizPort:   2005,
			},
		},
		{
			Id:  "node6",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1006"),
				ProcessId: "12345",
				BizPort:   2006,
			},
		},
	}
	for _, n := range nodes {
		node_mgr.MockedKessUpdateNodePing(te.mockKess, n.Id, &(n.NodePing))
	}
	te.TriggerDetectAndWaitFinish()

	logging.Info("mark node6 need to be removing")
	adminResp := &pb.AdminNodeResponse{}
	te.service.AdminNodes(
		[]*utils.RpcNode{utils.FromHostPort("127.0.0.1:1006")},
		true,
		pb.AdminNodeOp_kOffline,
		adminResp,
	)
	assert.Equal(t, adminResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, cmp.Len(adminResp.NodeResults, 1))
	assert.Equal(t, adminResp.NodeResults[0].Code, int32(pb.AdminError_kOk))

	i := 0
	for {
		logging.Info("start scheduler for round: %d, may generate or execute plan", i)
		te.TriggerScheduleAndWaitFinish()
		if tbl.plans == nil {
			break
		}
		assert.Assert(te.t, !te.service.tableNames["test"].StatsConsistency(false))
		te.TriggerCollectAndWaitFinish()
		assert.Assert(te.t, te.service.tableNames["test"].StatsConsistency(false))
		i++
	}

	te.TriggerCollectAndWaitFinish()
	listNodeReq := &pb.ListNodesRequest{}
	resp := &pb.ListNodesResponse{}
	te.service.ListNodes(listNodeReq, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, cmp.Len(resp.Nodes, 6))
	for _, brief := range resp.Nodes {
		if brief.Node.Port == 1006 {
			assert.Equal(t, brief.PrimaryCount, int32(0))
			assert.Equal(t, brief.SecondaryCount, int32(0))
			assert.Equal(t, brief.LearnerCount, int32(0))
			assert.Equal(t, brief.Op, pb.AdminNodeOp_kOffline)
		} else {
			assert.Assert(t, brief.PrimaryCount+brief.SecondaryCount > 0)
			assert.Equal(t, brief.LearnerCount, int32(0), "%s leaners are not 0", brief.Node.ToHostPort())
			assert.Equal(t, brief.Op, pb.AdminNodeOp_kNoop)
		}
	}

	for addr, client := range te.lpb.LocalClients {
		if addr == "127.0.0.1:1003" {
			assert.Equal(t, int32(client.GetMetric("add_replica")), tbl.PartsCount)
			assert.Equal(t, client.GetMetric("remove_replica"), 0)
		} else if addr == "127.0.0.1:1006" {
			assert.Equal(t, client.GetMetric("add_replica"), 0)
			assert.Equal(t, client.GetMetric("remove_replica"), 0)
		} else {
			if addr == "127.0.0.1:1001" || addr == "127.0.0.1:1002" {
				assert.Assert(t, client.GetMetric("remove_replica") > 0)
				removed := client.RemovedReplicas()
				assert.Assert(t, len(removed) > 0)
				for _, req := range removed {
					assert.Equal(t, req.EstimatedReplicas, int32(16))
				}
				assert.Assert(t, client.GetMetric("add_replica") > 0)
				kept := client.KeptReplicas()
				assert.Assert(t, len(kept) > 0)
				for _, req := range kept {
					assert.Equal(t, req.EstimatedReplicas, int32(32))
				}
			} else {
				assert.Equal(t, client.GetMetric("remove_replica"), 0)
				assert.Assert(t, client.GetMetric("add_replica") > 0)
				kept := client.KeptReplicas()
				assert.Assert(t, len(kept) > 0)
				for _, req := range kept {
					assert.Equal(t, req.EstimatedReplicas, int32(16))
				}
			}
		}
	}
}

func accessTableWhenNotNormal(t *testing.T, service *ServiceStat) {
	hubs := []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
	}
	result := service.AddHubs(hubs, true)
	assert.Equal(t, result.Code, int32(pb.AdminError_kServiceNotInServingState))

	result = service.RemoveHubs(hubs, true)
	assert.Equal(t, result.Code, int32(pb.AdminError_kServiceNotInServingState))

	result = service.UpdateHubs(hubs)
	assert.Equal(t, result.Code, int32(pb.AdminError_kServiceNotInServingState))

	result = service.UpdateScheduleOptions(
		&pb.ScheduleOptions{MaxSchedRatio: 10, EnablePrimaryScheduler: true},
		[]string{"enable_primary_scheduler", "max_sched_ratio"},
	)
	assert.Equal(t, result.Code, int32(pb.AdminError_kServiceNotInServingState))

	hints := map[string]*pb.NodeHints{
		"127.0.0.1:1001": {Hub: "yz1"},
	}
	result = service.GiveHints(hints, true)
	assert.Equal(t, result.Code, int32(pb.AdminError_kServiceNotInServingState))

	result = service.RecallHints([]*pb.RpcNode{utils.FromHostPort("127.0.0.1:1001").ToPb()}, true)
	assert.Equal(t, result.Code, int32(pb.AdminError_kServiceNotInServingState))

	shrinkReq := &pb.ShrinkAzRequest{
		Az:      "YZ",
		NewSize: 10,
	}
	shrinkResp := &pb.ShrinkAzResponse{}
	service.ShrinkAz(shrinkReq, shrinkResp)
	assert.Equal(t, shrinkResp.Status.Code, int32(pb.AdminError_kServiceNotInServingState))

	expandReq := &pb.ExpandAzsRequest{
		AzOptions: []*pb.ExpandAzsRequest_AzOption{
			{Az: "YZ", NewSize: 10},
		},
	}
	expandResp := service.ExpandAzs(expandReq)
	assert.Equal(t, expandResp.Code, int32(pb.AdminError_kServiceNotInServingState))

	cancelExpandReq := &pb.CancelExpandAzsRequest{
		Azs: []string{"YZ"},
	}
	cancelExpandResp := service.CancelExpandAzs(cancelExpandReq)
	assert.Equal(t, cancelExpandResp.Code, int32(pb.AdminError_kServiceNotInServingState))

	replaceReq := &pb.ReplaceNodesRequest{
		SrcNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.1", Port: 1001},
		},
		DstNodes: []*pb.RpcNode{
			{NodeName: "127.0.0.2", Port: 1001},
		},
	}
	replaceResp := service.ReplaceNodes(replaceReq)
	assert.Equal(t, replaceResp.Code, int32(pb.AdminError_kServiceNotInServingState))

	removeWatcherResp := service.RemoveWatcher(watcher.AzSizeWatcherName)
	assert.Equal(t, removeWatcherResp.Code, int32(pb.AdminError_kServiceNotInServingState))

	assignReq := &pb.AssignHubRequest{
		ServiceName: "",
		Node:        &pb.RpcNode{NodeName: "127.0.0.1", Port: 1001},
		Hub:         "YZ1",
	}
	assignResp := service.AssignHub(assignReq)
	assert.Equal(t, assignResp.Code, int32(pb.AdminError_kServiceNotInServingState))

	result = service.SwitchSchedulerStatus(true)
	assert.Equal(t, result.Code, int32(pb.AdminError_kServiceNotInServingState))

	result = service.SwitchKessPollerStatus(true)
	assert.Equal(t, result.Code, int32(pb.AdminError_kServiceNotInServingState))

	queryServiceResp := &pb.QueryServiceResponse{}
	service.QueryService(queryServiceResp)
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kServiceNotInServingState))

	table := &pb.Table{
		TableName: "test",
	}

	result = service.AddTable(table, "", "")
	assert.Equal(t, result.Code, int32(pb.AdminError_kServiceNotInServingState))

	result = service.UpdateTable(table)
	assert.Equal(t, result.Code, int32(pb.AdminError_kServiceNotInServingState))

	result = service.RemoveTable("test", false, 0)
	assert.Equal(t, result.Code, int32(pb.AdminError_kServiceNotInServingState))

	queryReq := &pb.QueryTableRequest{
		ServiceName:    "test",
		TableName:      "test",
		WithTasks:      true,
		WithPartitions: true,
	}
	queryResp := &pb.QueryTableResponse{}
	service.QueryTable(queryReq, queryResp)
	assert.Equal(t, queryResp.Status.Code, int32(pb.AdminError_kServiceNotInServingState))

	manualRemoveReq := &pb.ManualRemoveReplicasRequest{
		ServiceName: "test",
		TableName:   "test",
		Replicas: []*pb.ManualRemoveReplicasRequest_ReplicaItem{
			{
				Node:        &pb.RpcNode{NodeName: "127.0.0.1", Port: 1001},
				PartitionId: 10001,
			},
		},
	}
	manualRemoveResp := &pb.ManualRemoveReplicasResponse{}
	service.RemoveReplicas(manualRemoveReq, manualRemoveResp)
	assert.Equal(t, manualRemoveResp.Status.Code, int32(pb.AdminError_kServiceNotInServingState))

	listResp := &pb.ListTablesResponse{}
	service.ListTables(listResp)
	assert.Equal(t, queryResp.Status.Code, int32(pb.AdminError_kServiceNotInServingState))

	node := &utils.RpcNode{NodeName: "127.0.0.1", Port: 1234}

	adminResp := &pb.AdminNodeResponse{}
	service.AdminNodes([]*utils.RpcNode{node}, true, pb.AdminNodeOp_kNoop, adminResp)
	assert.Equal(t, queryResp.Status.Code, int32(pb.AdminError_kServiceNotInServingState))

	updateWeightResp := &pb.UpdateNodeWeightResponse{}
	service.UpdateNodeWeight(
		utils.FromHostPorts([]string{"127.0.0.1:2222"}),
		[]float32{1},
		updateWeightResp,
	)
	assert.Equal(t, updateWeightResp.Status.Code, int32(pb.AdminError_kServiceNotInServingState))

	queryNodeReq := &pb.QueryNodeInfoRequest{
		NodeName:  "127.0.0.1",
		Port:      1234,
		TableName: "",
		OnlyBrief: false,
		MatchPort: true,
	}
	queryNodeResp := &pb.QueryNodeInfoResponse{}
	service.QueryNodeInfo(queryNodeReq, queryNodeResp)
	assert.Equal(t, queryNodeResp.Status.Code, int32(pb.AdminError_kServiceNotInServingState))

	listNodeReq := &pb.ListNodesRequest{}
	listNodeResp := &pb.ListNodesResponse{}
	service.ListNodes(listNodeReq, listNodeResp)
	assert.Equal(t, queryNodeResp.Status.Code, int32(pb.AdminError_kServiceNotInServingState))

	task := &pb.PeriodicTask{}
	ans := service.UpdateTask("test", task)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kServiceNotInServingState))

	restoreReq := &pb.RestoreTableRequest{
		ServiceName: "test",
		TableName:   "test",
		RestorePath: "/tmp/checkpoint",
		Opts: &pb.RestoreOpts{
			MaxConcurrentNodesPerHub:  1,
			MaxConcurrentPartsPerNode: 1,
		},
	}
	ans = service.RestoreTable(restoreReq)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kServiceNotInServingState))

	splitReq := &pb.SplitTableRequest{
		ServiceName:     "test",
		TableName:       "test",
		NewSplitVersion: 1,
		Options: &pb.SplitTableOptions{
			MaxConcurrentParts: 1,
			DelaySeconds:       1,
		},
	}
	ans = service.SplitTable(splitReq)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kServiceNotInServingState))
}

func TestInvalidHubAz(t *testing.T) {
	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	zkStore := metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"},
		time.Second*10,
		acl,
		scheme,
		auth,
	)
	defer zkStore.Close()

	ans := zkStore.RecursiveDelete(context.Background(), serviceStatTestZkPrefix)
	assert.Assert(t, ans)
	ans = zkStore.RecursiveCreate(context.Background(), serviceStatTestZkPrefix)
	assert.Assert(t, ans)
	tablesManager := NewTablesManager(zkStore, serviceStatTestZkPrefix+"/tables")
	tablesManager.InitFromZookeeper()
	delayedExecutorManager := delay_execute.NewDelayedExecutorManager(
		zkStore,
		serviceStatTestZkPrefix+"/"+kDelayedExecutorNode,
	)
	delayedExecutorManager.InitFromZookeeper()

	// default create service will use cascade service discovery, which only allow valid hubs
	service1 := NewServiceStat(
		"test",
		"test",
		serviceStatTestZkPrefix,
		zkStore,
		tablesManager,
		delayedExecutorManager,
		make(chan<- string),
		WithRpcPoolBuilder(&rpc.LocalPSClientPoolBuilder{}),
	)

	createServiceReq := &pb.CreateServiceRequest{
		NodesHubs: []*pb.ReplicaHub{
			{Name: "yz", Az: "YZ"},
			{Name: "invalid", Az: "aa"},
		},
		ServiceType: pb.ServiceType_colossusdb_dummy,
	}
	status := service1.InitializeNew(createServiceReq)
	assert.Equal(t, status.Code, int32(pb.AdminError_kInvalidParameter))

	service2 := NewServiceStat(
		"test",
		"test",
		serviceStatTestZkPrefix,
		zkStore,
		tablesManager,
		delayedExecutorManager,
		make(chan<- string),
		WithRpcPoolBuilder(&rpc.LocalPSClientPoolBuilder{}),
	)

	hubs := []*pb.ReplicaHub{
		{Name: "yz", Az: "YZ"},
	}
	createServiceReq = &pb.CreateServiceRequest{
		NodesHubs:   hubs,
		ServiceType: pb.ServiceType_colossusdb_dummy,
	}
	status = service2.InitializeNew(createServiceReq)
	assert.Equal(t, status.Code, int32(pb.AdminError_kOk))
	status = service2.AddHubs([]*pb.ReplicaHub{
		{Name: "zw", Az: "ZW"},
		{Name: "invalid", Az: "aa"},
	}, true)
	assert.Equal(t, status.Code, int32(pb.AdminError_kInvalidParameter))

	queryResp := &pb.QueryServiceResponse{}
	service2.QueryService(queryResp)
	assert.Equal(t, queryResp.Status.Code, int32(pb.PartitionError_kOK))
	assert.Assert(t, utils.HubsEqual(queryResp.NodesHubs, hubs))
}

func TestAccessWhenStatNotReady(t *testing.T) {
	// first initialize zookeeper
	serviceName := "test"
	tableName := "test"
	hubs := []*pb.ReplicaHub{
		{Name: "YZ1", Az: "YZ"},
		{Name: "YZ2", Az: "YZ"},
		{Name: "YZ3", Az: "YZ"},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	metaStore := bootstrapService(t, opts, serviceName, tableName, 32, hubs, nil, "zk")
	defer metaStore.Close()

	tablesManager := NewTablesManager(metaStore, serviceStatTestZkPrefix+"/tables")
	tablesManager.InitFromZookeeper()
	delayedExecutorManager := delay_execute.NewDelayedExecutorManager(
		metaStore,
		serviceStatTestZkPrefix+"/"+kDelayedExecutorNode,
	)
	delayedExecutorManager.InitFromZookeeper()
	defer delayedExecutorManager.Stop()

	tdChan := make(chan string)
	service := NewServiceStat(
		serviceName,
		"test",
		serviceStatTestZkPrefix,
		metaStore,
		tablesManager,
		delayedExecutorManager,
		tdChan,
	)

	accessTableWhenNotNormal(t, service)

	service.LoadFromZookeeper()
	service.RemoveTable(tableName, false, 0)
	time.Sleep(time.Second * 2)
	service.Teardown()

	accessTableWhenNotNormal(t, service)
	name := <-tdChan
	assert.Equal(t, serviceName, name)
}

func TestHalfClosedCluster(t *testing.T) {
	// first initialize zookeeper
	serviceName := "test"
	tableName := "test"
	hubs := []*pb.ReplicaHub{
		{Name: "YZ1", Az: "YZ"},
		{Name: "YZ2", Az: "YZ"},
		{Name: "YZ3", Az: "YZ"},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	metaStore := bootstrapService(t, opts, serviceName, tableName, 32, hubs, nil, "zk")
	defer metaStore.Close()

	tablesManager := NewTablesManager(metaStore, serviceStatTestZkPrefix+"/tables")
	tablesManager.InitFromZookeeper()
	delayedExecutorManager := delay_execute.NewDelayedExecutorManager(
		metaStore,
		serviceStatTestZkPrefix+"/"+kDelayedExecutorNode,
	)
	delayedExecutorManager.InitFromZookeeper()
	defer delayedExecutorManager.Stop()

	tdChan := make(chan string, 1)
	service := NewServiceStat(
		serviceName,
		"test",
		serviceStatTestZkPrefix,
		metaStore,
		tablesManager,
		delayedExecutorManager,
		tdChan,
	)

	service.LoadFromZookeeper()
	service.properties.Dropped = true
	service.updateServiceProperties()
	service.stop()

	service.LoadFromZookeeper()
	assert.Equal(t, service.state.State, utils.StateDropped)
	name := <-tdChan
	assert.Equal(t, serviceName, name)

	logging.Info("check data cleaned from zk")
	_, exist, succ := metaStore.Get(context.Background(), service.zkPath)
	assert.Assert(t, succ)
	assert.Assert(t, !exist)
}

func TestCreateTableInvalidParameter(t *testing.T) {
	serviceName := "test"
	hubs := []*pb.ReplicaHub{
		{Name: "YZ1", Az: "YZ"},
		{Name: "YZ2", Az: "YZ"},
		{Name: "YZ3", Az: "YZ"},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_rodis}
	metaStore := bootstrapService(t, opts, serviceName, "", 32, hubs, nil, "zk")
	defer metaStore.Close()

	tablesManager := NewTablesManager(metaStore, serviceStatTestZkPrefix+"/tables")
	tablesManager.InitFromZookeeper()
	delayedExecutorManager := delay_execute.NewDelayedExecutorManager(
		metaStore,
		serviceStatTestZkPrefix+"/"+kDelayedExecutorNode,
	)
	delayedExecutorManager.InitFromZookeeper()
	defer delayedExecutorManager.Stop()

	tdChan := make(chan string, 1)
	service := NewServiceStat(
		serviceName,
		"test",
		serviceStatTestZkPrefix,
		metaStore,
		tablesManager,
		delayedExecutorManager,
		tdChan,
	)

	service.LoadFromZookeeper()

	tableName := "test"
	tablePb := &pb.Table{
		TableId:    0,
		TableName:  tableName,
		HashMethod: "crc32",
		PartsCount: 32,
		JsonArgs:   `{"a": "b", "c": "d"}`,
		KconfPath:  "reco.rodisFea.partitionKeeperHDFSTest",
	}

	status := service.AddTable(tablePb, "", "")
	assert.Equal(t, status.Code, int32(pb.AdminError_kInvalidParameter))

	resp := &pb.ListTablesResponse{}
	service.ListTables(resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, cmp.Len(resp.Tables, 0))
}

func TestScheduleCleanNodes(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ3",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
		{
			Id:  "node4",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.1:1004"),
				ProcessId: "12344",
				BizPort:   2004,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	te.makeTestTableNormal()

	adminResp := &pb.AdminNodeResponse{}
	te.service.AdminNodes(
		[]*utils.RpcNode{nodes[3].Address},
		true,
		pb.AdminNodeOp_kOffline,
		adminResp,
	)
	assert.Equal(t, adminResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, cmp.Len(adminResp.NodeResults, 1))
	assert.Equal(t, adminResp.NodeResults[0].Code, int32(pb.AdminError_kOk))

	node_mgr.MockedKessUpdateNodeId(te.mockKess, "127.0.0.1:1004", "node5")

	te.TriggerDetectAndWaitFinish()

	te.TriggerScheduleAndCollect()
	assert.Assert(t, te.service.nodes.GetNodeInfo("node4") == nil)
}

func TestServiceUpdateTask(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ3",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.3:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	te.makeTestTableNormal()

	task := &pb.PeriodicTask{
		TaskName: "invalid_task_name",
	}
	err := te.service.UpdateTask("invalid_table_name", task)
	assert.Equal(t, err.Code, int32(pb.AdminError_kTableNotExists))

	task.TaskName = checkpoint.TASK_NAME
	task.FirstTriggerUnixSeconds = 0
	task.PeriodSeconds = 0

	err = te.service.UpdateTask("test", task)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
}

func TestCreateTableUpdateTrafficFailed(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.3:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, false, "zk", 32})
	defer te.stop()

	tablePb := &pb.Table{
		TableId:           0,
		TableName:         "test",
		HashMethod:        "crc32",
		PartsCount:        4,
		JsonArgs:          `{"a": "b", "c": "d", "btq_prefix": "dummy"}`,
		KconfPath:         "reco.rodisFea.partitionKeeperHDFSTest",
		ScheduleGrayscale: kScheduleGrayscaleMax + 1,
	}
	err := te.service.AddTable(tablePb, "", "colossus.traffic.invalid_kconf_path")
	logging.Info("%v", err)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))
	existingTables := te.service.tablesManager.GetTablesByService("test")
	assert.Equal(t, len(existingTables), 0)
}

func TestCreateTableAuditResource(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.3:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_embedding_server}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, false, "zk", 32})
	defer te.stop()

	logging.Info("create will fail as server doesn't report resource")
	te.TriggerCollectAndWaitFinish()
	tablePb := &pb.Table{
		TableName:  "test",
		HashMethod: "crc32",
		PartsCount: 32,
		JsonArgs: `
		{
			"btq_prefix": "mio_embedding",
			"read_slots": "",
			"sign_format": "",
			"min_embeddings_required_per_part": 1099511627776,
			"table_num_keys": 10995116277760,
			"table_size_bytes": 512,
			"table_inflation_ratio": 1.5
		}
		`,
		KconfPath:         "reco.rodisFea.partitionKeeperHDFSTest",
		ScheduleGrayscale: kScheduleGrayscaleMax + 1,
	}
	err := te.service.AddTable(tablePb, "", "")
	assert.Equal(t, err.Code, int32(pb.AdminError_kNotEnoughResource))
	assert.Equal(t, len(te.service.tableNames), 0)

	logging.Info("make server to report resource")
	for _, node := range nodes {
		te.lpb.GetOrNewClient(node.Address.String()).SetServerStatistics(
			map[string]string{"instance_storage_limit_bytes": "1024"},
		)
	}

	te.TriggerCollectAndWaitFinish()
	for _, node := range nodes {
		assert.DeepEqual(
			t,
			te.service.nodes.MustGetNodeInfo(node.Id).GetResource(),
			utils.HardwareUnit{utils.MEM_CAP: 1024},
		)
	}

	logging.Info("create table succeed")
	err = te.service.AddTable(tablePb, "", "")
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(te.service.tableNames), 1)

	tablePb.TableName = "test1"
	tablePb.JsonArgs = `
	{
		"btq_prefix": "mio_embedding",
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_num_keys": 10995116277760,
		"table_size_bytes": 1024,
		"table_inflation_ratio": 1.5
	}`

	logging.Info("create table failed as need to much resource")
	err = te.service.AddTable(tablePb, "", "")
	assert.Equal(t, err.Code, int32(pb.AdminError_kNotEnoughResource))
	assert.Equal(t, len(te.service.tableNames), 1)

	logging.Info("create table succeed if reduce resource")
	tablePb.JsonArgs = `
	{
		"btq_prefix": "mio_embedding",
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_num_keys": 10995116277760,
		"table_size_bytes": 512,
		"table_inflation_ratio": 1.5
	}`
	err = te.service.AddTable(tablePb, "", "")
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(te.service.tableNames), 2)
}

func TestScheduleWillTriggerEvenNoPrimary(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1002"),
				ProcessId: "12341",
				BizPort:   2002,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1003"),
				ProcessId: "12342",
				BizPort:   2003,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	for _, node := range nodes {
		te.lpb.GetOrNewClient(node.Address.String()).SetReadyToPromote(false)
	}
	te.initGivenMembership(&table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kLearner,
		},
	})
	tbl := te.service.tableNames["test"]

	versions := make([]int64, tbl.PartsCount)
	for i := range versions {
		versions[i] = tbl.currParts[i].members.MembershipVersion
	}
	i := 0
	for i < 1000 {
		logging.Info("start to schedule for round %d", i)
		te.TriggerScheduleAndWaitFinish()
		versionUpdated := false
		for i := range versions {
			part := &(tbl.currParts[i])
			if versions[i] < part.members.MembershipVersion {
				versions[i] = part.members.MembershipVersion
				versionUpdated = true
			}
		}
		if !versionUpdated {
			break
		}
		te.TriggerCollectAndWaitFinish()
		i++
	}

	req := &pb.ListNodesRequest{
		ServiceName: "test",
		TableName:   "",
		Az:          "",
		HubName:     "",
	}

	resp := &pb.ListNodesResponse{}
	te.service.ListNodes(req, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(resp.Nodes), 3)
	for _, node := range resp.Nodes {
		assert.Equal(t, node.LearnerCount, node.EstimatedCount)
	}
}

func TestAssignHubNotAllowedForStaticIndex(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ_1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
				NodeIndex: "1.0",
			},
		},
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ_2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
				NodeIndex: "2.0",
			},
		},
	}
	opts := &pb.CreateServiceRequest{
		ServiceType:   pb.ServiceType_colossusdb_dummy,
		StaticIndexed: true,
	}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	req := &pb.AssignHubRequest{
		ServiceName: "test",
		Node:        utils.FromHostPort("127.0.0.1:1001").ToPb(),
		Hub:         "YZ_2",
	}
	ans := te.service.AssignHub(req)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
}

func TestAdminOperationWillAffectPlan(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node3",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2_1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.3:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node4",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2_2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.4:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()

	logging.Info("first mark node2 as offline")
	resp := &pb.AdminNodeResponse{}
	te.service.AdminNodes(
		utils.FromHostPorts([]string{"127.0.0.2:1001"}),
		true,
		pb.AdminNodeOp_kOffline,
		resp,
	)
	assert.Equal(t, len(resp.NodeResults), 1)
	assert.Equal(t, resp.NodeResults[0].Code, int32(pb.AdminError_kOk))

	tbl := te.service.tableNames["test"]
	acts := &actions.PartitionActions{}
	acts.AddAction(&actions.AddLearnerAction{
		Node: "node1",
	})
	tbl.AddPartitionPlan(0, acts)

	te.service.AdminNodes(
		utils.FromHostPorts([]string{"127.0.0.6:1001"}),
		true,
		pb.AdminNodeOp_kOffline,
		resp,
	)
	assert.Equal(t, len(resp.NodeResults), 1)
	assert.Equal(t, resp.NodeResults[0].Code, int32(pb.AdminError_kNodeNotExisting))
	assert.Assert(t, tbl.plans != nil)

	te.service.AdminNodes(
		utils.FromHostPorts([]string{"127.0.0.1:1001", "127.0.0.2:1001"}),
		true,
		pb.AdminNodeOp_kOffline,
		resp,
	)
	assert.Equal(t, len(resp.NodeResults), 2)
	assert.Equal(t, resp.NodeResults[0].Code, int32(pb.AdminError_kOk))
	assert.Equal(t, resp.NodeResults[1].Code, int32(pb.AdminError_kDuplicateRequest))
	assert.Assert(t, tbl.plans == nil)

	acts = &actions.PartitionActions{}
	acts.AddAction(&actions.AddLearnerAction{
		Node: "node1",
	})
	tbl.AddPartitionPlan(0, acts)

	shrinkReq := &pb.ShrinkAzRequest{
		ServiceName: "test",
		Az:          "YZ1",
		NewSize:     1,
	}
	shrinkResp := &pb.ShrinkAzResponse{}
	te.service.ShrinkAz(shrinkReq, shrinkResp)
	assert.Equal(t, shrinkResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(shrinkResp.Shrinked), 0)
	assert.Assert(t, tbl.plans != nil)

	te.service.AdminNodes(
		utils.FromHostPorts([]string{"127.0.0.1:1001", "127.0.0.2:1001"}),
		true,
		pb.AdminNodeOp_kNoop,
		resp,
	)
	assert.Equal(t, len(resp.NodeResults), 2)
	assert.Equal(t, resp.NodeResults[0].Code, int32(pb.AdminError_kOk))
	assert.Equal(t, resp.NodeResults[1].Code, int32(pb.AdminError_kOk))

	te.service.ShrinkAz(shrinkReq, shrinkResp)
	assert.Equal(t, shrinkResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(shrinkResp.Shrinked), 1)
	assert.Assert(t, tbl.plans == nil)

	acts = &actions.PartitionActions{}
	acts.AddAction(&actions.AddLearnerAction{
		Node: "node1",
	})
	tbl.AddPartitionPlan(0, acts)
	assignReq := &pb.AssignHubRequest{
		ServiceName: "test",
		Node:        &pb.RpcNode{NodeName: "127.0.0.3", Port: 1001},
		Hub:         "YZ2_1",
	}
	assignResp := te.service.AssignHub(assignReq)
	assert.Equal(t, assignResp.Code, int32(pb.AdminError_kDuplicateRequest))
	assert.Assert(t, tbl.plans != nil)

	assignReq.Hub = "YZ2_2"
	assignResp = te.service.AssignHub(assignReq)
	assert.Equal(t, assignResp.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, tbl.plans == nil)

	acts = &actions.PartitionActions{}
	acts.AddAction(&actions.AddLearnerAction{
		Node: "node1",
	})
	tbl.AddPartitionPlan(0, acts)
	updateReq := &pb.UpdateScheduleOptionsRequest{
		ServiceName: te.service.serviceName,
		SchedOpts: &pb.ScheduleOptions{
			Estimator: "invalid_estimator",
		},
		UpdatedOptionNames: []string{"estimator"},
	}
	updateSchedResp := te.service.UpdateScheduleOptions(
		updateReq.SchedOpts,
		updateReq.UpdatedOptionNames,
	)
	assert.Equal(t, updateSchedResp.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Assert(t, tbl.plans != nil)

	updateReq = &pb.UpdateScheduleOptionsRequest{
		ServiceName: te.service.serviceName,
		SchedOpts: &pb.ScheduleOptions{
			EnablePrimaryScheduler: false,
		},
		UpdatedOptionNames: []string{"enable_primary_scheduler"},
	}
	updateSchedResp = te.service.UpdateScheduleOptions(
		updateReq.SchedOpts,
		updateReq.UpdatedOptionNames,
	)
	assert.Equal(t, updateSchedResp.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, tbl.plans == nil)

	acts = &actions.PartitionActions{}
	acts.AddAction(&actions.AddLearnerAction{
		Node: "node1",
	})
	tbl.AddPartitionPlan(0, acts)
	updateWeightResp := &pb.UpdateNodeWeightResponse{}
	te.service.UpdateNodeWeight(
		utils.FromHostPorts([]string{"127.0.0.1:1001"}),
		[]float32{1001},
		updateWeightResp,
	)
	assert.Equal(t, updateWeightResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, tbl.plans == nil)
}

func TestTableScheduleGrayscale(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 1})
	defer te.stop()

	assert.Equal(t, te.service.tableNames["test"].ScheduleGrayscale, int32(kScheduleGrayscaleMax))

	p := table_model.PartitionMembership{
		MembershipVersion: 3,
		Peers: map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kLearner,
		},
	}
	te.initGivenMembership(&p)

	tbl := te.service.tableNames["test"]
	logging.Info("can't update grayscale for invalid table")
	updateReq := &pb.Table{
		TableName:         "invalid_name",
		ScheduleGrayscale: 0,
	}
	resp := te.service.UpdateTable(updateReq)
	assert.Equal(t, resp.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("can't update grayscale if grayscale value is too small")
	updateReq.ScheduleGrayscale = kScheduleGrayscaleMin - 1
	resp = te.service.UpdateTable(updateReq)
	assert.Equal(t, resp.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("can't update grayscale if grayscale value is too large")
	updateReq.ScheduleGrayscale = kScheduleGrayscaleMax + 1
	resp = te.service.UpdateTable(updateReq)
	assert.Equal(t, resp.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("set table schedule grayscale to 0")
	oldProto := proto.Clone(tbl.Table).(*pb.Table)
	updateReq.TableName = tbl.TableName
	updateReq.ScheduleGrayscale = 0
	resp = te.service.UpdateTable(updateReq)
	assert.Equal(t, resp.Code, int32(pb.AdminError_kOk))
	oldProto.ScheduleGrayscale = 0
	assert.Assert(t, proto.Equal(tbl.Table, oldProto), "%v vs %v", tbl.Table, oldProto)

	node1Client := te.lpb.GetClient(nodes[0].Address.String())
	node1Client.CleanMetrics()
	node2Client := te.lpb.GetClient(nodes[1].Address.String())
	node2Client.CleanMetrics()
	part0 := &(tbl.currParts[0])

	logging.Info("plan to transform leaner to node2, but no nodes will receive request")
	act := actions.TransferLearner("node1", "node2", false)
	tbl.AddPartitionPlan(0, act)

	te.TriggerScheduleAndWaitFinish()
	te.TriggerCollectAndWaitFinish()
	assert.Equal(t, part0.members.MembershipVersion, int64(4))
	assert.Equal(t, part0.members.GetMember("node2"), pb.ReplicaRole_kLearner)
	assert.Equal(t, part0.facts["node1"].membershipVersion, int64(3))
	assert.Assert(t, part0.facts["node2"] == nil)
	assert.Equal(t, node1Client.GetMetric("reconfig"), 0)
	assert.Equal(t, node2Client.GetMetric("add_replica"), 0)

	logging.Info("set grayscale to 55, scheduler will do reconcile, and node1 will recv request")
	updateReq.ScheduleGrayscale = 55
	resp = te.service.UpdateTable(updateReq)
	assert.Equal(t, resp.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, tbl.ScheduleGrayscale, int32(55))

	te.TriggerScheduleAndWaitFinish()
	te.TriggerCollectAndWaitFinish()
	assert.Equal(t, part0.members.MembershipVersion, int64(4))
	assert.Equal(t, part0.facts["node1"].membershipVersion, int64(4))
	assert.Assert(t, part0.facts["node2"] == nil)
	assert.Equal(t, node1Client.GetMetric("reconfig"), 1)
	assert.Equal(t, node2Client.GetMetric("add_replica"), 0)

	logging.Info("set grayscale to 100, scheduler will do reconcile, and node2 will recv request")
	updateReq.ScheduleGrayscale = 100
	resp = te.service.UpdateTable(updateReq)
	assert.Equal(t, resp.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, tbl.ScheduleGrayscale, int32(100))
	te.TriggerScheduleAndWaitFinish()
	te.TriggerCollectAndWaitFinish()
	assert.Equal(t, part0.members.MembershipVersion, int64(4))
	assert.Equal(t, part0.facts["node1"].membershipVersion, int64(4))
	assert.Equal(t, part0.facts["node2"].membershipVersion, int64(4))
	assert.Equal(t, node1Client.GetMetric("reconfig"), 1)
	assert.Equal(t, node2Client.GetMetric("add_replica"), 1)

	logging.Info(
		"set grayscale to 0 again, scheduler will remove node1, but no node will recv request",
	)
	node1Client.CleanMetrics()
	node2Client.CleanMetrics()
	updateReq.ScheduleGrayscale = 0
	resp = te.service.UpdateTable(updateReq)
	assert.Equal(t, resp.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, tbl.ScheduleGrayscale, int32(0))

	te.TriggerScheduleAndWaitFinish()
	te.TriggerCollectAndWaitFinish()
	assert.Equal(t, part0.members.MembershipVersion, int64(5))
	assert.Equal(t, part0.members.GetMember("node1"), pb.ReplicaRole_kInvalid)
	assert.Assert(t, part0.facts["node1"] == nil)
	assert.Equal(t, part0.facts["node2"].membershipVersion, int64(4))
	assert.Equal(t, node1Client.GetMetric("remove_replica"), 0)
	assert.Equal(t, node2Client.GetMetric("reconfig"), 0)

	logging.Info(
		"set grayscale to 100, scheduler will do reconcile, and 1.0 will be removed on node1",
	)
	updateReq.ScheduleGrayscale = 100
	resp = te.service.UpdateTable(updateReq)
	assert.Equal(t, resp.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, tbl.ScheduleGrayscale, int32(100))
	te.TriggerScheduleAndWaitFinish()
	te.TriggerCollectAndWaitFinish()
	assert.Equal(t, part0.members.MembershipVersion, int64(5))
	assert.Equal(t, part0.facts["node2"].membershipVersion, int64(5))
	assert.Equal(t, node1Client.GetMetric("remove_replica"), 1)
	assert.Equal(t, node2Client.GetMetric("reconfig"), 1)
}

func TestDeleteTable(t *testing.T) {
	*flagIdRecycleDuration = 3 * time.Second
	*flagIdRecycleIntervalMinute = 5 * time.Second
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()
	assert.Equal(t, te.service.tableNames["test"].ScheduleGrayscale, int32(kScheduleGrayscaleMax))
	// delete table 1
	tablePb := &pb.Table{
		TableId:           1,
		TableName:         "test1",
		HashMethod:        "crc32",
		PartsCount:        32,
		JsonArgs:          `{"a1": "b1", "c1": "d1"}`,
		KconfPath:         "/reco/embedding_server",
		ScheduleGrayscale: kScheduleGrayscaleMax + 1,
	}
	status := te.service.AddTable(tablePb, "", "")
	assert.Equal(t, status.Code, int32(pb.AdminError_kOk))
	te.service.RemoveTable("test1", false, 0)
	time.Sleep(time.Second * 3)
	// delete table 2
	tablePb = &pb.Table{
		TableId:           2,
		TableName:         "test2",
		HashMethod:        "crc32",
		PartsCount:        32,
		JsonArgs:          `{"a2": "b2", "c2": "d2"}`,
		KconfPath:         "/reco/embedding_server",
		ScheduleGrayscale: kScheduleGrayscaleMax + 1,
	}
	status = te.service.AddTable(tablePb, "", "")
	assert.Equal(t, status.Code, int32(pb.AdminError_kOk))
	tbId1 := te.service.tablesManager.tableNames["test2"]
	te.service.RemoveTable("test2", false, 0)
	time.Sleep(time.Second * 3)
	assert.Equal(t, len(te.service.tablesManager.recyclingIds), 1)
	for key := range te.service.tablesManager.recyclingIds {
		assert.Equal(t, key, tbId1.TableId)
	}

	// test reload recycle table
	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	zkStore := metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"},
		time.Second*10,
		acl,
		scheme,
		auth,
	)
	defer zkStore.Close()
	tablesManager := NewTablesManager(zkStore, serviceStatTestZkPrefix+"/tables")
	tablesManager.InitFromZookeeper()
	time.Sleep(time.Second * 1)
	assert.Equal(t, len(tablesManager.recyclingIds), 1)
	for key := range tablesManager.recyclingIds {
		assert.Equal(t, key, tbId1.TableId)
	}
	tablesManager.Stop()

	// reset
	*flagIdRecycleDuration = 7 * 24 * time.Hour
	*flagIdRecycleIntervalMinute = 5 * time.Minute
}

func TestCleanTableStateCostLong(t *testing.T) {
	serviceName := "test"
	tableName := "test"
	hubs := []*pb.ReplicaHub{
		{Name: "YZ1", Az: "YZ"},
		{Name: "YZ2", Az: "YZ"},
		{Name: "YZ3", Az: "YZ"},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	metaStore := bootstrapService(t, opts, serviceName, tableName, 32, hubs, nil, "mock")
	defer metaStore.Close()

	tablesManager := NewTablesManager(metaStore, serviceStatTestZkPrefix+"/tables")
	tablesManager.InitFromZookeeper()
	delayedExecutorManager := delay_execute.NewDelayedExecutorManager(
		metaStore,
		serviceStatTestZkPrefix+"/"+kDelayedExecutorNode,
	)
	delayedExecutorManager.InitFromZookeeper()
	defer delayedExecutorManager.Stop()

	tdChan := make(chan string)
	service := NewServiceStat(
		serviceName,
		"test",
		serviceStatTestZkPrefix,
		metaStore,
		tablesManager,
		delayedExecutorManager,
		tdChan,
	)
	service.LoadFromZookeeper()
	metaStore.(*metastore.MetaStoreMock).BlockApi("RecursiveDelete")
	ans := service.RemoveTable(tableName, false, 0)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	ans = service.RemoveTable(tableName, false, 0)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))

	ans = service.AddTable(&pb.Table{
		TableId:           0,
		TableName:         tableName,
		HashMethod:        "crc32",
		PartsCount:        32,
		JsonArgs:          `{"a": "b", "c": "d", "btq_prefix": "dummy"}`,
		KconfPath:         "reco.rodisFea.partitionKeeperHDFSTest",
		ScheduleGrayscale: kScheduleGrayscaleMax + 1,
	}, "", "")
	assert.Equal(t, ans.Code, int32(pb.AdminError_kTableExists))

	ans = service.Teardown()
	assert.Equal(t, ans.Code, int32(pb.AdminError_kTableNotEmpty))

	metaStore.(*metastore.MetaStoreMock).UnBlockApi("RecursiveDelete")
	time.Sleep(time.Millisecond * 200)

	ans = service.Teardown()
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	name := <-tdChan
	assert.Equal(t, name, serviceName)
}

func TestDeleteTableNotifyRemovePartition(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()
	assert.Equal(t, te.service.tableNames["test"].ScheduleGrayscale, int32(kScheduleGrayscaleMax))
	p := table_model.PartitionMembership{
		MembershipVersion: 3,
		Peers: map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kLearner,
		},
	}
	te.initGivenMembership(&p)
	te.service.RemoveTable("test", false, 0)
	te.TriggerCollectAndWaitFinish()
	assert.Equal(t, te.lpb.GetClient(nodes[0].Address.String()).GetMetric("remove_replica"), 32)
}

func TestLoadUnregisteredTable(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()
	assert.Equal(t, te.service.tableNames["test"].ScheduleGrayscale, int32(kScheduleGrayscaleMax))

	tablePb := &pb.Table{
		TableId:           1,
		TableName:         "test1",
		HashMethod:        "crc32",
		PartsCount:        32,
		JsonArgs:          `{"a1": "b1", "c1": "d1"}`,
		KconfPath:         "reco.rodisFea.partitionKeeperHDFSTest",
		ScheduleGrayscale: kScheduleGrayscaleMax + 1,
	}

	status := te.service.AddTable(tablePb, "", "")
	assert.Equal(t, status.Code, int32(pb.AdminError_kOk))

	a := make(map[string]*TableStats)
	te.service.stop()
	te.service.tableNames = a
	te.service.LoadFromZookeeper()
	assert.Equal(t, len(te.service.tableNames), 2)
	assert.Assert(t, te.service.tableNames["test"] != nil)
	assert.Assert(t, te.service.tableNames["test1"] != nil)

	tbId1 := te.service.tablesManager.tableNames["test1"]
	te.service.tablesManager.StartUnregisterTable(tbId1.TableId, false, 0)
	assert.Equal(t, te.service.tablesManager.IsUnregistered(tbId1.TableId), true)

	te.service.stop()
	a = make(map[string]*TableStats)
	te.service.tableNames = a
	te.service.LoadFromZookeeper()
	assert.Equal(t, len(te.service.tableNames), 1)
	assert.Assert(t, te.service.tableNames["test"] != nil)
	assert.Assert(t, te.service.tableNames["test1"] == nil)
}

func TestDeleteTableWithCleanTaskSideEffect(t *testing.T) {
	*flagIdRecycleDuration = 3 * time.Second
	*flagIdRecycleIntervalMinute = 5 * time.Second
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "STAGING",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "STAGING",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "STAGING",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "STAGING",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
	}
	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	te := setupTestServiceEnv(t, nodes, opts, &testServiceEnvSetupOptions{true, true, "zk", 32})
	defer te.stop()
	assert.Equal(t, te.service.tableNames["test"].ScheduleGrayscale, int32(kScheduleGrayscaleMax))

	tablePb := &pb.Table{
		TableId:           1,
		TableName:         "test1",
		HashMethod:        "crc32",
		PartsCount:        32,
		JsonArgs:          `{"a1": "b1", "c1": "d1"}`,
		KconfPath:         "reco.rodisFea.partitionKeeperHDFSTest",
		ScheduleGrayscale: kScheduleGrayscaleMax + 1,
	}
	status := te.service.AddTable(tablePb, "", "")
	assert.Equal(t, status.Code, int32(pb.AdminError_kOk))
	table1 := te.service.tableNames[tablePb.TableName]
	assert.Equal(t, len(table1.tasks), 1)
	te.service.RemoveTable("test1", true, 1)
	assert.Equal(
		t,
		te.service.delayedExecutorManager.ExecutorExist(
			getTaskExecutorName(tablePb.TableId, checkpoint.TASK_NAME),
		),
		true,
	)

	tablePb = &pb.Table{
		TableId:           2,
		TableName:         "test2",
		HashMethod:        "crc32",
		PartsCount:        32,
		JsonArgs:          `{"a2": "b2", "c2": "d2"}`,
		KconfPath:         "reco.rodisFea.partitionKeeperHDFSTest",
		ScheduleGrayscale: kScheduleGrayscaleMax + 1,
	}
	status = te.service.AddTable(tablePb, "", "")
	assert.Equal(t, status.Code, int32(pb.AdminError_kOk))
	tbId2 := te.service.tablesManager.tableNames["test2"]
	now := time.Now().Unix()
	te.service.tablesManager.recyclingIds[tbId2.TableId] = recyclingIdContext{
		Deadline:            now + 3600,
		CleanTaskSideEffect: true,
		CleanExecuteTime:    0,
	}
	assert.Equal(
		t,
		te.service.delayedExecutorManager.ExecutorExist(
			getTaskExecutorName(tbId2.TableId, checkpoint.TASK_NAME),
		),
		false,
	)
	te.service.stop()
	te.service.LoadFromZookeeper()
	assert.Equal(
		t,
		te.service.delayedExecutorManager.ExecutorExist(
			getTaskExecutorName(tbId2.TableId, checkpoint.TASK_NAME),
		),
		true,
	)
	// reset
	*flagIdRecycleDuration = 7 * 24 * time.Hour
	*flagIdRecycleIntervalMinute = 5 * time.Minute
}
