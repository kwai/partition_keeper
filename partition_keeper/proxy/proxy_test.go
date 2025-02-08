package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/proxy/meta"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/checkpoint"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/watcher"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
)

var (
	keeperTestZkPrefix = utils.SpliceZkRootPath("/test/proxy/keeper")
)

func cleanZookeeper(t *testing.T) {
	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	zkStore := metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"},
		time.Second*10,
		acl,
		scheme,
		auth,
	)
	defer zkStore.Close()

	ans := zkStore.RecursiveDelete(context.Background(), keeperTestZkPrefix)
	assert.Assert(t, ans)
	ans = zkStore.RecursiveDelete(context.Background(), keeperTestZkPrefix)
	assert.Assert(t, ans)
}

func startTestServer(port int, pollLeaderMillis int, namespace string) *server.Server {
	serviceType := map[pb.ServiceType]bool{
		pb.ServiceType_colossusdb_dummy: true,
		pb.ServiceType_colossusdb_rodis: true,
	}
	s := server.CreateServer(
		server.WithZk([]string{"127.0.0.1:2181"}),
		server.WithZkPrefix(keeperTestZkPrefix),
		server.WithHttpPort(port),
		server.WithNamespace(namespace),
		server.WithPollLeaderMillis(pollLeaderMillis),
		server.WithAllowServiceType(serviceType),
		server.WithServiceName("colossusdbUnitTestServer"),
	)
	go s.Start()
	return s
}

func startTestProxy(t *testing.T, port int) *Proxy {
	*flagHttpPort = port
	flagZkHosts = []string{"127.0.0.1:2181"}
	*flagZkPrefix = keeperTestZkPrefix
	*meta.FlagKconfPath = "reco.partitionKeeper.proxy_test"

	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	zkStore := metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"},
		time.Second*10,
		acl,
		scheme,
		auth,
	)

	for _, namespace := range namespaces {
		ans := zkStore.RecursiveCreate(
			context.Background(),
			keeperTestZkPrefix+"/"+namespace+"/service",
		)
		assert.Assert(t, ans)
		ans = zkStore.RecursiveCreate(
			context.Background(),
			keeperTestZkPrefix+"/"+namespace+"/tables",
		)
		assert.Assert(t, ans)
	}

	p := CreateProxy()
	go p.Start()
	time.Sleep(time.Second * 5)
	return p
}

func postToMethod(t *testing.T, method string, req, resp proto.Message) {
	err := utils.HttpPostJson("http://127.0.0.1:30030/v1/"+method, nil, nil, req, resp)
	assert.NilError(t, err, "got error: %v", err)
}

func getFromMethod(t *testing.T, method string, args map[string]string, resp proto.Message) {
	err := utils.HttpGet(
		"http://127.0.0.1:30030/v1/"+method,
		nil,
		args,
		resp,
		utils.DefaultCheckResp,
	)
	assert.NilError(t, err, "got error: %v", err)
}

func listServices(t *testing.T) *pb.ListServicesResponse {
	resp := &pb.ListServicesResponse{}
	getFromMethod(t, "list_services", map[string]string{}, resp)
	return resp
}

func createService(
	t *testing.T,
	serviceName string,
	hubs []*pb.ReplicaHub,
	svcType pb.ServiceType,
	fdType pb.NodeFailureDomainType,
) *pb.ErrorStatusResponse {
	request := &pb.CreateServiceRequest{
		ServiceName:       serviceName,
		NodesHubs:         hubs,
		ServiceType:       svcType,
		FailureDomainType: fdType,
		StaticIndexed:     false,
		UsePaz:            false,
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "create_service", request, resp)
	return resp
}

func addHubs(t *testing.T, serviceName string, hubs []*pb.ReplicaHub) *pb.ErrorStatusResponse {
	request := &pb.AddHubsRequest{
		ServiceName: serviceName,
		NodesHubs:   hubs,
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "add_hubs", request, resp)
	return resp
}

func removeHubs(
	t *testing.T,
	serviceName string,
	hubs []*pb.ReplicaHub,
) *pb.ErrorStatusResponse {
	request := &pb.RemoveHubsRequest{
		ServiceName: serviceName,
		NodesHubs:   hubs,
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "remove_hubs", request, resp)
	return resp
}

func updateHubs(t *testing.T, serviceName string, hubs []*pb.ReplicaHub) *pb.ErrorStatusResponse {
	request := &pb.UpdateHubsRequest{
		ServiceName: serviceName,
		NodesHubs:   hubs,
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "update_hubs", request, resp)
	return resp
}

func updateScheduleOptions(
	t *testing.T,
	serviceName string,
	opts *pb.ScheduleOptions,
	keys []string,
) *pb.ErrorStatusResponse {
	req := &pb.UpdateScheduleOptionsRequest{
		ServiceName:        serviceName,
		SchedOpts:          opts,
		UpdatedOptionNames: keys,
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "update_schedule_options", req, resp)
	return resp
}

func deleteService(t *testing.T, serviceName string) *pb.ErrorStatusResponse {
	req := &pb.DeleteServiceRequest{ServiceName: serviceName}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "delete_service", req, resp)
	return resp
}

func createTable(t *testing.T, req *pb.CreateTableRequest) *pb.ErrorStatusResponse {
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "create_table", req, resp)
	return resp
}

func updateTable(t *testing.T, req *pb.UpdateTableRequest) *pb.ErrorStatusResponse {
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "update_table", req, resp)
	return resp
}

func listTables(t *testing.T, serviceName string) *pb.ListTablesResponse {
	resp := &pb.ListTablesResponse{}
	getFromMethod(t, "list_tables", map[string]string{
		"service_name": serviceName,
	}, resp)
	return resp
}

func deleteTable(t *testing.T, serviceName string, tableName string) *pb.ErrorStatusResponse {
	req := &pb.DeleteTableRequest{ServiceName: serviceName, TableName: tableName}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "delete_table", req, resp)
	return resp
}

func queryTable(t *testing.T, req *pb.QueryTableRequest) *pb.QueryTableResponse {
	params := map[string]string{
		"service_name":    req.ServiceName,
		"table_name":      req.TableName,
		"with_tasks":      utils.Bool2Str(req.WithTasks),
		"with_partitions": utils.Bool2Str(req.WithPartitions),
	}

	resp := &pb.QueryTableResponse{}
	getFromMethod(t, "query_table", params, resp)
	return resp
}

func updateTableJsonArgs(
	t *testing.T,
	serviceName, tableName, jsonArgs string,
) *pb.ErrorStatusResponse {
	req := &pb.UpdateTableJsonArgsRequest{
		ServiceName: serviceName,
		TableName:   tableName,
		JsonArgs:    jsonArgs,
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "update_table_json_args", req, resp)
	return resp
}

func manualRemoveReplicas(
	t *testing.T,
	req *pb.ManualRemoveReplicasRequest,
) *pb.ManualRemoveReplicasResponse {
	resp := &pb.ManualRemoveReplicasResponse{}
	postToMethod(t, "remove_replicas", req, resp)
	return resp
}

func queryTask(t *testing.T, req *pb.QueryTaskRequest) *pb.QueryTaskResponse {
	params := map[string]string{
		"service_name": req.ServiceName,
		"table_name":   req.TableName,
		"task_name":    req.TaskName,
	}
	resp := &pb.QueryTaskResponse{}
	getFromMethod(t, "query_task", params, resp)
	return resp
}

func queryTaskCurrentExecution(
	t *testing.T,
	req *pb.QueryTaskCurrentExecutionRequest,
) *pb.QueryTaskCurrentExecutionResponse {
	params := map[string]string{
		"service_name": req.ServiceName,
		"table_name":   req.TableName,
		"task_name":    req.TaskName,
	}
	resp := &pb.QueryTaskCurrentExecutionResponse{}
	getFromMethod(t, "query_task_current_execution", params, resp)
	return resp
}

func deleteTask(t *testing.T, req *pb.DeleteTaskRequest) *pb.ErrorStatusResponse {
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "delete_task", req, resp)
	return resp
}

func restoreTable(t *testing.T, req *pb.RestoreTableRequest) *pb.ErrorStatusResponse {
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "restore_table", req, resp)
	return resp
}

func cleanTaskSideEffect(
	t *testing.T,
	req *pb.TriggerDeleteTaskSideEffectRequest,
) *pb.ErrorStatusResponse {
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "trigger_delete_task_side_effect", req, resp)
	return resp
}

func adminNode(
	t *testing.T,
	serviceName string,
	nodes []string,
	op pb.AdminNodeOp,
) *pb.AdminNodeResponse {
	req := &pb.AdminNodeRequest{
		ServiceName: serviceName,
		Op:          op,
	}
	metaNodes := utils.FromHostPorts(nodes)
	for _, m := range metaNodes {
		req.Nodes = append(req.Nodes, m.ToPb())
	}

	resp := &pb.AdminNodeResponse{}
	postToMethod(t, "admin_node", req, resp)
	return resp
}

func updateNodeWeight(
	t *testing.T,
	serviceName string,
	nodes []string,
	weight []float32,
) *pb.UpdateNodeWeightResponse {
	req := &pb.UpdateNodeWeightRequest{
		ServiceName: serviceName,
	}
	metaNodes := utils.FromHostPorts(nodes)
	for _, m := range metaNodes {
		req.Nodes = append(req.Nodes, m.ToPb())
	}
	req.Weights = weight
	resp := &pb.UpdateNodeWeightResponse{}
	postToMethod(t, "update_node_weight", req, resp)
	return resp
}

func giveHints(
	t *testing.T,
	serviceName string,
	onHubs map[string]string,
	matchPort bool,
) *pb.ErrorStatusResponse {
	req := &pb.GiveHintsRequest{
		ServiceName: serviceName,
		Hints:       make(map[string]*pb.NodeHints),
		MatchPort:   matchPort,
	}
	for host, hub := range onHubs {
		req.Hints[host] = &pb.NodeHints{Hub: hub}
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "give_hints", req, resp)
	return resp
}

func recallHints(
	t *testing.T,
	serviceName string,
	hostPorts []string,
	matchPort bool,
) *pb.ErrorStatusResponse {
	req := &pb.RecallHintsRequest{
		ServiceName: serviceName,
		MatchPort:   matchPort,
	}
	for _, hp := range hostPorts {
		req.Nodes = append(req.Nodes, utils.FromHostPort(hp).ToPb())
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "recall_hints", req, resp)
	return resp
}

func shrinkAz(t *testing.T, serviceName string, az string, newSize int) *pb.ShrinkAzResponse {
	req := &pb.ShrinkAzRequest{
		ServiceName: serviceName,
		Az:          az,
		NewSize:     int32(newSize),
	}
	resp := &pb.ShrinkAzResponse{}
	postToMethod(t, "shrink_az", req, resp)
	return resp
}

func expandAzs(t *testing.T, serviceName string, newSize map[string]int) *pb.ErrorStatusResponse {
	req := &pb.ExpandAzsRequest{
		ServiceName: serviceName,
		AzOptions:   nil,
	}
	for az, size := range newSize {
		req.AzOptions = append(
			req.AzOptions,
			&pb.ExpandAzsRequest_AzOption{Az: az, NewSize: int32(size)},
		)
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "expand_azs", req, resp)
	return resp
}

func replaceNodes(
	t *testing.T,
	serviceName string,
	replace map[string]string,
) *pb.ErrorStatusResponse {
	req := &pb.ReplaceNodesRequest{
		ServiceName: serviceName,
		SrcNodes:    nil,
		DstNodes:    nil,
	}
	for src, dst := range replace {
		req.SrcNodes = append(req.SrcNodes, utils.FromHostPort(src).ToPb())
		req.DstNodes = append(req.DstNodes, utils.FromHostPort(dst).ToPb())
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "replace_nodes", req, resp)
	return resp
}

func assignHub(t *testing.T, serviceName string, node string, hub string) *pb.ErrorStatusResponse {
	req := &pb.AssignHubRequest{
		ServiceName: serviceName,
		Node:        utils.FromHostPort(node).ToPb(),
		Hub:         hub,
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "assign_hub", req, resp)
	return resp
}

func listNodes(t *testing.T, req *pb.ListNodesRequest) *pb.ListNodesResponse {
	resp := &pb.ListNodesResponse{}
	params := map[string]string{
		"service_name": req.ServiceName,
		"table_name":   req.TableName,
		"hub_name":     req.HubName,
		"az":           req.Az,
	}

	getFromMethod(t, "list_nodes", params, resp)
	return resp
}

func queryNodesInfo(t *testing.T, serviceName string, nodes string) *pb.QueryNodesInfoResponse {
	var pbNodes []*pb.RpcNode
	for _, host := range strings.Split(nodes, ",") {
		pbNodes = append(pbNodes, utils.FromHostPort(host).ToPb())
	}

	req := &pb.QueryNodesInfoRequest{
		ServiceName: serviceName,
		Nodes:       pbNodes,
	}
	resp := &pb.QueryNodesInfoResponse{}
	postToMethod(t, "query_nodes_info", req, resp)
	return resp
}

func queryNodeInfo(t *testing.T, req *pb.QueryNodeInfoRequest) *pb.QueryNodeInfoResponse {
	params := map[string]string{
		"service_name": req.ServiceName,
		"node_name":    req.NodeName,
		"port":         fmt.Sprintf("%d", req.Port),
		"table_name":   req.TableName,
		"only_brief":   fmt.Sprintf("%t", req.OnlyBrief),
		"match_port":   fmt.Sprintf("%t", req.MatchPort),
	}
	resp := &pb.QueryNodeInfoResponse{}
	getFromMethod(t, "query_node_info", params, resp)
	return resp
}

func switchScheduler(t *testing.T, serviceName string, enable bool) *pb.ErrorStatusResponse {
	req := &pb.SwitchSchedulerStatusRequest{
		ServiceName: serviceName,
		Enable:      enable,
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "switch_scheduler_status", req, resp)
	return resp
}

func switchKessPoller(t *testing.T, serviceName string, enable bool) *pb.ErrorStatusResponse {
	req := &pb.SwitchKessPollerStatusRequest{
		ServiceName: serviceName,
		Enable:      enable,
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "switch_kess_poller_status", req, resp)
	return resp
}

func queryService(t *testing.T, serviceName string) *pb.QueryServiceResponse {
	resp := &pb.QueryServiceResponse{}
	getFromMethod(t, "query_service", map[string]string{"service_name": serviceName}, resp)
	return resp
}

func operateTask(
	t *testing.T,
	serviceName string,
	tableName string,
	method string,
	task *pb.PeriodicTask,
) *pb.ErrorStatusResponse {
	resp := &pb.ErrorStatusResponse{}

	req := &pb.OperateTaskRequest{
		ServiceName: serviceName,
		TableName:   tableName,
		Task:        task,
	}
	postToMethod(t, method, req, resp)
	return resp
}

func removeWatcher(t *testing.T, serviceName string, watcherName string) *pb.ErrorStatusResponse {
	req := &pb.RemoveWatcherRequest{
		ServiceName: serviceName,
		WatcherName: watcherName,
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "remove_watcher", req, resp)
	return resp
}

func TestProxy(t *testing.T) {
	cleanZookeeper(t)
	s1 := startTestServer(10009, 500, "test_default")
	defer s1.Stop()
	time.Sleep(time.Second * 1)

	s2 := startTestServer(11009, 500, "test_rodis")
	time.Sleep(time.Second * 1)

	s3 := startTestServer(12009, 500, "test_rodis2")
	time.Sleep(time.Second * 2)

	p := startTestProxy(t, 30030)
	defer p.Stop()

	logging.Info("create service invalid service type")
	createResp := createService(t, "test", []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
		{Name: "yz2", Az: "YZ"},
	}, pb.ServiceType_invalid, pb.NodeFailureDomainType_PROCESS)
	assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("create dummy service succ")
	hubs := []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
		{Name: "yz2", Az: "YZ"},
		{Name: "zw1", Az: "ZW"},
		{Name: "zw2", Az: "ZW"},
	}
	createResp = createService(
		t,
		"test",
		hubs,
		pb.ServiceType_colossusdb_dummy,
		pb.NodeFailureDomainType_PROCESS,
	)
	assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("create rodis service succ")
	createResp = createService(
		t,
		"rodis_service_test",
		hubs,
		pb.ServiceType_colossusdb_rodis,
		pb.NodeFailureDomainType_PROCESS,
	)
	assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("create service with already exists")
	createResp = createService(
		t,
		"test",
		hubs,
		pb.ServiceType_colossusdb_dummy,
		pb.NodeFailureDomainType_PROCESS,
	)
	assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kServiceExists))

	logging.Info("list services")
	listServicesResp := listServices(t)
	assert.Equal(
		t,
		listServicesResp.Status.Code,
		int32(pb.AdminError_kOk),
		"status: %v",
		listServicesResp.Status.Message,
	)
	assert.Assert(t, is.Len(listServicesResp.ServiceNames, 2))

	logging.Info("list all table ok")
	listTblResp := listTables(t, "")
	assert.Equal(t, listTblResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("rodis keeper dead")
	s2.Stop()
	time.Sleep(time.Second)
	s3.Stop()
	time.Sleep(time.Second)

	logging.Info("create rodis2 with keeper dead")
	createResp = createService(
		t,
		"rodis2_service_test",
		hubs,
		pb.ServiceType_colossusdb_rodis,
		pb.NodeFailureDomainType_PROCESS,
	)
	assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	queryServiceResp := queryService(t, "test2")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	queryServiceResp = queryService(t, "test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kOk))

	queryServiceResp = queryService(t, "rodis_service_test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("update schedule options with service not exist")
	updateSchedOptResp := updateScheduleOptions(
		t,
		"test2",
		&pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 100},
		[]string{"enable_primary_scheduler", "max_sched_ratio"},
	)
	assert.Equal(t, updateSchedOptResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("update schedule options with keeper dead")
	updateSchedOptResp = updateScheduleOptions(
		t,
		"rodis_service_test",
		&pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 100},
		[]string{"enable_primary_scheduler", "max_sched_ratio"},
	)
	assert.Equal(t, updateSchedOptResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("update schedule options succ")
	updateSchedOptResp = updateScheduleOptions(
		t,
		"test",
		&pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 100},
		[]string{"enable_primary_scheduler", "max_sched_ratio"},
	)
	assert.Equal(t, updateSchedOptResp.Status.Code, int32(pb.AdminError_kOk))

	// use a control machine, if it changed, we'd better change our test
	hostName := "reco-bjzey-c6-reco135.idchb1az3.hb1.kwaidc.com"
	logging.Info("give hints to non-existing service")
	giveHintsResp := giveHints(
		t,
		"test2",
		map[string]string{hostName + ":1001": "zw1"},
		true,
	)
	assert.Equal(t, giveHintsResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("give hints with keeper dead")
	giveHintsResp = giveHints(
		t,
		"rodis_service_test",
		map[string]string{hostName + ":1001": "zw1"},
		true,
	)
	assert.Equal(t, giveHintsResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("give hints succeed")
	giveHintsResp = giveHints(
		t,
		"test",
		map[string]string{hostName + ":1001": "zw1"},
		true,
	)
	assert.Equal(t, giveHintsResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("recall hints to non-existing service")
	recallHintsResp := recallHints(
		t,
		"test2",
		[]string{hostName + ":1001"},
		true,
	)
	assert.Equal(t, recallHintsResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("recall hints with keeper dead")
	recallHintsResp = recallHints(t, "rodis_service_test", []string{hostName + ":1001"}, true)
	assert.Equal(t, recallHintsResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("recall hints succeed")
	recallHintsResp = recallHints(t, "test", []string{hostName + ":1001"}, true)
	assert.Equal(t, recallHintsResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("add hubs for non-existing service")
	addHubsResp := addHubs(t, "test2", []*pb.ReplicaHub{
		{Name: "yz3", Az: "YZ"},
		{Name: "zw3", Az: "ZW"},
	})
	assert.Equal(t, addHubsResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("add hubs with keeper dead")
	addHubsResp = addHubs(t, "rodis_service_test", []*pb.ReplicaHub{
		{Name: "yz3", Az: "YZ"},
		{Name: "zw3", Az: "ZW"},
	})
	assert.Equal(t, addHubsResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("add hubs succeed")
	addHubsResp = addHubs(t, "test", []*pb.ReplicaHub{
		{Name: "yz3", Az: "YZ"},
		{Name: "zw3", Az: "ZW"},
	})
	assert.Equal(t, addHubsResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("update hubs with non-existing service")
	updateHubsResp := updateHubs(t, "test2", []*pb.ReplicaHub{
		{Name: "yz3", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kSecondary}},
	})
	assert.Equal(t, updateHubsResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("update hubs with keeper dead")
	updateHubsResp = updateHubs(t, "rodis_service_test", []*pb.ReplicaHub{
		{Name: "yz3", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kSecondary}},
		{Name: "zw3", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kPrimary}},
	})
	assert.Equal(t, updateHubsResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("update hubs succeed")
	updateHubsResp = updateHubs(t, "test", []*pb.ReplicaHub{
		{Name: "yz3", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kSecondary}},
		{Name: "zw3", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kPrimary}},
	})
	assert.Equal(t, updateHubsResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("remove hubs for non-existing service")
	removeHubsResp := removeHubs(t, "test2", []*pb.ReplicaHub{
		{Name: "yz3", Az: "YZ"},
		{Name: "zw3", Az: "ZW"},
	})
	assert.Equal(t, removeHubsResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("remove hubs with keeper dead")
	removeHubsResp = removeHubs(t, "rodis_service_test", []*pb.ReplicaHub{
		{Name: "yz3", Az: "YZ"},
		{Name: "zw3", Az: "ZW"},
	})
	assert.Equal(t, removeHubsResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("remove hubs succeed")
	removeHubsResp = removeHubs(t, "test", []*pb.ReplicaHub{
		{Name: "yz3", Az: "YZ"},
		{Name: "zw3", Az: "ZW"},
	})
	assert.Equal(t, removeHubsResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("create table on not exist service")
	createTblReq := &pb.CreateTableRequest{
		ServiceName: "test2",
		Table: &pb.Table{
			TableId:    0,
			TableName:  "test",
			HashMethod: "crc32",
			PartsCount: 32,
			JsonArgs: `{
					"a": "b",
					"c": "d"
				}`,
			KconfPath: "reco.rodisFea.partitionKeeperHDFSTest",
		},
	}
	createTblResp := createTable(t, createTblReq)
	assert.Equal(t, createTblResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("create table with invalid name")
	createTblReq.ServiceName = "test"
	createTblReq.Table.TableName = "invalid$table_name"
	createTblResp = createTable(t, createTblReq)
	assert.Equal(t, createTblResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("create table with keeper dead")
	createTblReq.Table.TableName = "test11"
	createTblReq.Table.PartsCount = 16
	createTblReq.ServiceName = "rodis_service_test"
	createTblResp = createTable(t, createTblReq)
	assert.Equal(t, createTblResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("create table")
	createTblReq.ServiceName = "test"
	createTblReq.Table.PartsCount = 32
	createTblReq.Table.TableName = "test"
	createTblResp = createTable(t, createTblReq)
	assert.Equal(t, createTblResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("create another table")
	createTblReq.Table.TableName = "test10"
	createTblReq.Table.PartsCount = 16
	createTblResp = createTable(t, createTblReq)
	assert.Equal(t, createTblResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("create existing table")
	createTblResp = createTable(t, createTblReq)
	assert.Equal(t, createTblResp.Status.Code, int32(pb.AdminError_kTableExists))

	logging.Info("update table to not exist service")
	updateTableReq := &pb.UpdateTableRequest{
		ServiceName: "not-exist-service-name",
		Table: &pb.Table{
			TableName:         "test",
			ScheduleGrayscale: 0,
		},
	}
	updateResp := updateTable(t, updateTableReq)
	assert.Equal(t, updateResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("update table with keeper dead")
	updateTableReq.ServiceName = "rodis_service_test"
	updateResp = updateTable(t, updateTableReq)
	assert.Equal(t, updateResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("update table ok")
	updateTableReq.ServiceName = "test"
	updateResp = updateTable(t, updateTableReq)
	assert.Equal(t, updateResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("list table with non-exist service")
	listTblResp = listTables(t, "test2")
	assert.Equal(t, listTblResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("list table with keeper dead")
	listTblResp = listTables(t, "rodis_service_test")
	assert.Equal(t, listTblResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("list table ok")
	listTblResp = listTables(t, "test")
	assert.Equal(t, listTblResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, is.Len(listTblResp.Tables, 2))
	table := listTblResp.Tables[0]
	assert.Assert(t, table.TableId > 1001)
	assert.Equal(t, table.TableName, "test")
	assert.Equal(t, table.PartsCount, int32(32))
	assert.Equal(t, table.JsonArgs, createTblReq.Table.JsonArgs)
	assert.Equal(t, table.KconfPath, createTblReq.Table.KconfPath)

	logging.Info("query table with non-exist service")
	queryTableReq := &pb.QueryTableRequest{
		ServiceName:    "test2",
		TableName:      "test",
		WithPartitions: true,
		WithTasks:      true,
	}
	queryTableResp := queryTable(t, queryTableReq)
	assert.Equal(t, queryTableResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("query table with keeper dead")
	queryTableReq.ServiceName = "rodis_service_test"
	queryTableResp = queryTable(t, queryTableReq)
	assert.Equal(t, queryTableResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("query table ok")
	queryTableReq.ServiceName = "test"
	queryTableResp = queryTable(t, queryTableReq)
	assert.Equal(t, queryTableResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, is.Len(queryTableResp.Partitions, 32))
	assert.Assert(t, is.Len(queryTableResp.Tasks, 1))
	task := queryTableResp.Tasks[0]
	assert.Equal(t, task.TaskName, checkpoint.TASK_NAME)
	assert.Equal(t, task.PeriodSeconds, int64(86400))
	assert.Equal(t, task.MaxConcurrencyPerNode, int32(5))
	assert.Assert(t, task.Args == nil)

	logging.Info("query table with no partition")
	queryTableReq.WithPartitions = false
	queryTableResp = queryTable(t, queryTableReq)
	assert.Equal(t, queryTableResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, is.Len(queryTableResp.Partitions, 0))
	assert.Assert(t, is.Len(queryTableResp.Tasks, 1))

	logging.Info("query table with no partition no task")
	queryTableReq.WithTasks = false
	queryTableResp = queryTable(t, queryTableReq)
	assert.Equal(t, queryTableResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, is.Len(queryTableResp.Partitions, 0))
	assert.Assert(t, is.Len(queryTableResp.Tasks, 0))

	logging.Info("update table with non-exist service")
	JsonArgs := `{
		"btq_prefix": "dummy",
		"zk_hosts": [],
		"cluster_prefix": "/test/rodis/test",
		"enable_publish_zk": false
	}`
	updateTableJsonResp := updateTableJsonArgs(t, "test2", "test", JsonArgs)
	assert.Equal(t, updateTableJsonResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("update table with keeper dead")
	updateTableJsonResp = updateTableJsonArgs(t, "rodis_service_test", "test", JsonArgs)
	assert.Equal(t, updateTableJsonResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("update table ok")
	updateTableJsonResp = updateTableJsonArgs(t, "test", "test", JsonArgs)
	assert.Equal(t, updateTableJsonResp.Status.Code, int32(pb.AdminError_kOk))
	queryTableReq.TableName = "test"
	queryTableResp = queryTable(t, queryTableReq)
	type tempUpdateJson struct {
		ZkHosts []string `json:"zk_hosts"`
	}
	rc := &tempUpdateJson{}
	err := json.Unmarshal([]byte(queryTableResp.Table.JsonArgs), &rc)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(rc.ZkHosts), 0)

	logging.Info("remove replicas of non-existing service")
	manualRemoveReplicasReq := &pb.ManualRemoveReplicasRequest{
		ServiceName: "not-existing",
		TableName:   "test",
		Replicas: []*pb.ManualRemoveReplicasRequest_ReplicaItem{
			{
				Node:        &pb.RpcNode{NodeName: "127.0.0.1", Port: 1001},
				PartitionId: 10001,
			},
		},
	}
	manualRemoveResp := manualRemoveReplicas(t, manualRemoveReplicasReq)
	assert.Equal(t, manualRemoveResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("remove replicas with keeper dead")
	manualRemoveReplicasReq.ServiceName = "rodis_service_test"
	manualRemoveResp = manualRemoveReplicas(t, manualRemoveReplicasReq)
	assert.Equal(t, manualRemoveResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("remove replicas ok")
	manualRemoveReplicasReq.ServiceName = "test"
	manualRemoveReplicasReq.TableName = "test"
	manualRemoveResp = manualRemoveReplicas(t, manualRemoveReplicasReq)
	assert.Equal(t, manualRemoveResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(manualRemoveResp.ReplicasResult), 1)

	logging.Info("query task with invalid service")
	queryTaskReq := &pb.QueryTaskRequest{
		ServiceName: "test99",
		TableName:   "test99",
		TaskName:    "checkpoint99",
	}
	queryTaskResp := queryTask(t, queryTaskReq)
	assert.Equal(t, queryTaskResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("query task with keeper dead")
	queryTaskReq.ServiceName = "rodis_service_test"
	queryTaskResp = queryTask(t, queryTaskReq)
	assert.Equal(t, queryTaskResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("query task ok")
	queryTaskReq.ServiceName = "test"
	queryTaskReq.TableName = "test"
	queryTaskReq.TaskName = checkpoint.TASK_NAME
	queryTaskResp = queryTask(t, queryTaskReq)
	assert.Equal(t, queryTaskResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryTaskResp.Task.TaskName, checkpoint.TASK_NAME)

	logging.Info("query task current execution with invalid service")
	queryTaskExecutionReq := &pb.QueryTaskCurrentExecutionRequest{
		ServiceName: "test99",
		TableName:   "test99",
		TaskName:    "checkpoint99",
	}
	queryTaskExecutionResp := queryTaskCurrentExecution(t, queryTaskExecutionReq)
	assert.Equal(t, queryTaskExecutionResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("query task with keeper dead")
	queryTaskExecutionReq.ServiceName = "rodis_service_test"
	queryTaskExecutionResp = queryTaskCurrentExecution(t, queryTaskExecutionReq)
	assert.Equal(t, queryTaskExecutionResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("query task ok")
	queryTaskExecutionReq.ServiceName = "test"
	queryTaskExecutionReq.TableName = "test"
	queryTaskExecutionReq.TaskName = checkpoint.TASK_NAME
	queryTaskExecutionResp = queryTaskCurrentExecution(t, queryTaskExecutionReq)
	assert.Equal(t, queryTaskExecutionResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("delete task side effect with invalid service")
	cleanTaskSideEffectReq := &pb.TriggerDeleteTaskSideEffectRequest{
		ServiceName: "test99",
		TableName:   "test99",
		TaskName:    "checkpoint99",
	}
	cleanTaskSideEffectResp := cleanTaskSideEffect(t, cleanTaskSideEffectReq)
	assert.Equal(t, cleanTaskSideEffectResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("delete task side effect with keeper dead")
	cleanTaskSideEffectReq.ServiceName = "rodis_service_test"
	cleanTaskSideEffectResp = cleanTaskSideEffect(t, cleanTaskSideEffectReq)
	assert.Equal(t, cleanTaskSideEffectResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("delete task side effect ok")
	cleanTaskSideEffectReq.ServiceName = "test"
	cleanTaskSideEffectReq.TableName = "test"
	cleanTaskSideEffectReq.TaskName = checkpoint.TASK_NAME
	cleanTaskSideEffectResp = cleanTaskSideEffect(t, cleanTaskSideEffectReq)
	assert.Equal(t, cleanTaskSideEffectResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("delete task with invalid service")
	deleteTaskReq := &pb.DeleteTaskRequest{
		ServiceName: "invalid_service",
	}
	deleteTaskResp := deleteTask(t, deleteTaskReq)
	assert.Equal(t, deleteTaskResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("delete task with keeper dead")
	deleteTaskReq.ServiceName = "rodis_service_test"
	deleteTaskResp = deleteTask(t, deleteTaskReq)
	assert.Equal(t, deleteTaskResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("delete task ok")
	deleteTaskReq.ServiceName = "test"
	deleteTaskReq.TableName = "test"
	deleteTaskReq.TaskName = checkpoint.TASK_NAME
	deleteTaskReq.CleanTaskSideEffect = false
	deleteTaskReq.CleanDelayMinutes = 0
	deleteTaskResp = deleteTask(t, deleteTaskReq)
	assert.Equal(t, deleteTaskResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("create task with invalid service")
	createTask := &pb.PeriodicTask{
		TaskName: "any_name",
	}
	createTaskResp := operateTask(
		t,
		"invalid_service",
		"test",
		"create_task",
		createTask,
	)
	assert.Equal(t, createTaskResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("create task with keeper dead")
	createTaskResp = operateTask(
		t,
		"rodis_service_test",
		"test",
		"create_task",
		createTask,
	)
	assert.Equal(t, createTaskResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("create task ok")
	createTask.KeepNums = 1
	createTask.TaskName = checkpoint.TASK_NAME
	createTask.FirstTriggerUnixSeconds = time.Now().Unix() + 7200
	createTask.PeriodSeconds = 3600 * 24
	createTask.NotifyMode = pb.TaskNotifyMode_NOTIFY_EVERY_REGION
	createTask.MaxConcurrencyPerNode = 5
	createTaskResp = operateTask(
		t,
		"test",
		"test",
		"create_task",
		createTask,
	)
	assert.Equal(t, createTaskResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("operate task to non-existing service")
	task2 := &pb.PeriodicTask{
		TaskName: "any_name",
	}
	updateTaskResp := operateTask(t, "test2", "test", "update_task", task2)
	assert.Equal(t, updateTaskResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("operate task with keeper dead")
	updateTaskResp = operateTask(t, "rodis_service_test", "test", "update_task", task2)
	assert.Equal(t, updateTaskResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("operate task ok")
	task2.KeepNums = 2
	task2.TaskName = checkpoint.TASK_NAME
	task2.FirstTriggerUnixSeconds = time.Now().Unix() + 7200
	task2.PeriodSeconds = 3600 * 24
	task2.NotifyMode = pb.TaskNotifyMode_NOTIFY_EVERY_REGION
	task2.MaxConcurrencyPerNode = 5
	updateTaskResp = operateTask(t, "test", "test", "update_task", task2)
	assert.Assert(t, updateTaskResp.Status.Code == int32(pb.AdminError_kOk))

	logging.Info("restore table with non-existing service")
	restoreTableReq := pb.RestoreTableRequest{
		ServiceName: "test",
		TableName:   "test",
		RestorePath: "/tmp/error_restore_path",
		Opts: &pb.RestoreOpts{
			MaxConcurrentNodesPerHub:  5,
			MaxConcurrentPartsPerNode: 1,
		},
	}
	restoreTableReq.ServiceName = "test2"
	restoreTableResp := restoreTable(t, &restoreTableReq)
	assert.Equal(t, restoreTableResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("restore table with keeper dead")
	restoreTableReq.ServiceName = "rodis_service_test"
	restoreTableResp = restoreTable(t, &restoreTableReq)
	assert.Equal(t, restoreTableResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("restore table with invalid parameter")
	restoreTableReq.ServiceName = "test"
	restoreTableResp = restoreTable(t, &restoreTableReq)
	assert.Equal(t, restoreTableResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("admin node with service not exist")
	adminResp := adminNode(t, "test2", []string{"127.0.0.1:1234"}, pb.AdminNodeOp_kRestart)
	assert.Equal(t, adminResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("admin node with keeper dead")
	adminResp = adminNode(
		t,
		"rodis_service_test",
		[]string{"127.0.0.1:1234"},
		pb.AdminNodeOp_kRestart,
	)
	assert.Equal(t, adminResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("admin node ok")
	adminResp = adminNode(
		t,
		"test",
		[]string{"127.0.0.1:1234", "127.0.0.1:1235"},
		pb.AdminNodeOp_kRestart,
	)
	assert.Equal(t, adminResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("update node weight, service not exist")
	updateWeightResp := updateNodeWeight(t, "test2", []string{"127.0.0.1:1234"}, []float32{8})
	assert.Equal(t, updateWeightResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("update node weight with keeper dead")
	updateWeightResp = updateNodeWeight(
		t,
		"rodis_service_test",
		[]string{"127.0.0.1:1234"},
		[]float32{8},
	)
	assert.Equal(t, updateWeightResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("update node weight ok")
	updateWeightResp = updateNodeWeight(
		t,
		"test",
		[]string{"127.0.0.1:1234", "127.0.0.1:1235", "127.0.0.1:1236"},
		[]float32{8, 4},
	)
	assert.Equal(t, updateWeightResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("shrink az, service not exists")
	shrinkNodeResp := shrinkAz(t, "test2", "no-exist", -1)
	assert.Equal(t, shrinkNodeResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("shrink az with keeper dead")
	shrinkNodeResp = shrinkAz(t, "rodis_service_test", "YZ", -1)
	assert.Equal(t, shrinkNodeResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("shrink az, ok")
	shrinkNodeResp = shrinkAz(t, "test", "YZ", 3)
	assert.Equal(t, shrinkNodeResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("expand azs, service not exists")
	expandResp := expandAzs(t, "test2", map[string]int{"YZ": 3})
	assert.Equal(t, expandResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("expand azs with keeper dead")
	expandResp = expandAzs(t, "rodis_service_test", map[string]int{"YZ": 3})
	assert.Equal(t, expandResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("expand az ok")
	expandResp = expandAzs(t, "test", map[string]int{"YZ": 3})
	assert.Equal(t, expandResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("replace nodes, service not exists")
	replaceResp := replaceNodes(t, "test2", map[string]string{"127.0.0.1:1001": "127.0.0.2:1001"})
	assert.Equal(t, replaceResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("replace nodes with keeper dead")
	replaceResp = replaceNodes(
		t,
		"rodis_service_test",
		map[string]string{"127.0.0.1:1001": "127.0.0.2:1001"},
	)
	assert.Equal(t, replaceResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("replace nodes, parameter not valid")
	replaceResp = replaceNodes(t, "test", map[string]string{"127.0.0.1:1001": "127.0.0.1:1001"})
	assert.Equal(t, replaceResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("remove watcher of expand az, service not exists")
	removeWatcherResp := removeWatcher(t, "test2", watcher.AzSizeWatcherName)
	assert.Equal(t, removeWatcherResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("remove watcher of expand az with keeper dead")
	removeWatcherResp = removeWatcher(t, "rodis_service_test", watcher.AzSizeWatcherName)
	assert.Equal(t, removeWatcherResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("remove watcher of expand az succeed")
	removeWatcherResp = removeWatcher(t, "test", watcher.AzSizeWatcherName)
	assert.Equal(t, removeWatcherResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("assign hub, service not exists")
	assignResp := assignHub(t, "test2", "127.0.0.1:1001", "yz2")
	assert.Equal(t, assignResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("assign hub with keeper dead")
	assignResp = assignHub(t, "rodis_service_test", "127.0.0.1:1001", "yz2")
	assert.Equal(t, assignResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("assign hub, parameter not valid")
	assignResp = assignHub(t, "test", "127.0.0.1:1001", "yz2")
	assert.Equal(t, assignResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("list node, service not exist")
	listNodesReq := &pb.ListNodesRequest{
		ServiceName: "test2",
		Az:          "",
		HubName:     "",
		TableName:   "",
	}
	listNodesResp := listNodes(t, listNodesReq)
	assert.Equal(t, listNodesResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("list node with keeper dead")
	listNodesReq.ServiceName = "rodis_service_test"
	listNodesResp = listNodes(t, listNodesReq)
	assert.Equal(t, listNodesResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("list nodes")
	listNodesReq.ServiceName = "test"
	listNodesResp = listNodes(t, listNodesReq)
	assert.Equal(t, listNodesResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, is.Len(listNodesResp.Nodes, 0))

	logging.Info("query nodes, service not exists")
	queryNodesResp := queryNodesInfo(t, "test2", "127.0.0.1:1234,127.0.0.1:1235")
	assert.Equal(t, queryNodesResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("query nodes with keeper dead")
	queryNodesResp = queryNodesInfo(t, "rodis_service_test", "127.0.0.1:1234,127.0.0.1:1235")
	assert.Equal(t, queryNodesResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("query nodes, nodes not exists")
	queryNodesResp = queryNodesInfo(t, "test", "127.0.0.1:1234,127.0.0.1:1235")
	assert.Equal(t, queryNodesResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("query node, service not exists")
	queryNodeReq := &pb.QueryNodeInfoRequest{
		ServiceName: "test2",
		NodeName:    "127.0.0.1",
		Port:        1234,
		TableName:   "",
		OnlyBrief:   false,
		MatchPort:   false,
	}
	queryResp := queryNodeInfo(t, queryNodeReq)
	assert.Equal(t, queryResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("query node with keeper dead")
	queryNodeReq.ServiceName = "rodis_service_test"
	queryResp = queryNodeInfo(t, queryNodeReq)
	assert.Equal(t, queryResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("query node, node not exists")
	queryNodeReq.ServiceName = "test"
	queryNodeReq.NodeName = "127.0.0.1"
	queryNodeReq.Port = 1234
	queryResp = queryNodeInfo(t, queryNodeReq)
	assert.Equal(t, queryResp.Status.Code, int32(pb.AdminError_kNodeNotExisting))

	logging.Info("enable a not-existing service")
	switchSchedResp := switchScheduler(t, "invalid_name", true)
	assert.Equal(t, switchSchedResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("enable service scheduler with keeper dead")
	switchSchedResp = switchScheduler(t, "rodis_service_test", true)
	assert.Equal(t, switchSchedResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("enable service scheduler")
	switchSchedResp = switchScheduler(t, "test", true)
	assert.Equal(t, switchSchedResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("query sched status now")
	queryServiceResp = queryService(t, "test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, queryServiceResp.ScheduleEnabled)

	logging.Info("disable sched again")
	switchSchedResp = switchScheduler(t, "test", false)
	assert.Equal(t, switchSchedResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("query after disabled")
	queryServiceResp = queryService(t, "test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, !queryServiceResp.ScheduleEnabled)

	logging.Info("switch kess poller with a not-existing service")
	switchKessPollerResp := switchKessPoller(t, "invalid_name", true)
	assert.Equal(t, switchKessPollerResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("switch kess poller with keeper dead")
	switchKessPollerResp = switchKessPoller(t, "rodis_service_test", true)
	assert.Equal(t, switchKessPollerResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("switch kess poller ok")
	switchKessPollerResp = switchKessPoller(t, "test", true)
	assert.Equal(t, switchKessPollerResp.Status.Code, int32(pb.AdminError_kOk))

	//table : test and test10
	logging.Info("delete table with invalid service name")
	delTableResp := deleteTable(t, "invalid_service", "test")
	assert.Equal(t, delTableResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("delete table with keeper dead")
	delTableResp = deleteTable(t, "rodis_service_test", "invalid_table")
	assert.Equal(t, delTableResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("delete table")
	delTableResp = deleteTable(t, "test", "test10")
	assert.Equal(t, delTableResp.Status.Code, int32(pb.AdminError_kOk))
	delTableResp = deleteTable(t, "test", "test")
	assert.Equal(t, delTableResp.Status.Code, int32(pb.AdminError_kOk))
	time.Sleep(time.Second)

	logging.Info("list all table with keeper dead")
	listTblResp = listTables(t, "")
	assert.Equal(t, listTblResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("delete service not-existing service")
	delResp := deleteService(t, "test2")
	assert.Equal(t, delResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("delete service with keeper dead")
	delResp = deleteService(t, "rodis_service_test")
	assert.Equal(t, delResp.Status.Code, int32(pb.AdminError_kHttpPostFailed))

	logging.Info("delete service ok")
	delResp = deleteService(t, "test")
	assert.Equal(t, delResp.Status.Code, int32(pb.AdminError_kOk))
	time.Sleep(time.Second)
}
