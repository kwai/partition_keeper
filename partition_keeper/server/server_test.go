package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/checkpoint"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/watcher"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"github.com/go-zookeeper/zk"
	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
)

var (
	serverTestZkPrefix = utils.SpliceZkRootPath("/test/pk/server")
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

	ans := zkStore.RecursiveDelete(context.Background(), serverTestZkPrefix)
	assert.Assert(t, ans)
}

func startTestServer(port int, pollLeaderMillis int) *Server {
	s := CreateServer(
		WithZk([]string{"127.0.0.1:2181"}),
		WithZkPrefix(serverTestZkPrefix),
		WithHttpPort(port),
		WithNamespace("test_namespace"),
		WithPollLeaderMillis(pollLeaderMillis),
		WithAllowServiceType(map[pb.ServiceType]bool{pb.ServiceType_colossusdb_dummy: true}),
		WithServiceName("colossusdbUnitTestServer"),
	)
	go s.Start()
	return s
}

func postToMethod(t *testing.T, method string, req, resp proto.Message) {
	err := utils.HttpPostJson("http://127.0.0.1:6060/v1/"+method, nil, nil, req, resp)
	assert.NilError(t, err, "got error: %v", err)
}

func getFromMethod(t *testing.T, method string, args map[string]string, resp proto.Message) {
	err := utils.HttpGet(
		"http://127.0.0.1:6060/v1/"+method,
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

func splitTable(t *testing.T, req *pb.SplitTableRequest) *pb.ErrorStatusResponse {
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "split_table", req, resp)
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

func cancelExpandAzs(t *testing.T, serviceName string, azs []string) *pb.ErrorStatusResponse {
	req := &pb.CancelExpandAzsRequest{
		ServiceName: serviceName,
		Azs:         azs,
	}
	resp := &pb.ErrorStatusResponse{}
	postToMethod(t, "cancel_expand_azs", req, resp)
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

func listNodes(t *testing.T, serviceName string) *pb.ListNodesResponse {
	resp := &pb.ListNodesResponse{}
	getFromMethod(t, "list_nodes", map[string]string{
		"service_name": serviceName,
	}, resp)
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

func queryNodeInfo(t *testing.T, serviceName string, node string) *pb.QueryNodeInfoResponse {
	n := utils.FromHostPort(node)
	resp := &pb.QueryNodeInfoResponse{}
	getFromMethod(t, "query_node_info", map[string]string{
		"service_name": serviceName,
		"node_name":    n.NodeName,
		"port":         fmt.Sprintf("%d", n.Port),
	}, resp)
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

func TestServer(t *testing.T) {
	cleanZookeeper(t)
	s1 := startTestServer(7070, 500)
	defer s1.Stop()

	// TODO: maybe we can refactor here to let sever notify us when http port is
	// listened successfully
	time.Sleep(time.Second * 1)

	// well, 7070 will be leader and 6060 will be follower
	s2 := startTestServer(6060, 500)
	defer s2.Stop()
	time.Sleep(time.Second * 3)

	logging.Info("create service wih invalid name")
	createResp := createService(t, "invalid#name", []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
		{Name: "yz2", Az: "YZ"},
		{Name: "zw1", Az: "ZW"},
		{Name: "zw2", Az: "ZW"},
	}, pb.ServiceType_colossusdb_dummy, pb.NodeFailureDomainType_PROCESS)
	assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	//logging.Info("create service wih empty hub list")
	//createResp = createService(
	//	t,
	//	"test",
	//	[]*pb.ReplicaHub{},
	//	pb.ServiceType_colossusdb_dummy,
	//	pb.NodeFailureDomainType_PROCESS)
	//assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("create service with invalid az name")
	createResp = createService(t, "test", []*pb.ReplicaHub{
		{Name: "yz1", Az: "AMERICA"},
		{Name: "yz2", Az: "AMERICA"},
	}, pb.ServiceType_colossusdb_dummy, pb.NodeFailureDomainType_PROCESS,
	)
	assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("create service with conflict hublist")
	createResp = createService(t, "test", []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
		{Name: "yz1", Az: "YZ"},
	}, pb.ServiceType_colossusdb_dummy, pb.NodeFailureDomainType_PROCESS)
	assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("create service invalid service type")
	createResp = createService(t, "test", []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
		{Name: "yz2", Az: "YZ"},
	}, pb.ServiceType_invalid, pb.NodeFailureDomainType_PROCESS)
	assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("create service with unsupported service type")
	createResp = createService(t, "test", []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
		{Name: "yz2", Az: "YZ"},
	}, pb.ServiceType_colossusdb_embedding_server, pb.NodeFailureDomainType_PROCESS)
	assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("list services")
	listServicesResp := listServices(t)
	assert.Equal(
		t,
		listServicesResp.Status.Code,
		int32(pb.AdminError_kOk),
		"status: %v",
		listServicesResp.Status.Message,
	)
	assert.Assert(t, is.Len(listServicesResp.ServiceNames, 0))

	logging.Info("create service")
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

	logging.Info("list services")
	listServicesResp = listServices(t)
	assert.Equal(t, listServicesResp.Status.Code, int32(pb.AdminError_kOk))
	assert.DeepEqual(t, listServicesResp.ServiceNames, []string{"test"})

	queryServiceResp := queryService(t, "test2")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	queryServiceResp = queryService(t, "test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, utils.HubsEqual(queryServiceResp.NodesHubs, hubs))
	assert.Equal(t, queryServiceResp.ServiceType, pb.ServiceType_colossusdb_dummy)
	assert.Equal(t, queryServiceResp.StaticIndexed, false)
	assert.Equal(t, queryServiceResp.SchedOpts.MaxSchedRatio, int32(10))
	assert.Equal(t, queryServiceResp.SchedOpts.EnablePrimaryScheduler, true)

	logging.Info("update schedule options")
	updateSchedOptResp := updateScheduleOptions(
		t,
		"test",
		&pb.ScheduleOptions{EnablePrimaryScheduler: false, MaxSchedRatio: 100},
		[]string{"enable_primary_scheduler", "max_sched_ratio"},
	)
	assert.Equal(t, updateSchedOptResp.Status.Code, int32(pb.AdminError_kOk))
	queryServiceResp = queryService(t, "test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, queryServiceResp.SchedOpts.MaxSchedRatio, int32(100))
	assert.Equal(t, queryServiceResp.SchedOpts.EnablePrimaryScheduler, false)

	logging.Info("create service with same name")
	createResp = createService(t, "test", []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
	}, pb.ServiceType_colossusdb_dummy, pb.NodeFailureDomainType_PROCESS)
	assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kServiceExists))

	logging.Info("give hints to non-existing service")
	giveHintsResp := giveHints(
		t,
		"test2",
		map[string]string{"dev-huyifan03-02.dev.kwaidc.com:1001": "zw1"},
		true,
	)
	assert.Equal(t, giveHintsResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("give hints succeed")
	giveHintsResp = giveHints(
		t,
		"test",
		map[string]string{"dev-huyifan03-02.dev.kwaidc.com:1001": "zw1"},
		true,
	)
	assert.Equal(t, giveHintsResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("recall hints to non-existing service")
	recallHintsResp := recallHints(
		t,
		"test2",
		[]string{"dev-huyifan03-02.dev.kwaidc.com:1001"},
		true,
	)
	assert.Equal(t, recallHintsResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("recall hints succeed")
	recallHintsResp = recallHints(t, "test", []string{"dev-huyifan03-02.dev.kwaidc.com:1001"}, true)
	assert.Equal(t, recallHintsResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("add hubs for non-existing service")
	addHubsResp := addHubs(t, "test2", []*pb.ReplicaHub{
		{Name: "yz3", Az: "YZ"},
		{Name: "zw3", Az: "ZW"},
	})
	assert.Equal(t, addHubsResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	queryServiceResp = queryService(t, "test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, utils.HubsEqual(queryServiceResp.NodesHubs, hubs))
	assert.Equal(t, queryServiceResp.FailureDomainType, pb.NodeFailureDomainType_PROCESS)

	logging.Info("add hubs succeed")
	addHubsResp = addHubs(t, "test", []*pb.ReplicaHub{
		{Name: "yz3", Az: "YZ"},
		{Name: "zw3", Az: "ZW"},
	})

	assert.Equal(t, addHubsResp.Status.Code, int32(pb.AdminError_kOk))
	newHubs := []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
		{Name: "yz2", Az: "YZ"},
		{Name: "yz3", Az: "YZ"},
		{Name: "zw1", Az: "ZW"},
		{Name: "zw2", Az: "ZW"},
		{Name: "zw3", Az: "ZW"},
	}

	queryServiceResp = queryService(t, "test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, utils.HubsEqual(queryServiceResp.NodesHubs, newHubs))

	logging.Info("update hubs with non-existing service")
	updateHubsResp := updateHubs(t, "test2", []*pb.ReplicaHub{
		{Name: "yz3", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kSecondary}},
	})
	assert.Equal(t, updateHubsResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("update hubs with non-existing hub name")
	updateHubsResp = updateHubs(t, "test", []*pb.ReplicaHub{
		{Name: "yz3", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kSecondary}},
		{Name: "yz4", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kSecondary}},
	})
	assert.Equal(t, updateHubsResp.Status.Code, int32(pb.AdminError_kInvalidParameter))
	queryServiceResp = queryService(t, "test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, utils.HubsEqual(queryServiceResp.NodesHubs, newHubs))

	logging.Info("update hubs succeed")
	updateHubsResp = updateHubs(t, "test", []*pb.ReplicaHub{
		{Name: "yz3", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kSecondary}},
		{Name: "zw3", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kPrimary}},
	})
	assert.Equal(t, updateHubsResp.Status.Code, int32(pb.AdminError_kOk))
	queryServiceResp = queryService(t, "test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kOk))

	newHubs[2].DisallowedRoles = []pb.ReplicaRole{pb.ReplicaRole_kSecondary}
	newHubs[5].DisallowedRoles = []pb.ReplicaRole{pb.ReplicaRole_kPrimary}
	assert.Assert(t, utils.HubsEqual(queryServiceResp.NodesHubs, newHubs))

	logging.Info("update hubs with clean disallow_roles")
	updateHubsResp = updateHubs(t, "test", []*pb.ReplicaHub{
		{Name: "yz3"},
		{Name: "zw3"},
	})
	assert.Equal(t, updateHubsResp.Status.Code, int32(pb.AdminError_kOk))
	queryServiceResp = queryService(t, "test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kOk))

	newHubs[2].DisallowedRoles = nil
	newHubs[5].DisallowedRoles = nil
	assert.Assert(t, utils.HubsEqual(queryServiceResp.NodesHubs, newHubs))

	logging.Info("remove hubs for non-existing service")
	removeHubsResp := removeHubs(t, "test2", []*pb.ReplicaHub{
		{Name: "yz3", Az: "YZ"},
		{Name: "zw3", Az: "ZW"},
	})
	assert.Equal(t, removeHubsResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	queryServiceResp = queryService(t, "test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, utils.HubsEqual(queryServiceResp.NodesHubs, newHubs))

	logging.Info("remove hubs succeed")
	removeHubsResp = removeHubs(t, "test", []*pb.ReplicaHub{
		{Name: "yz3", Az: "YZ"},
		{Name: "zw3", Az: "ZW"},
	})
	assert.Equal(t, removeHubsResp.Status.Code, int32(pb.AdminError_kOk))

	queryServiceResp = queryService(t, "test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, utils.HubsEqual(queryServiceResp.NodesHubs, hubs))

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

	logging.Info("create table with invalid partition count")
	createTblReq.ServiceName = "test"
	createTblReq.Table.TableName = "test"
	createTblReq.Table.PartsCount = 0
	createTblResp = createTable(t, createTblReq)
	assert.Equal(t, createTblResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

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

	logging.Info("update table ok")
	updateTableReq.ServiceName = "test"
	updateResp = updateTable(t, updateTableReq)
	assert.Equal(t, updateResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("list table with non-exist service")
	listTblResp := listTables(t, "test2")
	assert.Equal(t, listTblResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("list table")
	listTblResp = listTables(t, "test")
	assert.Equal(t, listTblResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, is.Len(listTblResp.Tables, 2))
	table := listTblResp.Tables[0]
	assert.Assert(t, table.TableId > 1001)
	assert.Equal(t, table.TableName, "test")
	assert.Equal(t, table.PartsCount, int32(32))
	assert.Equal(t, table.JsonArgs, createTblReq.Table.JsonArgs)
	assert.Equal(t, table.KconfPath, createTblReq.Table.KconfPath)

	table = listTblResp.Tables[1]
	assert.Assert(t, table.TableId > listTblResp.Tables[0].TableId)
	assert.Equal(t, table.TableName, "test10")
	assert.Equal(t, table.PartsCount, int32(16))
	assert.Equal(t, table.JsonArgs, createTblReq.Table.JsonArgs)

	listAllTblResp := listTables(t, "")
	assert.Assert(t, proto.Equal(listTblResp, listAllTblResp))

	logging.Info("query table")
	queryTableReq := &pb.QueryTableRequest{
		ServiceName:    "test",
		TableName:      "test",
		WithPartitions: true,
		WithTasks:      true,
	}
	queryTableResp := queryTable(t, queryTableReq)
	assert.Assert(t, proto.Equal(queryTableResp.Table, listTblResp.Tables[0]))
	assert.Assert(t, is.Len(queryTableResp.Partitions, 32))
	assert.Assert(t, is.Len(queryTableResp.Tasks, 1))
	task := queryTableResp.Tasks[0]
	assert.Equal(t, task.TaskName, checkpoint.TASK_NAME)
	assert.Equal(t, task.PeriodSeconds, int64(86400))
	assert.Equal(t, task.MaxConcurrencyPerNode, int32(5))
	assert.Assert(t, task.Args == nil)

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

	logging.Info("remove replicas of non-existing table")
	manualRemoveReplicasReq.ServiceName = "test"
	manualRemoveReplicasReq.TableName = "non-existing-table"
	manualRemoveResp = manualRemoveReplicas(t, manualRemoveReplicasReq)
	assert.Equal(t, manualRemoveResp.Status.Code, int32(pb.AdminError_kTableNotExists))

	logging.Info("remove replicas service & table valid")
	manualRemoveReplicasReq.ServiceName = "test"
	manualRemoveReplicasReq.TableName = "test"
	manualRemoveResp = manualRemoveReplicas(t, manualRemoveReplicasReq)
	assert.Equal(t, manualRemoveResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(manualRemoveResp.ReplicasResult), 1)

	logging.Info("operate task to non-existing service")
	task2 := &pb.PeriodicTask{
		TaskName: "any_name",
	}
	updateTaskResp := operateTask(t, "test2", "test", "update_task", task2)
	assert.Equal(t, updateTaskResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("operate task to valid service")
	updateTaskResp = operateTask(t, "test", "test", "update_task", task)
	assert.Assert(t, updateTaskResp.Status.Code != int32(pb.AdminError_kServiceNotExists))

	logging.Info("query not exist service")
	queryTableReq = &pb.QueryTableRequest{
		ServiceName:    "test2",
		TableName:      "test",
		WithPartitions: true,
		WithTasks:      true,
	}
	queryTableResp = queryTable(t, queryTableReq)
	assert.Equal(t, queryTableResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("query not exist table")
	queryTableReq.ServiceName = "test"
	queryTableReq.TableName = "test2"
	queryTableResp = queryTable(t, queryTableReq)
	assert.Equal(t, queryTableResp.Status.Code, int32(pb.AdminError_kTableNotExists))

	JsonArgs := `{
		"btq_prefix": "dummy",
		"zk_hosts": [],
		"cluster_prefix": "/test/rodis/test",
		"enable_publish_zk": false
	}`
	updateTableJsonResp := updateTableJsonArgs(t, "test", "test", JsonArgs)
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

	logging.Info("query task with invalid service")
	queryTaskReq := &pb.QueryTaskRequest{
		ServiceName: "test99",
		TableName:   "test99",
		TaskName:    "checkpoint99",
	}
	queryTaskResp := queryTask(t, queryTaskReq)
	assert.Equal(t, queryTaskResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("query task with invalid table")
	queryTaskReq.ServiceName = "test"
	queryTaskResp = queryTask(t, queryTaskReq)
	assert.Equal(t, queryTaskResp.Status.Code, int32(pb.AdminError_kTableNotExists))

	logging.Info("query task with invalid task")
	queryTaskReq.TableName = "test"
	queryTaskResp = queryTask(t, queryTaskReq)
	assert.Equal(t, queryTaskResp.Status.Code, int32(pb.AdminError_kTaskNotExists))

	logging.Info("query task ok")
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

	logging.Info("query task with invalid table")
	queryTaskExecutionReq.ServiceName = "test"
	queryTaskExecutionResp = queryTaskCurrentExecution(t, queryTaskExecutionReq)
	assert.Equal(t, queryTaskExecutionResp.Status.Code, int32(pb.AdminError_kTableNotExists))

	logging.Info("query task with invalid task")
	queryTaskExecutionReq.TableName = "test"
	queryTaskExecutionResp = queryTaskCurrentExecution(t, queryTaskExecutionReq)
	assert.Equal(t, queryTaskExecutionResp.Status.Code, int32(pb.AdminError_kTaskNotExists))

	logging.Info("query task ok")
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
	logging.Info("delete task side effect with invalid table")
	cleanTaskSideEffectReq.ServiceName = "test"
	cleanTaskSideEffectResp = cleanTaskSideEffect(t, cleanTaskSideEffectReq)
	assert.Equal(t, cleanTaskSideEffectResp.Status.Code, int32(pb.AdminError_kTableNotExists))
	logging.Info("delete task side effect with invalid task")
	cleanTaskSideEffectReq.TableName = "test"
	cleanTaskSideEffectResp = cleanTaskSideEffect(t, cleanTaskSideEffectReq)
	assert.Equal(t, cleanTaskSideEffectResp.Status.Code, int32(pb.AdminError_kTaskNotExists))
	logging.Info("delete task side effect ok")
	cleanTaskSideEffectReq.TaskName = checkpoint.TASK_NAME
	cleanTaskSideEffectResp = cleanTaskSideEffect(t, cleanTaskSideEffectReq)
	assert.Equal(t, cleanTaskSideEffectResp.Status.Code, int32(pb.AdminError_kOk))

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

	logging.Info("create task with invalid table")
	createTaskResp = operateTask(
		t,
		"test",
		"invalid_table",
		"create_task",
		createTask,
	)
	assert.Equal(t, createTaskResp.Status.Code, int32(pb.AdminError_kTableNotExists))

	logging.Info("create task with invalid first trigger unix seconds")
	createTask.FirstTriggerUnixSeconds = 5
	createTaskResp = operateTask(
		t,
		"test",
		"test",
		"create_task",
		createTask,
	)
	assert.Equal(t, createTaskResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("create task with invalid period seconds")
	createTask.FirstTriggerUnixSeconds = time.Now().Unix()
	createTask.PeriodSeconds = -1
	createTaskResp = operateTask(
		t,
		"test",
		"test",
		"create_task",
		createTask,
	)
	assert.Equal(t, createTaskResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	createTask.PeriodSeconds = 3600 * 24 * 400
	createTaskResp = operateTask(
		t,
		"test",
		"test",
		"create_task",
		createTask,
	)
	assert.Equal(t, createTaskResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("create task with invalid concurrency per node")
	createTask.PeriodSeconds = 60
	createTask.MaxConcurrencyPerNode = -1
	createTaskResp = operateTask(
		t,
		"test",
		"test",
		"create_task",
		createTask,
	)
	assert.Equal(t, createTaskResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("create task with invalid keep nums")
	createTask.MaxConcurrencyPerNode = 1
	createTask.KeepNums = -10
	createTaskResp = operateTask(
		t,
		"test",
		"test",
		"create_task",
		createTask,
	)
	assert.Equal(t, createTaskResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("create task with unsupported task name")
	createTask.KeepNums = 1
	createTask.TaskName = "error_task"
	createTaskResp = operateTask(
		t,
		"test",
		"test",
		"create_task",
		createTask,
	)
	assert.Equal(t, createTaskResp.Status.Code, int32(pb.AdminError_kUnsupportedTaskName))

	logging.Info("create task with exists task name")
	createTask.KeepNums = 1
	createTask.TaskName = checkpoint.TASK_NAME
	createTaskResp = operateTask(
		t,
		"test",
		"test",
		"create_task",
		createTask,
	)
	assert.Equal(t, createTaskResp.Status.Code, int32(pb.AdminError_kTaskExists))

	logging.Info("delete task with invalid service")
	deleteTaskReq := &pb.DeleteTaskRequest{
		ServiceName: "invalid_service",
	}
	deleteTaskResp := deleteTask(t, deleteTaskReq)
	assert.Equal(t, deleteTaskResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("delete task with invalid table")
	deleteTaskReq.ServiceName = "test"
	deleteTaskReq.TableName = "invalid_table"
	deleteTaskResp = deleteTask(t, deleteTaskReq)
	assert.Equal(t, deleteTaskResp.Status.Code, int32(pb.AdminError_kTableNotExists))

	logging.Info("delete task with invalid task")
	deleteTaskReq.TableName = "test"
	deleteTaskReq.TaskName = "invalid_task"
	deleteTaskResp = deleteTask(t, deleteTaskReq)
	assert.Equal(t, deleteTaskResp.Status.Code, int32(pb.AdminError_kTaskNotExists))

	logging.Info("delete task with invalid clean delay minutes")
	deleteTaskReq.TaskName = checkpoint.TASK_NAME
	deleteTaskReq.CleanTaskSideEffect = true
	deleteTaskReq.CleanDelayMinutes = -1
	deleteTaskResp = deleteTask(t, deleteTaskReq)
	assert.Equal(t, deleteTaskResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("delete task ok")
	deleteTaskReq.CleanTaskSideEffect = false
	deleteTaskReq.CleanDelayMinutes = 0
	deleteTaskResp = deleteTask(t, deleteTaskReq)
	assert.Equal(t, deleteTaskResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("create task ok")
	createTask.KeepNums = 1
	createTask.TaskName = checkpoint.TASK_NAME
	createTaskResp = operateTask(
		t,
		"test",
		"test",
		"create_task",
		createTask,
	)
	assert.Equal(t, createTaskResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("restore table with invalid MaxConcurrentNodesPerHub")
	restoreTableReq := pb.RestoreTableRequest{
		ServiceName: "test",
		TableName:   "test",
		RestorePath: "/tmp/error_restore_path",
		Opts: &pb.RestoreOpts{
			MaxConcurrentNodesPerHub:  -5,
			MaxConcurrentPartsPerNode: 1,
		},
	}
	restoreTableResp := restoreTable(t, &restoreTableReq)
	assert.Equal(t, restoreTableResp.Status.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Assert(
		t,
		strings.Contains(
			restoreTableResp.Status.Message,
			"MaxConcurrentNodesPerHub should larger than 0 or equal to -1",
		),
	)

	logging.Info("restore table with invalid MaxConcurrentPartsPerNode")
	restoreTableReq.Opts.MaxConcurrentNodesPerHub = -1
	restoreTableReq.Opts.MaxConcurrentPartsPerNode = -5
	restoreTableResp = restoreTable(t, &restoreTableReq)
	assert.Equal(t, restoreTableResp.Status.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Assert(
		t,
		strings.Contains(
			restoreTableResp.Status.Message,
			"MaxConcurrentPartsPerNode should larger than 0 or equal to -1",
		),
	)

	logging.Info("restore table with invalid RestorePath")
	restoreTableReq.Opts.MaxConcurrentPartsPerNode = 1
	restoreTableResp = restoreTable(t, &restoreTableReq)
	assert.Equal(t, restoreTableResp.Status.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Assert(t, strings.Contains(restoreTableResp.Status.Message, "invalid restore path"))

	logging.Info("split table with invalid service name")
	splitReq := &pb.SplitTableRequest{
		ServiceName:     "invalid_service_name",
		TableName:       "invalid_table_name",
		NewSplitVersion: 12,
		Options:         &pb.SplitTableOptions{MaxConcurrentParts: 1, DelaySeconds: 1},
	}
	splitResp := splitTable(t, splitReq)
	assert.Equal(t, splitResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("split table with invalid table name")
	splitReq.ServiceName = "test"
	splitResp = splitTable(t, splitReq)
	assert.Equal(t, splitResp.Status.Code, int32(pb.AdminError_kTableNotExists))

	logging.Info("admin node, service not exist")
	adminResp := adminNode(t, "test2", []string{"127.0.0.1:1234"}, pb.AdminNodeOp_kRestart)
	assert.Equal(t, adminResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("admin node, node not exists")
	adminResp = adminNode(
		t,
		"test",
		[]string{"127.0.0.1:1234", "127.0.0.1:1235"},
		pb.AdminNodeOp_kRestart,
	)
	assert.Equal(t, adminResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, is.Len(adminResp.NodeResults, 2))
	for _, res := range adminResp.NodeResults {
		assert.Equal(t, res.Code, int32(pb.AdminError_kNodeNotExisting))
	}

	logging.Info("update node weight, service not exist")
	updateWeightResp := updateNodeWeight(t, "test2", []string{"127.0.0.1:1234"}, []float32{8})
	assert.Equal(t, updateWeightResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("update node weight, node node exist")
	updateWeightResp = updateNodeWeight(
		t,
		"test",
		[]string{"127.0.0.1:1234", "127.0.0.1:1235", "127.0.0.1:1236"},
		[]float32{8, 4},
	)
	assert.Equal(t, updateWeightResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(updateWeightResp.NodeResults), 2)
	for _, res := range updateWeightResp.NodeResults {
		assert.Equal(t, res.Code, int32(pb.AdminError_kNodeNotExisting))
	}

	logging.Info("list node, service not exist")
	listNodesResp := listNodes(t, "test2")
	assert.Equal(t, listNodesResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("list nodes")
	listNodesResp = listNodes(t, "test")
	assert.Equal(t, listNodesResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, is.Len(listNodesResp.Nodes, 0))

	logging.Info("shrink az, service not exists")
	shrinkNodeResp := shrinkAz(t, "test2", "no-exist", -1)
	assert.Equal(t, shrinkNodeResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("shrink az, az not exists")
	shrinkNodeResp = shrinkAz(t, "test", "no-exist", -1)
	assert.Equal(t, shrinkNodeResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("shrink az, invalid count")
	shrinkNodeResp = shrinkAz(t, "test", "YZ", 0)
	assert.Equal(t, shrinkNodeResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("shrink az, succeed")
	shrinkNodeResp = shrinkAz(t, "test", "YZ", 3)
	assert.Equal(t, shrinkNodeResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, is.Len(shrinkNodeResp.Shrinked, 0))

	logging.Info("expand azs, service not exists")
	expandResp := expandAzs(t, "test2", map[string]int{"YZ": 3})
	assert.Equal(t, expandResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("expand azs, az not exists")
	expandResp = expandAzs(t, "test", map[string]int{"TXSGP1": 3})
	assert.Equal(t, expandResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("expand az ok")
	expandResp = expandAzs(t, "test", map[string]int{"YZ": 3})
	assert.Equal(t, expandResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("replace nodes, service not exists")
	replaceResp := replaceNodes(t, "test2", map[string]string{"127.0.0.1:1001": "127.0.0.2:1001"})
	assert.Equal(t, replaceResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("replace nodes, parameter not valid")
	replaceResp = replaceNodes(t, "test", map[string]string{"127.0.0.1:1001": "127.0.0.1:1001"})
	assert.Equal(t, replaceResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("remove watcher of expand az, service not exists")
	removeWatcherResp := removeWatcher(t, "test2", watcher.AzSizeWatcherName)
	assert.Equal(t, removeWatcherResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("remove watcher of expand az succeed")
	removeWatcherResp = removeWatcher(t, "test", watcher.AzSizeWatcherName)
	assert.Equal(t, removeWatcherResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("remove not exists watcher")
	removeWatcherResp = removeWatcher(t, "test", watcher.ReplaceNodesWatcherName)
	assert.Equal(t, removeWatcherResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("cancel expand az, service not exist")
	cancelExpandAzResp := cancelExpandAzs(t, "not_exist_service_name", []string{"YZ_1"})
	assert.Equal(t, cancelExpandAzResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("cancel expand az, watcher not exist")
	cancelExpandAzResp = cancelExpandAzs(t, "test", []string{"YZ_1"})
	assert.Equal(t, cancelExpandAzResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("assign hub, service not exists")
	assignResp := assignHub(t, "test2", "127.0.0.1:1001", "yz2")
	assert.Equal(t, assignResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("assign hub, parameter not valid")
	assignResp = assignHub(t, "test", "127.0.0.1:1001", "yz2")
	assert.Equal(t, assignResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("query nodes, service not exists")
	queryNodesResp := queryNodesInfo(t, "test2", "127.0.0.1:1234,127.0.0.1:1235")
	assert.Equal(t, queryNodesResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("query nodes, nodes not exists")
	queryNodesResp = queryNodesInfo(t, "test", "127.0.0.1:1234,127.0.0.1:1235")
	assert.Equal(t, queryNodesResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(queryNodesResp.Nodes), 2)
	assert.Equal(t, queryNodesResp.Nodes[0].Status.Code, int32(pb.AdminError_kNodeNotExisting))
	assert.Equal(t, queryNodesResp.Nodes[1].Status.Code, int32(pb.AdminError_kNodeNotExisting))

	logging.Info("query node, service not exists")
	queryResp := queryNodeInfo(t, "test2", "127.0.0.1:1234")
	assert.Equal(t, queryResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("query node, node not exists")
	queryResp = queryNodeInfo(t, "test", "127.0.0.1:1234")
	assert.Equal(t, queryResp.Status.Code, int32(pb.AdminError_kNodeNotExisting))

	logging.Info("query scheduler status of no exist service")
	queryServiceResp = queryService(t, "invalid_name")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("a service is disabled by default")
	queryServiceResp = queryService(t, "test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, !queryServiceResp.ScheduleEnabled)

	logging.Info("enable a not-existing service")
	switchSchedResp := switchScheduler(t, "invalid_name", true)
	assert.Equal(t, switchSchedResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

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

	//table : test and test10
	logging.Info("delete table with invalid service name")
	delTableResp := deleteTable(t, "invalid_service", "test")
	assert.Equal(t, delTableResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("delete table with invalid table name")
	delTableResp = deleteTable(t, "test", "invalid_table")
	assert.Equal(t, delTableResp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("delete table")
	delTableResp = deleteTable(t, "test", "test")
	assert.Equal(t, delTableResp.Status.Code, int32(pb.AdminError_kOk))
	delTableResp = deleteTable(t, "test", "test10")
	assert.Equal(t, delTableResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("delete service")
	delResp := deleteService(t, "test")
	for i := 0; i < 50; i++ {
		if delResp.Status.Code == int32(pb.AdminError_kTableNotEmpty) {
			time.Sleep(time.Millisecond * 100)
			delResp = deleteService(t, "test")
		}
	}
	assert.Equal(t, delResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("service will be deleted finally")
	for i := 0; i < 10; i++ {
		delResp = deleteService(t, "test")
		if delResp.Status.Code == int32(pb.AdminError_kOk) {
			break
		}
		assert.Equal(t, delResp.Status.Code, int32(pb.AdminError_kServiceNotInServingState))
		time.Sleep(time.Second)
	}
	assert.Equal(t, delResp.Status.Code, int32(pb.AdminError_kOk))

	logging.Info("query service after delete")
	queryServiceResp = queryService(t, "test")
	assert.Equal(t, queryServiceResp.Status.Code, int32(pb.AdminError_kServiceNotExists))

	logging.Info("list services after delete")
	listServicesResp = listServices(t)
	assert.Equal(t, listServicesResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, is.Len(listServicesResp.ServiceNames, 0))
}

func TestForwardToWrongLeader(t *testing.T) {
	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	zkStore := metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"},
		time.Second*10,
		acl,
		scheme,
		auth,
	)
	defer zkStore.Close()

	ans := zkStore.RecursiveDelete(context.Background(), serverTestZkPrefix)
	assert.Assert(t, ans)

	ans = zkStore.RecursiveCreate(context.Background(), serverTestZkPrefix+"/test_namespace/LOCK")
	assert.Assert(t, ans)

	distLock := zk.NewLock(zkStore.GetRawSession(),
		serverTestZkPrefix+"/test_namespace/LOCK",
		acl,
	)

	hn, _ := os.Hostname()
	err := distLock.LockWithData([]byte(hn + ":7070"))
	assert.NilError(t, err)

	defer distLock.Unlock()

	s1 := startTestServer(7070, 500)
	defer s1.Stop()
	time.Sleep(time.Second * 1)

	// well, both 7070 & 6060 will be follower, but we still think 7070 is leader
	s2 := startTestServer(6060, 500)
	defer s2.Stop()
	time.Sleep(time.Second * 2)

	createResp := createService(t, "invalid#name", []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
		{Name: "yz2", Az: "YZ"},
		{Name: "zw1", Az: "ZW"},
		{Name: "zw2", Az: "ZW"},
	}, pb.ServiceType_colossusdb_dummy, pb.NodeFailureDomainType_PROCESS)
	assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kServerNotLeader))
	assert.Equal(t, createResp.Status.Message, "forwarded not leader")
}

func TestForwardFailAsNoLeader(t *testing.T) {
	cleanZookeeper(t)
	s1 := startTestServer(7070, 5000)
	defer s1.Stop()

	// TODO: maybe we can refactor here to let sever notify us when http port is
	// listened successfully
	time.Sleep(time.Second * 1)

	// well, 7070 will be leader and 6060 will be follower
	s2 := startTestServer(6060, 5000)
	defer s2.Stop()
	time.Sleep(time.Second * 2)
	createResp := createService(t, "invalid#name", []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
		{Name: "yz2", Az: "YZ"},
		{Name: "zw1", Az: "ZW"},
		{Name: "zw2", Az: "ZW"},
	}, pb.ServiceType_colossusdb_dummy, pb.NodeFailureDomainType_PROCESS)
	assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kServerNotLeader))
	assert.Equal(t, createResp.Status.Message, "can't find leader")
}

func TestMustLoadMetaAfterAcquireLock(t *testing.T) {
	cleanZookeeper(t)
	s1 := startTestServer(7070, 500)

	// TODO: maybe we can refactor here to let sever notify us when http port is
	// listened successfully
	time.Sleep(time.Second * 1)

	// well, 7070 will be leader and 6060 will be follower
	s2 := startTestServer(6060, 500)
	defer s2.Stop()
	time.Sleep(time.Second * 3)

	assert.Equal(t, s1.leaderInfo.isLeader, true)
	assert.Equal(t, s2.leaderInfo.isLeader, false)

	hubs := []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
		{Name: "yz2", Az: "YZ"},
		{Name: "zw1", Az: "ZW"},
		{Name: "zw2", Az: "ZW"},
	}
	createResp := createService(
		t,
		"test",
		hubs,
		pb.ServiceType_colossusdb_dummy,
		pb.NodeFailureDomainType_PROCESS,
	)
	assert.Equal(t, createResp.Status.Code, int32(pb.AdminError_kOk))

	createTblReq := &pb.CreateTableRequest{
		ServiceName: "test",
		Table: &pb.Table{
			TableId:    0,
			TableName:  "test",
			HashMethod: "crc32",
			PartsCount: 32,
			JsonArgs:   `{"a": "b"}`,
			KconfPath:  "reco.rodisFea.partitionKeeperHDFSTest",
		},
	}

	createTblResp := createTable(t, createTblReq)
	assert.Equal(t, createTblResp.Status.Code, int32(pb.AdminError_kOk))

	listTablesResp := listTables(t, "test")
	assert.Equal(t, listTablesResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(listTablesResp.Tables), 1)
	assert.Equal(t, listTablesResp.Tables[0].TableName, "test")

	s1.Stop()
	for i := 0; i < 10; i++ {
		if isLeader, _, hasInitialized := s2.getServerStatus(); isLeader && hasInitialized {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	assert.Equal(t, s2.leaderInfo.isLeader, true)

	for i := 0; i < 10; i++ {
		if s2.allServices["test"].state.State != utils.StateNormal {
			time.Sleep(time.Millisecond * 100)
		} else {
			break
		}
	}
	assert.Equal(t, s2.allServices["test"].state.State, utils.StateNormal)

	listTablesResp = listTables(t, "test")
	assert.Equal(t, listTablesResp.Status.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(listTablesResp.Tables), 1)
	assert.Equal(t, listTablesResp.Tables[0].TableName, "test")
}
