package proxy

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/proxy/meta"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/version"
	"google.golang.org/protobuf/proto"
)

var (
	flagHttpPort = flag.Int(
		"proxy_http_port",
		33000,
		"bind port of partition keeper proxy http server",
	)
)

type Proxy struct {
	myself      string
	httpPort    int
	httpServer  *http.Server
	watcher     *Watcher
	meta        *meta.Meta
	status_code int
	states      []string
}

func CreateProxy() *Proxy {
	p := &Proxy{
		httpPort: *flagHttpPort,
	}
	if hostName, err := os.Hostname(); err != nil {
		logging.Fatal("get host name failed: %v", err.Error())
	} else {
		p.myself = fmt.Sprintf("%s:%d", hostName, p.httpPort)
		p.states = []string{"STOPPED", "STARTING", "RUNNING", "STOPPING"}
		p.StatusCodeUpdate(0)
	}
	return p
}

func (p *Proxy) StatusCodeUpdate(status_code int) {
	if status_code < 0 || status_code >= 4 {
		logging.Info("server status code not in range")
		return
	}
	p.status_code = status_code
}

func (p *Proxy) GetStatusCode() int {
	return p.status_code
}

func (p *Proxy) GetStatusString() string {
	status_code := p.GetStatusCode()
	return p.states[status_code]
}

func (p *Proxy) Start() {
	logging.Info("%s: start proxy with git commit id: %s", p.myself, version.GitCommitId)
	p.meta = meta.NewMeta()
	p.meta.Initialize()

	p.watcher = NewWatcher(p.meta)
	p.watcher.Start()
	p.startHttpServer()
}

func (p *Proxy) Stop() {
	p.StatusCodeUpdate(3)
	p.httpServer.Close()
	p.StatusCodeUpdate(0)
	p.watcher.Stop()
}

type parseRequest func(r *http.Request) (proto.Message, *pb.ErrorStatus)
type handleRequest func(string, string, proto.Message) (proto.Message, string)

func (p *Proxy) handleRequest(
	w http.ResponseWriter,
	r *http.Request,
	parse parseRequest,
	handle handleRequest,
	path string,
	funcName string,
) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)

	var output proto.Message = nil
	defer func() {
		data, err := json.MarshalIndent(output, "", "  ")
		if err != nil {
			logging.Error("%s: marshal data error: %s", p.myself, err.Error())
			panic(err)
		} else {
			logging.Verbose(1, "%s: response data: %s", p.myself, string(data))
			w.Write(data)
		}
	}()

	request, err := parse(r)
	if err != nil {
		logging.Info("%s: parse request failed: %s", p.myself, err.Message)
		output = &pb.ErrorStatusResponse{
			Status: err,
		}
		return
	}
	logging.Info("%s: got request url %s req %v from %s", p.myself, r.URL, request, r.RemoteAddr)

	start := time.Now()
	output, serviceName := handle(path, r.URL.RawQuery, request)
	elapse := time.Since(start).Microseconds()
	if serviceName == "" {
		serviceName = "InternalPerfService"
	}
	third_party.PerfLog2(
		"reco",
		"reco.colossusdb.keeper_proxy",
		"req_cost",
		serviceName,
		funcName,
		uint64(elapse),
	)
	third_party.PerfLog2(
		"reco",
		"reco.colossusdb.keeper_proxy",
		"resp_size",
		serviceName,
		funcName,
		uint64(proto.Size(output)),
	)
}

func (p *Proxy) registerHttpHandle(
	router *mux.Router,
	name, method, path string,
	parse parseRequest,
	handle handleRequest,
) {
	handleFunc := func(w http.ResponseWriter, r *http.Request) {
		p.handleRequest(w, r, parse, handle, path, name)
	}

	router.Name(name).
		Methods(strings.ToUpper(method)).
		Path(path).
		HandlerFunc(handleFunc)
}

func (p *Proxy) getUrl(serviceName string) (string, *pb.ErrorStatus) {
	url := p.meta.GetUrlByService(serviceName)
	if url == "" {
		return url, pb.AdminErrorMsg(
			pb.AdminError_kServiceNotExists,
			"service %s not exists",
			serviceName,
		)
	}
	return url, nil
}

func (p *Proxy) listServices(method, params string, input proto.Message) (proto.Message, string) {
	resp := &pb.ListServicesResponse{
		Status:       &pb.ErrorStatus{Code: 0},
		ServiceNames: p.meta.GetServices(),
	}
	return resp, ""
}

func (p *Proxy) createService(method, params string, input proto.Message) (proto.Message, string) {
	req := input.(*pb.CreateServiceRequest)
	resp := &pb.ErrorStatusResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}
	url := p.meta.GetUrlByService(req.ServiceName)
	if url != "" {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kServiceExists,
			"service %s already exists",
			req.ServiceName,
		)
		return resp, req.ServiceName
	}

	{
		// check all keeper
		urls := p.meta.GetAllUrls()
		param := fmt.Sprintf("service_name=%s", req.ServiceName)
		responseChannel := make(chan pb.AdminError_Code, len(urls))
		for _, url := range urls {
			go func(u string) {
				queryResp := &pb.QueryServiceResponse{}
				err := p.get(u, "/v1/query_service", param, queryResp)
				if err != nil {
					responseChannel <- pb.AdminError_kHttpPostFailed
					return
				}

				if pb.AdminError_Code(
					queryResp.Status.GetCode(),
				) != pb.AdminError_kServiceNotExists {
					responseChannel <- pb.AdminError_kServiceExists
					return
				}
				responseChannel <- pb.AdminError_kOk
			}(url)
		}

		for i := 0; i < len(urls); i++ {
			code := <-responseChannel
			if code == pb.AdminError_kHttpPostFailed {
				resp.Status = pb.AdminErrorMsg(
					pb.AdminError_kHttpPostFailed,
					"proxy post %s to keeper failed, service:%s",
					method,
					req.ServiceName,
				)
				return resp, req.ServiceName
			}
			if code == pb.AdminError_kServiceExists {
				resp.Status = pb.AdminErrorMsg(
					pb.AdminError_kServiceExists,
					"service %s already exists",
					req.ServiceName,
				)
				return resp, req.ServiceName
			}
		}
	}

	url = p.meta.GetUrlByType(req.ServiceType)
	if url == "" {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"service %s Invalid service type:%s, please contact the administrator to check the service type",
			req.ServiceName,
			pb.ServiceType_name[int32(req.ServiceType)],
		)
		return resp, req.ServiceName
	}

	err := p.post(url, method, req.ServiceName, req, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, service:%s, err:%s",
			method,
			url,
			req.ServiceName,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) createTable(method, params string, input proto.Message) (proto.Message, string) {
	req := input.(*pb.CreateTableRequest)
	resp := &pb.ErrorStatusResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	url, errStatus := p.getUrl(req.ServiceName)
	if url == "" {
		resp.Status = errStatus
		return resp, req.ServiceName
	}

	if p.meta.TableExist(req.Table.TableName) {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kTableExists,
			"service %s table %s already exists",
			req.ServiceName,
			req.Table.TableName,
		)
		return resp, req.ServiceName
	}

	err := p.post(url, method, req.ServiceName, req, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, service:%s, err:%s",
			method,
			url,
			req.ServiceName,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) listTables(method, params string, input proto.Message) (proto.Message, string) {
	req := input.(*pb.ListTablesRequest)
	resp := &pb.ListTablesResponse{
		Status: pb.ErrStatusOk(),
	}

	param := fmt.Sprintf("service_name=%s", req.ServiceName)
	if req.ServiceName == "" {
		// list all tables
		urls := p.meta.GetAllUrls()
		for _, url := range urls {
			tmpResp := &pb.ListTablesResponse{
				Status: pb.ErrStatusOk(),
			}
			err := p.get(url, method, param, tmpResp)
			if err != nil {
				resp.Status = pb.AdminErrorMsg(
					pb.AdminError_kHttpPostFailed,
					"http post %s to %s failed, service:%s, err:%s",
					method,
					url,
					req.ServiceName,
					err.Error(),
				)
				resp.Tables = []*pb.Table{}
				return resp, req.ServiceName
			}
			resp.Tables = append(resp.Tables, tmpResp.Tables...)
		}
		return resp, req.ServiceName
	}

	url, errStatus := p.getUrl(req.ServiceName)
	if url == "" {
		resp.Status = errStatus
		return resp, req.ServiceName
	}

	err := p.get(url, method, param, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, service:%s, err:%s",
			method,
			url,
			req.ServiceName,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) queryTable(method, params string, input proto.Message) (proto.Message, string) {
	req := input.(*pb.QueryTableRequest)
	resp := &pb.QueryTableResponse{
		Status: pb.ErrStatusOk(),
	}
	url, errStatus := p.getUrl(req.ServiceName)
	if url == "" {
		resp.Status = errStatus
		return resp, req.ServiceName
	}

	err := p.get(url, method, params, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, params:%s, err:%s",
			method,
			url,
			params,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) queryPartition(method, params string, input proto.Message) (proto.Message, string) {
	req := input.(*pb.QueryPartitionRequest)
	resp := &pb.QueryPartitionResponse{
		Status: pb.ErrStatusOk(),
	}
	url, errStatus := p.getUrl(req.ServiceName)
	if url == "" {
		resp.Status = errStatus
		return resp, req.ServiceName
	}

	err := p.get(url, method, params, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, params:%s, err:%s",
			method,
			url,
			params,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) removeReplicas(method, params string, input proto.Message) (proto.Message, string) {
	req := input.(*pb.ManualRemoveReplicasRequest)
	resp := &pb.ManualRemoveReplicasResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	url, errStatus := p.getUrl(req.ServiceName)
	if url == "" {
		resp.Status = errStatus
		return resp, req.ServiceName
	}

	err := p.post(url, method, req.ServiceName, req, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, service:%s, err:%s",
			method,
			url,
			req.ServiceName,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) queryTask(method, params string, input proto.Message) (proto.Message, string) {
	req := input.(*pb.QueryTaskRequest)
	resp := &pb.QueryTaskResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	url, errStatus := p.getUrl(req.ServiceName)
	if url == "" {
		resp.Status = errStatus
		return resp, req.ServiceName
	}

	err := p.get(url, method, params, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, params:%s, err:%s",
			method,
			url,
			params,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) queryTaskCurrentExecution(
	method, params string,
	input proto.Message,
) (proto.Message, string) {
	req := input.(*pb.QueryTaskCurrentExecutionRequest)
	resp := &pb.QueryTaskCurrentExecutionResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	url, errStatus := p.getUrl(req.ServiceName)
	if url == "" {
		resp.Status = errStatus
		return resp, req.ServiceName
	}

	err := p.get(url, method, params, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, params:%s, err:%s",
			method,
			url,
			params,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) adminNode(method, params string, input proto.Message) (proto.Message, string) {
	req := input.(*pb.AdminNodeRequest)
	resp := &pb.AdminNodeResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	url, errStatus := p.getUrl(req.ServiceName)
	if url == "" {
		resp.Status = errStatus
		return resp, req.ServiceName
	}

	err := p.post(url, method, req.ServiceName, req, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, service:%s, err:%s",
			method,
			url,
			req.ServiceName,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) updateNodeWeight(
	method, params string,
	input proto.Message,
) (proto.Message, string) {
	req := input.(*pb.UpdateNodeWeightRequest)
	resp := &pb.UpdateNodeWeightResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	url, errStatus := p.getUrl(req.ServiceName)
	if url == "" {
		resp.Status = errStatus
		return resp, req.ServiceName
	}

	err := p.post(url, method, req.ServiceName, req, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, service:%s, err:%s",
			method,
			url,
			req.ServiceName,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) shrinkAz(method, params string, input proto.Message) (proto.Message, string) {
	req := input.(*pb.ShrinkAzRequest)
	resp := &pb.ShrinkAzResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	url, errStatus := p.getUrl(req.ServiceName)
	if url == "" {
		resp.Status = errStatus
		return resp, req.ServiceName
	}

	err := p.post(url, method, req.ServiceName, req, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, service:%s, err:%s",
			method,
			url,
			req.ServiceName,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) listNodes(method, params string, input proto.Message) (proto.Message, string) {
	req := input.(*pb.ListNodesRequest)
	resp := &pb.ListNodesResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	url, errStatus := p.getUrl(req.ServiceName)
	if url == "" {
		resp.Status = errStatus
		return resp, req.ServiceName
	}

	err := p.get(url, method, params, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, params:%s, err:%s",
			method,
			url,
			params,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) queryNodeInfo(method, params string, input proto.Message) (proto.Message, string) {
	req := input.(*pb.QueryNodeInfoRequest)
	resp := &pb.QueryNodeInfoResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	url, errStatus := p.getUrl(req.ServiceName)
	if url == "" {
		resp.Status = errStatus
		return resp, req.ServiceName
	}

	err := p.get(url, method, params, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, params:%s, err:%s",
			method,
			url,
			params,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) queryNodesInfo(method, params string, input proto.Message) (proto.Message, string) {
	req := input.(*pb.QueryNodesInfoRequest)
	resp := &pb.QueryNodesInfoResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	url, errStatus := p.getUrl(req.ServiceName)
	if url == "" {
		resp.Status = errStatus
		return resp, req.ServiceName
	}

	err := p.post(url, method, req.ServiceName, req, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, service:%s, err:%s",
			method,
			url,
			req.ServiceName,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) queryService(method, params string, input proto.Message) (proto.Message, string) {
	req := input.(*pb.QueryServiceRequest)
	resp := &pb.QueryServiceResponse{}

	url, errStatus := p.getUrl(req.ServiceName)
	if url == "" {
		resp.Status = errStatus
		return resp, req.ServiceName
	}

	err := p.get(url, method, params, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, params:%s, err:%s",
			method,
			url,
			params,
			err.Error(),
		)
	}
	return resp, req.ServiceName
}

func (p *Proxy) generalPostHandle(
	method, params string,
	input proto.Message,
) (proto.Message, string) {
	serviceName := ""
	resp := &pb.ErrorStatusResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}
	switch req := input.(type) {
	case *pb.DeleteServiceRequest:
		serviceName = req.ServiceName
	case *pb.AddHubsRequest:
		serviceName = req.ServiceName
	case *pb.RemoveHubsRequest:
		serviceName = req.ServiceName
	case *pb.UpdateHubsRequest:
		serviceName = req.ServiceName
	case *pb.GiveHintsRequest:
		serviceName = req.ServiceName
	case *pb.RecallHintsRequest:
		serviceName = req.ServiceName
	case *pb.UpdateScheduleOptionsRequest:
		serviceName = req.ServiceName
	case *pb.DeleteTableRequest:
		serviceName = req.ServiceName
	case *pb.UpdateTableRequest:
		serviceName = req.ServiceName
	case *pb.UpdateTableJsonArgsRequest:
		serviceName = req.ServiceName
	case *pb.RestoreTableRequest:
		serviceName = req.ServiceName
	case *pb.SplitTableRequest:
		serviceName = req.ServiceName
	case *pb.OperateTaskRequest:
		serviceName = req.ServiceName
	case *pb.DeleteTaskRequest:
		serviceName = req.ServiceName
	case *pb.TriggerDeleteTaskSideEffectRequest:
		serviceName = req.ServiceName
	case *pb.ExpandAzsRequest:
		serviceName = req.ServiceName
	case *pb.CancelExpandAzsRequest:
		serviceName = req.ServiceName
	case *pb.AssignHubRequest:
		serviceName = req.ServiceName
	case *pb.ReplaceNodesRequest:
		serviceName = req.ServiceName
	case *pb.SwitchSchedulerStatusRequest:
		serviceName = req.ServiceName
	case *pb.SwitchKessPollerStatusRequest:
		serviceName = req.ServiceName
	case *pb.RemoveWatcherRequest:
		serviceName = req.ServiceName
	}

	url, errStatus := p.getUrl(serviceName)
	if url == "" {
		resp.Status = errStatus
		return resp, serviceName
	}
	err := p.post(url, method, serviceName, input, resp)
	if err != nil {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kHttpPostFailed,
			"http post %s to %s failed, service:%s, err:%s",
			method,
			url,
			serviceName,
			err.Error(),
		)
	}

	return resp, serviceName
}

func (p *Proxy) post(url, method, service string, req, resp proto.Message) error {
	logging.Info(
		"proxy post request %s to %s, service:%s",
		method,
		url,
		service,
	)
	return utils.HttpPostJson(url+method, nil, nil, req, resp)
}
func (p *Proxy) get(url, method, params string, resp proto.Message) error {
	sendUrl := fmt.Sprintf("%s%s?%s", url, method, params)
	logging.Info("proxy post request : %s", sendUrl)
	return utils.HttpGet(sendUrl, nil, map[string]string{}, resp, utils.DefaultCheckResp)
}

func (p *Proxy) startHttpServer() {
	router := mux.NewRouter().StrictSlash(true)
	p.StatusCodeUpdate(2)
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	router.HandleFunc("/server_status_code", func(w http.ResponseWriter, r *http.Request) {
		status_code := p.GetStatusCode()
		status_string := p.GetStatusString()
		json_health_response := fmt.Sprintf(
			`{"code": 0, "status": %d, "status_string": "%s"}`,
			status_code,
			status_string,
		)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		_, err := w.Write([]byte(json_health_response))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)

	p.registerHttpHandle(
		router,
		"ListServices",
		"get",
		"/v1/list_services",
		utils.ParseListServicesReq,
		p.listServices,
	)
	p.registerHttpHandle(
		router,
		"CreateService",
		"post",
		"/v1/create_service",
		utils.ParseCreateServiceReq,
		p.createService,
	)
	p.registerHttpHandle(
		router,
		"DeleteService",
		"post",
		"/v1/delete_service",
		utils.ParseDeleteServiceReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"AddHubs",
		"post",
		"/v1/add_hubs",
		utils.ParseAddHubsReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"RemoveHubs",
		"post",
		"/v1/remove_hubs",
		utils.ParseRemoveHubsReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"UpdateHubs",
		"post",
		"/v1/update_hubs",
		utils.ParseUpdateHubsReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"GiveHints",
		"post",
		"/v1/give_hints",
		utils.ParseGiveHintsReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"RecallHints",
		"post",
		"/v1/recall_hints",
		utils.ParseRecallHintsReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"UpdateScheduleOptions",
		"post",
		"/v1/update_schedule_options",
		utils.ParseUpdateScheduleOptionsReq,
		p.generalPostHandle,
	)

	p.registerHttpHandle(
		router,
		"CreateTable",
		"post",
		"/v1/create_table",
		utils.ParseCreateTableReq,
		p.createTable,
	)
	p.registerHttpHandle(
		router,
		"DeleteTable",
		"post",
		"/v1/delete_table",
		utils.ParseDeleteTableReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"UpdateTable",
		"post",
		"/v1/update_table",
		utils.ParseUpdateTableReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"UpdateTableJsonArgs",
		"post",
		"/v1/update_table_json_args",
		utils.ParseUpdateTableJsonArgsReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"ListTables",
		"get",
		"/v1/list_tables",
		utils.ParseListTablesReq,
		p.listTables,
	)
	p.registerHttpHandle(
		router,
		"QueryTable",
		"get",
		"/v1/query_table",
		utils.ParseQueryTableReq,
		p.queryTable,
	)
	p.registerHttpHandle(
		router,
		"RemoveReplicas",
		"post",
		"/v1/remove_replicas",
		utils.ParseManualRemoveReplicasRequest,
		p.removeReplicas,
	)
	p.registerHttpHandle(
		router,
		"RestoreTable",
		"post",
		"/v1/restore_table",
		utils.ParseRestoreTableRequest,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"SplitTable",
		"post",
		"/v1/split_table",
		utils.ParseSplitTableReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"QueryPartition",
		"get",
		"/v1/query_partition",
		utils.ParseQueryPartitionReq,
		p.queryPartition,
	)
	p.registerHttpHandle(
		router,
		"CreateTask",
		"post",
		"/v1/create_task",
		utils.ParseOperateTaskReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"UpdateTask",
		"post",
		"/v1/update_task",
		utils.ParseOperateTaskReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"DeleteTask",
		"post",
		"/v1/delete_task",
		utils.ParseDeleteTaskReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"QueryTask",
		"get",
		"/v1/query_task",
		utils.ParseQueryTaskReq,
		p.queryTask,
	)
	p.registerHttpHandle(
		router,
		"TriggerDeleteTaskSideEffect",
		"post",
		"/v1/trigger_delete_task_side_effect",
		utils.ParseTriggerDeleteTaskSideEffectReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"QueryTaskCurrentExecution",
		"get",
		"/v1/query_task_current_execution",
		utils.ParseQueryTaskCurrentExecution,
		p.queryTaskCurrentExecution,
	)
	p.registerHttpHandle(
		router,
		"AdminNode",
		"post",
		"/v1/admin_node",
		utils.ParseAdminNodeReq,
		p.adminNode,
	)
	p.registerHttpHandle(
		router,
		"UpdateNodeWeight",
		"post",
		"/v1/update_node_weight",
		utils.ParseUpdateNodeWeightReq,
		p.updateNodeWeight,
	)
	p.registerHttpHandle(
		router,
		"ShrinkAz",
		"post",
		"/v1/shrink_az",
		utils.ParseShrinkAzReq,
		p.shrinkAz,
	)
	p.registerHttpHandle(
		router,
		"ExpandAzs",
		"post",
		"/v1/expand_azs",
		utils.ParseExpandAzsReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"CancelExpandAzs",
		"post",
		"/v1/cancel_expand_azs",
		utils.ParseCancelExpandAzsReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"AssignHub",
		"post",
		"/v1/assign_hub",
		utils.ParseAssignHubReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"ReplaceNodes",
		"post",
		"/v1/replace_nodes",
		utils.ParseReplaceNodesReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"ListNodes",
		"get",
		"/v1/list_nodes",
		utils.ParseListNodesReq,
		p.listNodes)
	p.registerHttpHandle(
		router,
		"QueryNodeInfo",
		"get",
		"/v1/query_node_info",
		utils.ParseQueryNodeInfoReq,
		p.queryNodeInfo,
	)
	p.registerHttpHandle(
		router,
		"QueryNodesInfo",
		"post",
		"/v1/query_nodes_info",
		utils.ParseQueryNodesInfoRequest,
		p.queryNodesInfo,
	)
	p.registerHttpHandle(
		router,
		"SwitchSchedulerStatus",
		"post",
		"/v1/switch_scheduler_status",
		utils.ParseSwitchSchedulerStatusReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"SwitchKessPollerStatus",
		"post",
		"/v1/switch_kess_poller_status",
		utils.ParseSwitchKessPollerStatusReq,
		p.generalPostHandle,
	)
	p.registerHttpHandle(
		router,
		"QueryService",
		"get",
		"/v1/query_service",
		utils.ParseQueryServiceReq,
		p.queryService,
	)
	p.registerHttpHandle(
		router,
		"RemoveWatcher",
		"post",
		"/v1/remove_watcher",
		utils.ParseRemoveWatcherReq,
		p.generalPostHandle,
	)
	logging.Info("%s: start to listening to port %v", p.myself, p.httpPort)
	p.httpServer = &http.Server{Addr: fmt.Sprintf(":%v", p.httpPort), Handler: router}
	if err := p.httpServer.ListenAndServe(); err != http.ErrServerClosed {
		logging.Error("%s: listen to http port %d failed: %s", p.myself, p.httpPort, err.Error())
	}
}
