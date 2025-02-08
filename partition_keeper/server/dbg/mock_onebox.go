package dbg

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/sd"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
)

const (
	MOCK_NODES_FILE        = "mock_nodes_info"
	MOCK_ALL_REPLICAS_FILE = "mock_all_replicas"
)

type mockedServiceEnv struct {
	name            string
	discoveryServer *sd.DiscoveryMockServer
	discoveryClient sd.ServiceDiscovery
	nodeDetector    *node_mgr.KessBasedNodeDetector

	clientPoolBuilder *rpc.LocalPSClientPoolBuilder
	nodeCollector     *node_mgr.NodeCollector
	// id -> node_info
	nodes map[string]*node_mgr.NodeInfo
}

func (m *mockedServiceEnv) buildEnvs(kessStartPort int, namespace string) {
	m.discoveryServer = sd.NewDiscoveryMockServer()
	m.discoveryServer.Start(kessStartPort)

	for _, node := range m.nodes {
		if node.IsAlive {
			m.discoveryServer.UpdateServiceNode(node.NodePing.ToServiceNode(node.Id))
		}
	}
	m.discoveryClient = sd.NewServiceDiscovery(
		sd.SdTypeDirectUrl,
		m.name,
		sd.DirectSdGivenUrl(fmt.Sprintf("http://127.0.0.1:%d", kessStartPort)),
	)
	m.nodeDetector = node_mgr.NewNodeDetector(
		m.name,
		node_mgr.WithServiceDiscovery(m.discoveryClient),
	)

	m.clientPoolBuilder = rpc.NewLocalPSClientPoolBuilder()
	m.nodeCollector = node_mgr.NewNodeCollector(m.name, namespace, m.clientPoolBuilder)
}

type MockedEnv struct {
	kessStartPort int
	services      map[string]*mockedServiceEnv
}

func NewMockedEnv() *MockedEnv {
	output := &MockedEnv{
		kessStartPort: 4000,
		services:      make(map[string]*mockedServiceEnv),
	}
	output.initialize()
	return output
}

func (m *MockedEnv) initialize() {
	m.prepareNodes()
	m.prepareReplicas()
}

func (m *MockedEnv) getServiceEnv(serviceName string, create bool) *mockedServiceEnv {
	res, ok := m.services[serviceName]
	if ok {
		return res
	}
	if !create {
		return nil
	}
	res = &mockedServiceEnv{
		name:  serviceName,
		nodes: make(map[string]*node_mgr.NodeInfo),
	}
	m.services[serviceName] = res
	return res
}

func (m *MockedEnv) prepareNodes() {
	file, err := os.Open(MOCK_NODES_FILE)
	if err != nil {
		logging.Error("Mock onebox open mock_nodes_info fail:%s", err.Error())
		os.Exit(1)
	}
	defer file.Close()

	rd := bufio.NewReader(file)
	for {
		line, err := rd.ReadString('\n')
		if err != nil || err == io.EOF {
			break
		}

		serviceName := line[:strings.Index(line, ":")]
		nodesJson := line[strings.Index(line, ":")+1:]

		serviceEnv := m.getServiceEnv(serviceName, true)
		if nodesJson == "" || nodesJson == "\n" {
			continue
		}
		nodesInfo := strings.Split(nodesJson, "&&")

		for _, nodeInfo := range nodesInfo {
			info := &node_mgr.NodeInfo{}
			utils.UnmarshalJsonOrDie([]byte(nodeInfo), info)
			serviceEnv.nodes[info.Id] = info
		}
	}
}

func (m *MockedEnv) prepareReplicas() {
	file, err := os.Open(MOCK_ALL_REPLICAS_FILE)
	if err != nil {
		logging.Error("Mock onebox open mock_all_replicas fail:%s", err.Error())
		os.Exit(1)
	}
	defer file.Close()
	rd := bufio.NewReader(file)

	key := ""
	for i := 0; ; i++ {
		if i%2 == 0 {
			// hostname
			var data string
			n, err := fmt.Fscanf(rd, "%s\n", &data)
			if n == 0 || err == io.EOF {
				break
			}
			logging.Assert(err == nil, "")
			key = data
		} else {
			// replicas
			var data []byte
			n, err := fmt.Fscanf(rd, "%X\n", &data)
			if n == 0 || err == io.EOF {
				break
			}
			logging.Assert(err == nil, "")
			buffer := bytes.NewBuffer(data)
			var res pb.GetReplicasResponse

			decoder := gob.NewDecoder(buffer)
			if err := decoder.Decode(&res); err != nil {
				logging.Fatal("GetReplicasResponse decode fail. host:%s", key)
			}
			rpc.MockAllReplicasInfo.Add(key, &res)
		}
	}
}

func (m *MockedEnv) GetMockOneboxHandler(
	serviceName, namespace string,
) (rpc.ConnPoolBuilder, *node_mgr.KessBasedNodeDetector, *node_mgr.NodeCollector) {
	serviceEnv := m.getServiceEnv(serviceName, false)
	logging.Assert(serviceEnv != nil, "")
	serviceEnv.buildEnvs(m.kessStartPort, namespace)
	m.kessStartPort++
	return serviceEnv.clientPoolBuilder, serviceEnv.nodeDetector, serviceEnv.nodeCollector
}

func parseStopNodeReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	req := &pb.StopNodeRequest{}
	if err := utils.GetReqFromHttpBody(r, req); err == nil {
		return req, nil
	} else {
		return nil, err
	}
}

func (m *MockedEnv) stopNode(input proto.Message) proto.Message {
	req := input.(*pb.StopNodeRequest)
	resp := &pb.ErrorStatusResponse{
		Status: pb.ErrStatusOk(),
	}
	service := m.getServiceEnv(req.ServiceName, false)
	if service == nil {
		resp.Status.Code = int32(pb.AdminError_kServiceNotExists)
		return resp
	}
	info := service.nodes[req.NodeId]
	if info == nil {
		resp.Status.Code = int32(pb.AdminError_kNodeNotExisting)
		return resp
	}
	if service.discoveryServer.RemoveNode(info.Address.String()) {
		return resp
	} else {
		resp.Status.Code = int32(pb.AdminError_kNodeNotExisting)
		return resp
	}
}

func parseStartNodeReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	req := &pb.StartNodeRequest{}
	if err := utils.GetReqFromHttpBody(r, req); err == nil {
		return req, nil
	} else {
		return nil, err
	}
}

func (m *MockedEnv) startNode(input proto.Message) proto.Message {
	req := input.(*pb.StartNodeRequest)
	resp := &pb.ErrorStatusResponse{
		Status: pb.ErrStatusOk(),
	}
	service := m.getServiceEnv(req.ServiceName, false)
	if service == nil {
		resp.Status.Code = int32(pb.AdminError_kServiceNotExists)
		return resp
	}
	nodeInfo := service.nodes[req.NodeId]
	if nodeInfo == nil {
		vaz := utils.Paz2Vaz(req.Paz)
		if vaz == "" {
			resp.Status = pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"invalid paz: %s",
				req.Paz,
			)
			return resp
		}
		hostname, err := os.Hostname()
		if err != nil {
			resp.Status = pb.AdminErrorMsg(
				pb.AdminError_kDependencyServiceError,
				"get host name failed: %v",
				err,
			)
			return resp
		}
		nodeInfo := &node_mgr.NodeInfo{
			NodePing: node_mgr.NodePing{
				IsAlive: true,
				Az:      vaz,
				Address: &utils.RpcNode{
					NodeName: hostname,
					Port:     req.Port,
				},
				ProcessId: fmt.Sprintf("%d", rand.Int31n(1000000)+1),
				BizPort:   req.Port + 1,
				NodeIndex: req.NodeIndex,
			},
		}
		service.discoveryServer.UpdateServiceNode(nodeInfo.ToServiceNode(req.NodeId))
		return resp
	} else {
		nodeInfo.ProcessId = fmt.Sprintf("%d", rand.Int31n(1000000)+1)
		service.discoveryServer.UpdateServiceNode(nodeInfo.ToServiceNode(req.NodeId))
		return resp
	}
}

type parseRequest func(r *http.Request) (proto.Message, *pb.ErrorStatus)
type handleRequest func(proto.Message) proto.Message

func (m *MockedEnv) handleRequest(parser parseRequest, handler handleRequest) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		var output proto.Message = nil
		defer func() {
			data, err := json.MarshalIndent(output, "", "  ")
			if err != nil {
				panic(err)
			} else {
				w.Write(data)
			}
		}()

		request, err := parser(r)
		if err != nil {
			output = &pb.ErrorStatusResponse{
				Status: err,
			}
			return
		}
		output = handler(request)
	}
}

func (m *MockedEnv) registerHttpHandle(
	router *mux.Router,
	name, method, path string,
	parse parseRequest,
	handle handleRequest,
) {
	router.Name(name).
		Methods(strings.ToUpper(method)).
		Path(path).
		HandlerFunc(m.handleRequest(parse, handle))
}

func (m *MockedEnv) RegisterMockAdmin(router *mux.Router) {
	m.registerHttpHandle(
		router,
		"StopNode",
		"post",
		"/mock/stop_node",
		parseStopNodeReq,
		m.stopNode,
	)
	m.registerHttpHandle(
		router,
		"StartNode",
		"post",
		"/mock/start_node",
		parseStartNodeReq,
		m.startNode,
	)
}
