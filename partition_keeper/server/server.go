package server

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/delay_execute"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/dbg"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/version"

	"github.com/go-zookeeper/zk"
	"github.com/gorilla/mux"
	"google.golang.org/protobuf/proto"
)

const (
	kLockNode             = "LOCK"
	kServiceNode          = "service"
	kTablesNode           = "tables"
	kDelayedExecutorNode  = "delayed_executor"
	kRequestForwardHeader = "FOLLOWER_FORWARDED"
	kAllowAllServices     = "all_types"
)

var (
	flagDefaultSvcType   pb.ServiceType = pb.ServiceType_invalid
	flagServiceWhiteList utils.StrlistFlag
	flagServiceBlackList utils.StrlistFlag
	flagAllowSvcTypes    utils.StrlistFlag
	flagZkHosts          utils.StrlistFlag
	flagServiceName      = flag.String(
		"service_name",
		"",
		"partition keeper's service name registered to kess",
	)
	flagZkPrefix  = flag.String("zk_prefix", "/ks/reco/keeper", "zk prefix")
	flagHttpPort  = flag.Int("http_port", 30100, "bind port of partition_keeper http server")
	flagNamespace = flag.String(
		"namespace",
		"",
		"namespace of the keeper, which specifies a subdir under zk_prefix",
	)
	flagPollLeaderMillis = flag.Int("poll_leader_millis", 2000, "poll leader milliseconds")
)

func init() {
	flag.Var(
		&flagServiceWhiteList,
		"service_white_list",
		"white_list services with format <name1>,<name2>...",
	)
	flag.Var(
		&flagServiceBlackList,
		"service_black_list",
		"black_list services with format <name1>,<name2>...",
	)
	flag.Var(&flagZkHosts, "zk_hosts", "zk hosts with format <ip:port>,<ip:port>...")
	flag.Var(&flagDefaultSvcType, "default_service_type", "default service type for create service")
	flag.Var(&flagAllowSvcTypes, "allow_service_types", "allowed service types")
}

type leaderInfo struct {
	isLeader       bool
	leaderNode     string
	leaderAddr     string
	quitPollLeader chan bool
}

type serviceAccessController struct {
	whiteList map[string]bool
	blackList map[string]bool
}

func (s *serviceAccessController) manageThisService(name string) bool {
	if len(s.whiteList) > 0 {
		_, ok := s.whiteList[name]
		return ok
	}
	if len(s.blackList) > 0 {
		_, ok := s.blackList[name]
		return !ok
	}
	return true
}

type Server struct {
	myself           string
	zkHosts          []string
	zkPrefix         string
	httpPort         int
	namespace        string
	serviceName      string
	pollLeaderMillis int
	allowedSvcTypes  map[pb.ServiceType]bool
	serviceCtl       *serviceAccessController

	nsZkPath               string
	distLockPath           string
	servicesPath           string
	tablesPath             string
	zkStore                *metastore.ZookeeperStore
	tablesManager          *TablesManager
	delayedExecutorManager *delay_execute.DelayedExecutorManager
	distLeaderLock         *zk.Lock
	httpServer             *http.Server

	mu             sync.RWMutex
	leaderInfo     leaderInfo
	hasInitialized bool
	allServices    map[string]*ServiceStat
	mockedEnv      *dbg.MockedEnv
	serviceDropped chan string
}

type serverOpts func(sv *Server)

func WithZk(zkHosts []string) serverOpts {
	return func(sv *Server) {
		sv.zkHosts = zkHosts
	}
}

func WithZkPrefix(zkPrefix string) serverOpts {
	return func(sv *Server) {
		sv.zkPrefix = zkPrefix
	}
}

func WithHttpPort(port int) serverOpts {
	return func(sv *Server) {
		sv.httpPort = port
	}
}

func WithNamespace(ns string) serverOpts {
	return func(sv *Server) {
		sv.namespace = ns
	}
}

func WithServiceName(name string) serverOpts {
	return func(sv *Server) {
		sv.serviceName = name
	}
}

func WithPollLeaderMillis(s int) serverOpts {
	return func(sv *Server) {
		sv.pollLeaderMillis = s
	}
}

func WithAllowServiceType(types map[pb.ServiceType]bool) serverOpts {
	return func(sv *Server) {
		sv.allowedSvcTypes = types
	}
}

func parseSvcTypes(types []string) map[pb.ServiceType]bool {
	output := map[pb.ServiceType]bool{}
	if len(types) == 1 && types[0] == kAllowAllServices {
		for _, st := range pb.ServiceType_value {
			output[pb.ServiceType(st)] = true
		}
		return output
	}
	for _, t := range types {
		if svcType, ok := pb.ServiceType_value[t]; ok {
			output[pb.ServiceType(svcType)] = true
		} else {
			logging.Fatal("parse %s as service type failed, should be one in map %v", t, pb.ServiceType_value)
		}
	}
	return output
}

func parseServiceAccessController(white, black []string) *serviceAccessController {
	output := &serviceAccessController{
		whiteList: map[string]bool{},
		blackList: map[string]bool{},
	}

	for _, name := range white {
		output.whiteList[name] = true
	}
	for _, name := range black {
		output.blackList[name] = true
	}

	return output
}

func CreateServer(opts ...serverOpts) *Server {
	s := &Server{
		zkHosts:          flagZkHosts,
		zkPrefix:         *flagZkPrefix,
		httpPort:         *flagHttpPort,
		namespace:        *flagNamespace,
		serviceName:      *flagServiceName,
		pollLeaderMillis: *flagPollLeaderMillis,
		allowedSvcTypes:  parseSvcTypes(flagAllowSvcTypes),
		serviceCtl:       parseServiceAccessController(flagServiceWhiteList, flagServiceBlackList),
		allServices:      make(map[string]*ServiceStat),
		hasInitialized:   false,
		serviceDropped:   make(chan string, 1),
	}
	if dbg.RunOnebox() {
		s.mockedEnv = dbg.NewMockedEnv()
	}
	s.leaderInfo.isLeader = false
	s.leaderInfo.quitPollLeader = make(chan bool)
	for _, opt := range opts {
		opt(s)
	}

	if s.namespace == "" {
		logging.Fatal("server namespace shouldn't be empty")
	}
	if s.serviceName == "" {
		logging.Fatal("server kess service name shouldn't be empty")
	}
	if hostName, err := os.Hostname(); err != nil {
		logging.Fatal("get host name failed: %v", err.Error())
	} else {
		s.myself = fmt.Sprintf("%s:%d", hostName, s.httpPort)
	}

	return s
}

func (s *Server) Start() {
	logging.Info("%s: start server with git commit id: %s", s.myself, version.GitCommitId)
	s.prepareZookeeper()
	go s.acquireLeaderLock()
	s.startHttpServer()
}

func (s *Server) Stop() {
	s.httpServer.Close()
	s.distLeaderLock.Unlock()
	s.zkStore.Close()
}

func (s *Server) getService(serviceName string) (*ServiceStat, *pb.ErrorStatus) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if ans, ok := s.allServices[serviceName]; ok {
		return ans, nil
	} else {
		return nil, pb.AdminErrorMsg(
			pb.AdminError_kServiceNotExists,
			"service %s not exists",
			serviceName,
		)
	}
}

func (s *Server) prepareZookeeper() {
	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	s.zkStore = metastore.CreateZookeeperStore(s.zkHosts, time.Second*60, acl, scheme, auth)

	s.nsZkPath = fmt.Sprintf("%s/%s", s.zkPrefix, s.namespace)
	logging.Assert(s.zkStore.RecursiveCreate(context.Background(), s.nsZkPath), "")

	s.distLockPath = fmt.Sprintf("%s/%s", s.nsZkPath, kLockNode)
	logging.Assert(s.zkStore.Create(context.Background(), s.distLockPath, nil), "")
	logging.Info("%s: zookeeper has been prepared", s.myself)
}

func (s *Server) startTableManager() {
	s.tablesPath = fmt.Sprintf("%s/%s", s.nsZkPath, kTablesNode)
	s.tablesManager = NewTablesManager(s.zkStore, s.tablesPath)
	s.tablesManager.InitFromZookeeper()
}

func (s *Server) startDelayedExecutorManager() {
	zkPath := fmt.Sprintf("%s/%s", s.nsZkPath, kDelayedExecutorNode)
	s.delayedExecutorManager = delay_execute.NewDelayedExecutorManager(s.zkStore, zkPath)
	s.delayedExecutorManager.InitFromZookeeper()
}

func (s *Server) startServices() {
	s.servicesPath = fmt.Sprintf("%s/%s", s.nsZkPath, kServiceNode)
	logging.Assert(s.zkStore.Create(context.Background(), s.servicesPath, nil), "")

	children, exists, succ := s.zkStore.Children(context.Background(), s.servicesPath)
	logging.Assert(succ && exists, "")
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, child := range children {
		if s.serviceCtl.manageThisService(child) {
			var svc *ServiceStat
			if dbg.RunOnebox() {
				lpb, kessDetector, nodeCollector := s.mockedEnv.GetMockOneboxHandler(
					child,
					s.namespace,
				)
				svc = NewServiceStat(
					child,
					s.namespace,
					s.servicesPath,
					s.zkStore,
					s.tablesManager,
					s.delayedExecutorManager,
					s.serviceDropped,
					WithRpcPoolBuilder(lpb),
					WithDetector(kessDetector),
					WithCollector(nodeCollector),
				)
			} else {
				svc = NewServiceStat(
					child,
					s.namespace,
					s.servicesPath,
					s.zkStore,
					s.tablesManager,
					s.delayedExecutorManager,
					s.serviceDropped,
				)
			}

			s.allServices[child] = svc
			go svc.LoadFromZookeeper()
		} else {
			logging.Info("%s: skip to manager %s as required", s.myself, child)
		}
	}
	go s.bgCleanDroppedServices()
	s.hasInitialized = true
	logging.Info("%s: services has been started", s.myself)
}

func (s *Server) bgCleanDroppedServices() {
	for {
		name := <-s.serviceDropped
		logging.Info("%s: remove service %s as it has been dropped", s.myself, name)
		s.mu.Lock()
		delete(s.allServices, name)
		s.mu.Unlock()
	}
}

func (s *Server) getLeaderFromZk() {
	children, _, err := s.zkStore.GetRawSession().Children(s.distLockPath)
	if err != nil {
		logging.Info(
			"%s: acquire children for path %s failed: %s",
			s.myself,
			s.distLockPath,
			err.Error(),
		)
		return
	}
	minSeqIndex := -1
	minSeq := -1
	for i, child := range children {
		parts := strings.Split(child, "-")
		if len(parts) == 1 {
			parts = strings.Split(child, "__")
		}
		seq, err := strconv.Atoi(parts[len(parts)-1])
		if err != nil {
			logging.Info("%s: got invalid seq node %s: %s", s.myself, child, err.Error())
		} else {
			if minSeq == -1 || minSeq > seq {
				minSeq = seq
				minSeqIndex = i
			}
		}
	}
	if minSeqIndex == -1 {
		logging.Info("%s: can't find valid child", s.myself)
	}
	if s.leaderInfo.leaderNode == children[minSeqIndex] {
		return
	}

	path := fmt.Sprintf("%s/%s", s.distLockPath, children[minSeqIndex])
	data, _, err := s.zkStore.GetRawSession().Get(path)
	if err != nil {
		logging.Info("%s: read zk %s failed: %s", s.myself, path, err.Error())
	}
	s.setCurrentLeader(children[minSeqIndex], string(data))
}

func (s *Server) pollLeader() {
	ticker := time.NewTicker(time.Millisecond * time.Duration(s.pollLeaderMillis))
	for {
		select {
		case <-ticker.C:
			s.getLeaderFromZk()
		case <-s.leaderInfo.quitPollLeader:
			ticker.Stop()
			return
		}
	}
}

func (s *Server) acquireLeaderLock() {
	acl, _, _ := acl.GetKeeperACLandAuthForZK()
	s.distLeaderLock = zk.NewLock(
		s.zkStore.GetRawSession(),
		s.distLockPath,
		acl,
	)

	go s.pollLeader()
	err := s.distLeaderLock.LockWithData([]byte(s.myself))
	if err == zk.ErrClosing || err == zk.ErrConnectionClosed {
		logging.Info("%s: zk session is closed", s.myself)
		s.leaderInfo.quitPollLeader <- true
		return
	}
	if err != nil {
		logging.Fatal("%s: acquire lock from zk failed: %v", s.myself, err.Error())
	}
	logging.Info("%s: acquire leader lock succeed", s.myself)
	s.setIsLeader()
	s.leaderInfo.quitPollLeader <- true

	s.startTableManager()
	s.startDelayedExecutorManager()
	s.startServices()
}

func (s *Server) setIsLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leaderInfo.isLeader = true
}

func (s *Server) setCurrentLeader(leaderNode, leaderAddr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logging.Info("%s: leader change from %s@%s to %s@%s",
		s.myself,
		s.leaderInfo.leaderNode, s.leaderInfo.leaderAddr,
		leaderNode, leaderAddr,
	)
	s.leaderInfo.leaderNode = leaderNode
	s.leaderInfo.leaderAddr = leaderAddr
}

func (s *Server) getServerStatus() (bool, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leaderInfo.isLeader, s.leaderInfo.leaderAddr, s.hasInitialized
}

type parseRequest func(r *http.Request) (proto.Message, *pb.ErrorStatus)
type handleRequest func(proto.Message) (proto.Message, string)

func (s *Server) forwardRequest(url string, old *http.Request) (*http.Response, error) {
	logging.Verbose(1, "%s: forward to url %s", s.myself, url)
	req, err := http.NewRequest(old.Method, url, old.Body)
	if err != nil {
		return nil, err
	}
	req.Header = old.Header.Clone()
	req.Header.Add(kRequestForwardHeader, "true")

	return http.DefaultClient.Do(req)
}

func (s *Server) tryForwardRequest(leaderAddr string, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	var output *pb.ErrorStatusResponse = nil
	defer func() {
		if output == nil {
			return
		}
		w.WriteHeader(http.StatusOK)
		data, err := json.MarshalIndent(output, "", "  ")
		if err != nil {
			logging.Error("%s: marshal data error: %s", s.myself, err.Error())
			panic(err)
		} else {
			logging.Verbose(1, "%s: response data: %s", s.myself, string(data))
			w.Write(data)
		}
	}()

	if data := r.Header.Get(kRequestForwardHeader); data != "" {
		logging.Info(
			"%s: request %s is forwarded itself, don't forward anymore",
			s.myself,
			r.RequestURI,
		)
		output = &pb.ErrorStatusResponse{
			Status: pb.AdminErrorMsg(pb.AdminError_kServerNotLeader, "forwarded not leader"),
		}
		return
	}
	if leaderAddr == "" {
		logging.Info("%s: can't find leader, don't forward %s", s.myself, r.RequestURI)
		output = &pb.ErrorStatusResponse{
			Status: pb.AdminErrorMsg(pb.AdminError_kServerNotLeader, "can't find leader"),
		}
		return
	}

	url := fmt.Sprintf("http://%s%s", leaderAddr, r.URL.Path)
	if r.URL.RawQuery != "" {
		url = url + "?" + r.URL.RawQuery
	}
	resp, err := s.forwardRequest(url, r)
	if err != nil {
		output = &pb.ErrorStatusResponse{
			Status: pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, err.Error()),
		}
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		logging.Info("%s: got forwarded response from %s: %s", s.myself, url, resp.Status)
		w.WriteHeader(resp.StatusCode)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		logging.Info("%s: copy from forwarded url %s failed: %s", s.myself, url, err.Error())
	}
}

func (s *Server) handleRequest(
	initialized bool,
	w http.ResponseWriter,
	r *http.Request,
	parse parseRequest,
	handle handleRequest,
	funcName string,
) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)

	var output proto.Message = nil
	defer func() {
		data, err := json.MarshalIndent(output, "", "  ")
		if err != nil {
			logging.Error("%s: marshal data error: %s", s.myself, err.Error())
			panic(err)
		} else {
			logging.Verbose(1, "%s: response data: %s", s.myself, string(data))
			w.Write(data)
		}
	}()

	if !initialized {
		output = &pb.ErrorStatusResponse{
			Status: pb.AdminErrorMsg(pb.AdminError_kServerNotLeader,
				"server can't serve as leader: is_leader: true, has_initialized: false"),
		}
		return
	}

	request, err := parse(r)
	if err != nil {
		logging.Info("%s: parse request failed: %s", s.myself, err.Message)
		output = &pb.ErrorStatusResponse{
			Status: err,
		}
		return
	}
	logging.Info("%s: got request url %s req %v from %s", s.myself, r.URL, request, r.RemoteAddr)

	start := time.Now()
	output, serviceName := handle(request)
	elapse := time.Since(start).Microseconds()
	if serviceName == "" {
		serviceName = "InternalPerfService"
	}
	third_party.PerfLog1("reco", "reco.colossusdb.req_cost", serviceName, funcName, uint64(elapse))
	third_party.PerfLog1(
		"reco",
		"reco.colossusdb.resp_size",
		serviceName,
		funcName,
		uint64(proto.Size(output)),
	)
}

func (s *Server) handleIfReadyToServe(
	parse parseRequest,
	handle handleRequest,
	funcName string,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logging.Verbose(1, "%s: got req %s from %s", s.myself, r.URL, r.RemoteAddr)
		isLeader, leaderAddr, initialized := s.getServerStatus()
		if !isLeader {
			s.tryForwardRequest(leaderAddr, w, r)
		} else {
			s.handleRequest(initialized, w, r, parse, handle, funcName)
		}
	}
}

func (s *Server) sanitizeHubNames(hubs []*pb.ReplicaHub) *pb.ErrorStatus {
	for _, hub := range hubs {
		hub.Az = utils.KrpAzToStandard(hub.Az)
		if !utils.AzNameValid(hub.Az) {
			return pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"can't recognize %s in hub %s as a valid az", hub.Az, hub.Name,
			)
		}
	}
	return nil
}

func (s *Server) registerHttpHandle(
	router *mux.Router,
	name, method, path string,
	parse parseRequest,
	handle handleRequest,
) {
	router.Name(name).
		Methods(strings.ToUpper(method)).
		Path(path).
		HandlerFunc(s.handleIfReadyToServe(parse, handle, name))
}

// these http handlers should keep the same with pb.partition_keeper_admin.proto
func (s *Server) listServices(input proto.Message) (proto.Message, string) {
	resp := &pb.ListServicesResponse{
		Status:       &pb.ErrorStatus{Code: 0},
		ServiceNames: []string{},
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, service := range s.allServices {
		resp.ServiceNames = append(resp.ServiceNames, service.serviceName)
	}
	return resp, ""
}

func (s *Server) createService(input proto.Message) (proto.Message, string) {
	req := input.(*pb.CreateServiceRequest)
	resp := &pb.ErrorStatusResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	if req.ServiceType == pb.ServiceType_invalid {
		req.ServiceType = flagDefaultSvcType
	}

	if _, ok := s.allowedSvcTypes[req.ServiceType]; !ok {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"only allow to manager %v for this deployment, reject %s",
			s.allowedSvcTypes,
			req.ServiceType,
		)
		return resp, req.ServiceName
	}

	if !utils.IsValidName(req.ServiceName) {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"invalid service name: %s",
			req.ServiceName,
		)
		return resp, req.ServiceName
	}

	//if len(req.NodesHubs) == 0 {
	//	resp.Status = pb.AdminErrorMsg(
	//		pb.AdminError_kInvalidParameter,
	//		"nodes_hubs should have at least 1 element",
	//	)
	//	return resp, req.ServiceName
	//}

	if errMsg := s.sanitizeHubNames(req.NodesHubs); errMsg != nil {
		resp.Status = errMsg
		return resp, req.ServiceName
	}

	var handle *ServiceStat
	if dbg.RunOnebox() {
		lpb, kessDetector, nodeCollector := s.mockedEnv.GetMockOneboxHandler(
			req.ServiceName,
			s.namespace,
		)
		handle = NewServiceStat(
			req.ServiceName,
			s.namespace,
			s.servicesPath,
			s.zkStore,
			s.tablesManager,
			s.delayedExecutorManager,
			s.serviceDropped,
			WithRpcPoolBuilder(lpb),
			WithDetector(kessDetector),
			WithCollector(nodeCollector),
		)
	} else {
		handle = NewServiceStat(
			req.ServiceName,
			s.namespace,
			s.servicesPath,
			s.zkStore,
			s.tablesManager,
			s.delayedExecutorManager,
			s.serviceDropped,
		)
	}

	s.mu.Lock()
	if _, ok := s.allServices[handle.serviceName]; ok {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kServiceExists,
			"service %s exists",
			req.ServiceName,
		)
		s.mu.Unlock()
		return resp, req.ServiceName
	}
	s.allServices[handle.serviceName] = handle
	s.mu.Unlock()

	resp.Status = handle.InitializeNew(req)
	if resp.Status.Code != 0 {
		s.mu.Lock()
		delete(s.allServices, req.ServiceName)
		s.mu.Unlock()
	}
	return resp, req.ServiceName
}

func (s *Server) deleteService(input proto.Message) (proto.Message, string) {
	req := input.(*pb.DeleteServiceRequest)
	resp := &pb.ErrorStatusResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}
	var service *ServiceStat
	var ok bool
	s.mu.Lock()
	defer s.mu.Unlock()
	if service, ok = s.allServices[req.ServiceName]; !ok {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kOk,
			"service %s not exists, treat as delete succeed",
			req.ServiceName,
		)
	} else {
		resp.Status = service.Teardown()
	}
	return resp, req.ServiceName
}

func (s *Server) addHubs(input proto.Message) (proto.Message, string) {
	req := input.(*pb.AddHubsRequest)
	resp := &pb.ErrorStatusResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	service, err := s.getService(req.ServiceName)
	if service == nil {
		resp.Status = err
		return resp, req.ServiceName
	}

	if errMsg := s.sanitizeHubNames(req.NodesHubs); err != nil {
		resp.Status = errMsg
		return resp, req.ServiceName
	}

	resp.Status = service.AddHubs(req.NodesHubs, req.TryGather)
	return resp, req.ServiceName
}

func (s *Server) removeHubs(input proto.Message) (proto.Message, string) {
	req := input.(*pb.RemoveHubsRequest)
	resp := &pb.ErrorStatusResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	service, err := s.getService(req.ServiceName)
	if service == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = service.RemoveHubs(req.NodesHubs, req.TryScatter)
	return resp, req.ServiceName
}

func (s *Server) updateHubs(input proto.Message) (proto.Message, string) {
	req := input.(*pb.UpdateHubsRequest)
	resp := &pb.ErrorStatusResponse{
		Status: nil,
	}

	service, err := s.getService(req.ServiceName)
	if service == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = service.UpdateHubs(req.NodesHubs)
	return resp, req.ServiceName
}

func (s *Server) giveHints(input proto.Message) (proto.Message, string) {
	req := input.(*pb.GiveHintsRequest)
	resp := &pb.ErrorStatusResponse{
		Status: pb.ErrStatusOk(),
	}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}

	resp.Status = handle.GiveHints(req.Hints, req.MatchPort)
	return resp, req.ServiceName
}

func (s *Server) recallHints(input proto.Message) (proto.Message, string) {
	req := input.(*pb.RecallHintsRequest)
	resp := &pb.ErrorStatusResponse{
		Status: pb.ErrStatusOk(),
	}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}

	resp.Status = handle.RecallHints(req.Nodes, req.MatchPort)
	return resp, req.ServiceName
}

func (s *Server) updateScheduleOptions(input proto.Message) (proto.Message, string) {
	req := input.(*pb.UpdateScheduleOptionsRequest)
	resp := &pb.ErrorStatusResponse{
		Status: pb.ErrStatusOk(),
	}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = handle.UpdateScheduleOptions(req.SchedOpts, req.UpdatedOptionNames)
	return resp, req.ServiceName
}

func (s *Server) expandAzs(input proto.Message) (proto.Message, string) {
	req := input.(*pb.ExpandAzsRequest)
	resp := &pb.ErrorStatusResponse{
		Status: pb.ErrStatusOk(),
	}

	handler, err := s.getService(req.ServiceName)
	if handler == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = handler.ExpandAzs(req)
	return resp, req.ServiceName
}

func (s *Server) cancelExpandAzs(input proto.Message) (proto.Message, string) {
	req := input.(*pb.CancelExpandAzsRequest)
	resp := &pb.ErrorStatusResponse{
		Status: pb.ErrStatusOk(),
	}

	handler, err := s.getService(req.ServiceName)
	if handler == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = handler.CancelExpandAzs(req)
	return resp, req.ServiceName
}

func (s *Server) assignHub(input proto.Message) (proto.Message, string) {
	req := input.(*pb.AssignHubRequest)
	resp := &pb.ErrorStatusResponse{}

	handler, err := s.getService(req.ServiceName)
	if handler == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = handler.AssignHub(req)
	return resp, req.ServiceName
}

func (s *Server) replaceNodes(input proto.Message) (proto.Message, string) {
	req := input.(*pb.ReplaceNodesRequest)
	resp := &pb.ErrorStatusResponse{
		Status: pb.ErrStatusOk(),
	}

	handler, err := s.getService(req.ServiceName)
	if handler == nil {
		resp.Status = err
		return resp, req.ServiceName
	}

	resp.Status = handler.ReplaceNodes(req)
	return resp, req.ServiceName
}

func (s *Server) createTable(input proto.Message) (proto.Message, string) {
	req := input.(*pb.CreateTableRequest)
	resp := &pb.ErrorStatusResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = handle.AddTable(req.Table, req.RestorePath, req.TrafficKconfPath)
	if resp.Status.Code != int32(pb.AdminError_kOk) {
		logging.Info(
			"%s create table %s error: %v",
			req.ServiceName,
			req.Table.TableName,
			resp.Status,
		)
	}
	return resp, req.ServiceName
}

func (s *Server) deleteTable(input proto.Message) (proto.Message, string) {
	req := input.(*pb.DeleteTableRequest)
	resp := &pb.ErrorStatusResponse{
		Status: &pb.ErrorStatus{Code: 0},
	}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	if req.CleanTaskSideEffect && req.CleanDelayMinutes < 0 {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"clean_delay_minutes cannot be less than 0",
		)
		return resp, req.ServiceName
	}

	resp.Status = handle.RemoveTable(req.TableName, req.CleanTaskSideEffect, req.CleanDelayMinutes)
	return resp, req.ServiceName
}

func (s *Server) updateTable(input proto.Message) (proto.Message, string) {
	req := input.(*pb.UpdateTableRequest)
	resp := &pb.ErrorStatusResponse{Status: pb.ErrStatusOk()}
	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = handle.UpdateTable(req.Table)
	return resp, req.ServiceName
}

func (s *Server) updateTableJsonArgs(input proto.Message) (proto.Message, string) {
	req := input.(*pb.UpdateTableJsonArgsRequest)
	resp := &pb.ErrorStatusResponse{Status: pb.ErrStatusOk()}
	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = handle.UpdateTableJsonArgs(req.TableName, req.JsonArgs)
	return resp, req.ServiceName
}

func (s *Server) listTables(input proto.Message) (proto.Message, string) {
	req := input.(*pb.ListTablesRequest)
	resp := &pb.ListTablesResponse{
		Status: pb.ErrStatusOk(),
	}

	if req.ServiceName == "" {
		services := []*ServiceStat{}
		s.mu.RLock()
		for _, service := range s.allServices {
			services = append(services, service)
		}
		s.mu.RUnlock()

		for _, service := range services {
			oneResp := &pb.ListTablesResponse{}
			service.ListTables(oneResp)
			if len(oneResp.Tables) > 0 {
				resp.Tables = append(resp.Tables, oneResp.Tables...)
			}
		}
		return resp, req.ServiceName
	} else {
		handle, err := s.getService(req.ServiceName)
		if handle == nil {
			resp.Status = err
			return resp, req.ServiceName
		}

		handle.ListTables(resp)
		return resp, req.ServiceName
	}
}

func (s *Server) queryTable(input proto.Message) (proto.Message, string) {
	req := input.(*pb.QueryTableRequest)
	resp := pb.QueryTableResponse{}
	resp.Status = &pb.ErrorStatus{Code: 0}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return &resp, req.ServiceName
	}
	handle.QueryTable(req, &resp)
	return &resp, req.ServiceName
}

func (s *Server) removeReplicas(input proto.Message) (proto.Message, string) {
	req := input.(*pb.ManualRemoveReplicasRequest)
	resp := pb.ManualRemoveReplicasResponse{}
	resp.Status = pb.ErrStatusOk()

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return &resp, req.ServiceName
	}
	handle.RemoveReplicas(req, &resp)
	return &resp, req.ServiceName
}

func (s *Server) restoreTable(input proto.Message) (proto.Message, string) {
	req := input.(*pb.RestoreTableRequest)
	resp := &pb.ErrorStatusResponse{}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = handle.RestoreTable(req)
	return resp, req.ServiceName
}

func (s *Server) splitTable(input proto.Message) (proto.Message, string) {
	req := input.(*pb.SplitTableRequest)
	resp := &pb.ErrorStatusResponse{}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = handle.SplitTable(req)
	return resp, req.ServiceName
}

func (s *Server) queryPartition(input proto.Message) (proto.Message, string) {
	req := input.(*pb.QueryPartitionRequest)
	resp := pb.QueryPartitionResponse{}
	resp.Status = &pb.ErrorStatus{Code: 0}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return &resp, req.ServiceName
	}
	handle.QueryPartition(req, &resp)
	return &resp, req.ServiceName
}

func (s *Server) createTask(input proto.Message) (proto.Message, string) {
	req := input.(*pb.OperateTaskRequest)
	resp := &pb.ErrorStatusResponse{}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = handle.CreateTask(req.TableName, req.Task)
	return resp, req.ServiceName
}

func (s *Server) updateTask(input proto.Message) (proto.Message, string) {
	req := input.(*pb.OperateTaskRequest)
	resp := &pb.ErrorStatusResponse{}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = handle.UpdateTask(req.TableName, req.Task)
	return resp, req.ServiceName
}

func (s *Server) deleteTask(input proto.Message) (proto.Message, string) {
	req := input.(*pb.DeleteTaskRequest)
	resp := &pb.ErrorStatusResponse{}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	if req.CleanTaskSideEffect && req.CleanDelayMinutes < 0 {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"clean_delay_minutes cannot be less than 0",
		)
		return resp, req.ServiceName
	}
	resp.Status = handle.DeleteTask(
		req.TableName,
		req.TaskName,
		req.CleanTaskSideEffect,
		req.CleanDelayMinutes,
	)
	return resp, req.ServiceName
}

func (s *Server) queryTask(input proto.Message) (proto.Message, string) {
	req := input.(*pb.QueryTaskRequest)
	resp := pb.QueryTaskResponse{}

	resp.Status = &pb.ErrorStatus{Code: 0}
	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return &resp, req.ServiceName
	}
	handle.QueryTask(req, &resp)
	return &resp, req.ServiceName
}

func (s *Server) queryTaskCurrentExecution(input proto.Message) (proto.Message, string) {
	req := input.(*pb.QueryTaskCurrentExecutionRequest)
	resp := pb.QueryTaskCurrentExecutionResponse{}

	resp.Status = &pb.ErrorStatus{Code: 0}
	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return &resp, req.ServiceName
	}
	handle.QueryTaskCurrentExecution(req, &resp)
	return &resp, req.ServiceName
}

func (s *Server) triggerDeleteTaskSideEffect(input proto.Message) (proto.Message, string) {
	req := input.(*pb.TriggerDeleteTaskSideEffectRequest)
	resp := &pb.ErrorStatusResponse{}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = handle.TriggerDeleteTaskSideEffect(req.TableName, req.TaskName)
	return resp, req.ServiceName
}

func (s *Server) adminNode(input proto.Message) (proto.Message, string) {
	req := input.(*pb.AdminNodeRequest)

	resp := &pb.AdminNodeResponse{}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}

	resp.Status = &pb.ErrorStatus{Code: 0}
	handle.AdminNodes(utils.FromPbs(req.Nodes), req.MatchPort, req.Op, resp)
	return resp, req.ServiceName
}

func (s *Server) updateNodeWeight(input proto.Message) (proto.Message, string) {
	req := input.(*pb.UpdateNodeWeightRequest)
	resp := &pb.UpdateNodeWeightResponse{}
	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	handle.UpdateNodeWeight(utils.FromPbs(req.Nodes), req.Weights, resp)
	return resp, req.ServiceName
}

func (s *Server) shrinkAz(input proto.Message) (proto.Message, string) {
	req := input.(*pb.ShrinkAzRequest)
	resp := &pb.ShrinkAzResponse{}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}

	handle.ShrinkAz(req, resp)
	return resp, req.ServiceName
}

func (s *Server) listNodes(input proto.Message) (proto.Message, string) {
	req := input.(*pb.ListNodesRequest)
	resp := &pb.ListNodesResponse{}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}

	handle.ListNodes(req, resp)
	return resp, req.ServiceName
}

func (s *Server) queryNodeInfo(input proto.Message) (proto.Message, string) {
	request := input.(*pb.QueryNodeInfoRequest)
	resp := &pb.QueryNodeInfoResponse{}
	resp.Status = &pb.ErrorStatus{Code: 0}

	handle, err := s.getService(request.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, request.ServiceName
	}

	handle.QueryNodeInfo(request, resp)
	return resp, request.ServiceName
}

func (s *Server) queryNodesInfo(input proto.Message) (proto.Message, string) {
	request := input.(*pb.QueryNodesInfoRequest)
	resp := &pb.QueryNodesInfoResponse{}
	resp.Status = &pb.ErrorStatus{Code: 0}

	handle, err := s.getService(request.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, request.ServiceName
	}

	handle.QueryNodesInfo(request, resp)
	return resp, request.ServiceName
}

func (s *Server) switchSchedulerStatus(input proto.Message) (proto.Message, string) {
	req := input.(*pb.SwitchSchedulerStatusRequest)
	resp := &pb.ErrorStatusResponse{}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}

	resp.Status = handle.SwitchSchedulerStatus(req.Enable)
	return resp, req.ServiceName
}

func (s *Server) switchKessPollerStatus(input proto.Message) (proto.Message, string) {
	req := input.(*pb.SwitchKessPollerStatusRequest)
	resp := &pb.ErrorStatusResponse{}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}

	resp.Status = handle.SwitchKessPollerStatus(req.Enable)
	return resp, req.ServiceName
}

func (s *Server) queryService(input proto.Message) (proto.Message, string) {
	req := input.(*pb.QueryServiceRequest)
	resp := &pb.QueryServiceResponse{}

	handle, err := s.getService(req.ServiceName)
	if handle == nil {
		resp.Status = err
		return resp, req.ServiceName
	}

	handle.QueryService(resp)
	return resp, req.ServiceName
}

func (s *Server) removeWatcher(input proto.Message) (proto.Message, string) {
	req := input.(*pb.RemoveWatcherRequest)
	resp := &pb.ErrorStatusResponse{}

	handler, err := s.getService(req.ServiceName)
	if handler == nil {
		resp.Status = err
		return resp, req.ServiceName
	}
	resp.Status = handler.RemoveWatcher(req.WatcherName)
	return resp, req.ServiceName
}

func (s *Server) startHttpServer() {
	router := mux.NewRouter().StrictSlash(true)

	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)

	s.registerHttpHandle(
		router,
		"ListServices",
		"get",
		"/v1/list_services",
		utils.ParseListServicesReq,
		s.listServices,
	)
	s.registerHttpHandle(
		router,
		"CreateService",
		"post",
		"/v1/create_service",
		utils.ParseCreateServiceReq,
		s.createService,
	)
	s.registerHttpHandle(
		router,
		"DeleteService",
		"post",
		"/v1/delete_service",
		utils.ParseDeleteServiceReq,
		s.deleteService,
	)
	s.registerHttpHandle(
		router,
		"AddHubs",
		"post",
		"/v1/add_hubs",
		utils.ParseAddHubsReq,
		s.addHubs,
	)
	s.registerHttpHandle(
		router,
		"RemoveHubs",
		"post",
		"/v1/remove_hubs",
		utils.ParseRemoveHubsReq,
		s.removeHubs,
	)
	s.registerHttpHandle(
		router,
		"UpdateHubs",
		"post",
		"/v1/update_hubs",
		utils.ParseUpdateHubsReq,
		s.updateHubs,
	)
	s.registerHttpHandle(
		router,
		"GiveHints",
		"post",
		"/v1/give_hints",
		utils.ParseGiveHintsReq,
		s.giveHints,
	)
	s.registerHttpHandle(
		router,
		"RecallHints",
		"post",
		"/v1/recall_hints",
		utils.ParseRecallHintsReq,
		s.recallHints,
	)
	s.registerHttpHandle(
		router,
		"UpdateScheduleOptions",
		"post",
		"/v1/update_schedule_options",
		utils.ParseUpdateScheduleOptionsReq,
		s.updateScheduleOptions,
	)

	s.registerHttpHandle(
		router,
		"CreateTable",
		"post",
		"/v1/create_table",
		utils.ParseCreateTableReq,
		s.createTable,
	)
	s.registerHttpHandle(
		router,
		"DeleteTable",
		"post",
		"/v1/delete_table",
		utils.ParseDeleteTableReq,
		s.deleteTable,
	)
	s.registerHttpHandle(
		router,
		"UpdateTable",
		"post",
		"/v1/update_table",
		utils.ParseUpdateTableReq,
		s.updateTable,
	)
	s.registerHttpHandle(
		router,
		"UpdateTableJsonArgs",
		"post",
		"/v1/update_table_json_args",
		utils.ParseUpdateTableJsonArgsReq,
		s.updateTableJsonArgs,
	)
	s.registerHttpHandle(
		router,
		"ListTables",
		"get",
		"/v1/list_tables",
		utils.ParseListTablesReq,
		s.listTables,
	)
	s.registerHttpHandle(
		router,
		"QueryTable",
		"get",
		"/v1/query_table",
		utils.ParseQueryTableReq,
		s.queryTable,
	)
	s.registerHttpHandle(
		router,
		"RemoveReplicas",
		"post",
		"/v1/remove_replicas",
		utils.ParseManualRemoveReplicasRequest,
		s.removeReplicas,
	)
	s.registerHttpHandle(
		router,
		"RestoreTable",
		"post",
		"/v1/restore_table",
		utils.ParseRestoreTableRequest,
		s.restoreTable,
	)
	s.registerHttpHandle(
		router,
		"SplitTable",
		"post",
		"/v1/split_table",
		utils.ParseSplitTableReq,
		s.splitTable,
	)

	s.registerHttpHandle(
		router,
		"QueryPartition",
		"get",
		"/v1/query_partition",
		utils.ParseQueryPartitionReq,
		s.queryPartition,
	)

	s.registerHttpHandle(
		router,
		"CreateTask",
		"post",
		"/v1/create_task",
		utils.ParseOperateTaskReq,
		s.createTask,
	)
	s.registerHttpHandle(
		router,
		"UpdateTask",
		"post",
		"/v1/update_task",
		utils.ParseOperateTaskReq,
		s.updateTask,
	)
	s.registerHttpHandle(
		router,
		"DeleteTask",
		"post",
		"/v1/delete_task",
		utils.ParseDeleteTaskReq,
		s.deleteTask,
	)
	s.registerHttpHandle(
		router,
		"QueryTask",
		"get",
		"/v1/query_task",
		utils.ParseQueryTaskReq,
		s.queryTask,
	)
	s.registerHttpHandle(
		router,
		"TriggerDeleteTaskSideEffect",
		"post",
		"/v1/trigger_delete_task_side_effect",
		utils.ParseTriggerDeleteTaskSideEffectReq,
		s.triggerDeleteTaskSideEffect,
	)
	s.registerHttpHandle(
		router,
		"QueryTaskCurrentExecution",
		"get",
		"/v1/query_task_current_execution",
		utils.ParseQueryTaskCurrentExecution,
		s.queryTaskCurrentExecution,
	)
	s.registerHttpHandle(
		router,
		"AdminNode",
		"post",
		"/v1/admin_node",
		utils.ParseAdminNodeReq,
		s.adminNode,
	)
	s.registerHttpHandle(
		router,
		"UpdateNodeWeight",
		"post",
		"/v1/update_node_weight",
		utils.ParseUpdateNodeWeightReq,
		s.updateNodeWeight,
	)
	s.registerHttpHandle(
		router,
		"ShrinkAz",
		"post",
		"/v1/shrink_az",
		utils.ParseShrinkAzReq,
		s.shrinkAz,
	)
	s.registerHttpHandle(
		router,
		"ExpandAzs",
		"post",
		"/v1/expand_azs",
		utils.ParseExpandAzsReq,
		s.expandAzs,
	)
	s.registerHttpHandle(
		router,
		"CancelExpandAzs",
		"post",
		"/v1/cancel_expand_azs",
		utils.ParseCancelExpandAzsReq,
		s.cancelExpandAzs,
	)
	s.registerHttpHandle(
		router,
		"AssignHub",
		"post",
		"/v1/assign_hub",
		utils.ParseAssignHubReq,
		s.assignHub,
	)
	s.registerHttpHandle(
		router,
		"ReplaceNodes",
		"post",
		"/v1/replace_nodes",
		utils.ParseReplaceNodesReq,
		s.replaceNodes,
	)
	s.registerHttpHandle(
		router,
		"ListNodes",
		"get",
		"/v1/list_nodes",
		utils.ParseListNodesReq,
		s.listNodes)
	s.registerHttpHandle(
		router,
		"QueryNodeInfo",
		"get",
		"/v1/query_node_info",
		utils.ParseQueryNodeInfoReq,
		s.queryNodeInfo,
	)
	s.registerHttpHandle(
		router,
		"QueryNodesInfo",
		"post",
		"/v1/query_nodes_info",
		utils.ParseQueryNodesInfoRequest,
		s.queryNodesInfo,
	)

	s.registerHttpHandle(
		router,
		"SwitchSchedulerStatus",
		"post",
		"/v1/switch_scheduler_status",
		utils.ParseSwitchSchedulerStatusReq,
		s.switchSchedulerStatus,
	)
	s.registerHttpHandle(
		router,
		"SwitchKessPollerStatus",
		"post",
		"/v1/switch_kess_poller_status",
		utils.ParseSwitchKessPollerStatusReq,
		s.switchKessPollerStatus,
	)
	s.registerHttpHandle(
		router,
		"QueryService",
		"get",
		"/v1/query_service",
		utils.ParseQueryServiceReq,
		s.queryService,
	)
	s.registerHttpHandle(
		router,
		"RemoveWatcher",
		"post",
		"/v1/remove_watcher",
		utils.ParseRemoveWatcherReq,
		s.removeWatcher,
	)

	if dbg.RunOnebox() {
		s.mockedEnv.RegisterMockAdmin(router)
	}

	logging.Info("%s: start to listening to port %v", s.myself, s.httpPort)
	s.httpServer = &http.Server{Addr: fmt.Sprintf(":%v", s.httpPort), Handler: router}
	if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
		logging.Error("%s: listen to http port %d failed: %s", s.myself, s.httpPort, err.Error())
	}
}
