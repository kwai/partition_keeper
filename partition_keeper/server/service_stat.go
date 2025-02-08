package server

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/delay_execute"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	cmd_base "github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/dbg"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/est"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/watcher"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
)

const (
	kNodesZNode           = "nodes"
	kHintsZNode           = "hints"
	kWatchersZNode        = "watchers"
	kScheduleOptionsZNode = "sched_opts"
)

var (
	flagScheduleIntervalSeconds = flag.Int64(
		"schedule_interval_secs",
		5,
		"schedule interval seconds",
	)
	flagMaxPartitionCount = flag.Int("max_partition_count", 1048576, "max partition count")
)

type serviceProps struct {
	Dropped           bool                     `json:"dropped"`
	DoSchedule        bool                     `json:"do_schedule"`
	DoPoller          bool                     `json:"do_poller"`
	AdjustHubs        bool                     `json:"adjust_hubs"`
	Hubs              []*pb.ReplicaHub         `json:"hubs"`
	SvcTypeRep        string                   `json:"service_type"`
	FailureDomainType pb.NodeFailureDomainType `json:"failure_domain_type"`
	StaticIndexed     bool                     `json:"static_indexed"`
	UsePaz            bool                     `json:"use_paz"`
	Ksn               string                   `json:"ksn"`
	svcStrategy       strategy.ServiceStrategy
	hubmap            *utils.HubHandle
}

func (sp *serviceProps) updateHubList() {
	sp.Hubs = sp.hubmap.ListHubs()
}

func (sp *serviceProps) addHubs(hubs []*pb.ReplicaHub) *pb.ErrorStatus {
	if err := sp.hubmap.AddHubs(hubs); err != nil {
		return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, err.Error())
	}
	sp.updateHubList()
	return pb.ErrStatusOk()
}

func (sp *serviceProps) removeHubs(hubs []*pb.ReplicaHub) *pb.ErrorStatus {
	if err := sp.hubmap.RemoveHubs(hubs); err != nil {
		return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, err.Error())
	}
	sp.updateHubList()
	return pb.ErrStatusOk()
}

func (sp *serviceProps) updateHubProps(hubs []*pb.ReplicaHub) *pb.ErrorStatus {
	if err := sp.hubmap.UpdateHubProps(hubs); err != nil {
		return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, err.Error())
	}
	sp.updateHubList()
	return pb.ErrStatusOk()
}

func (sp *serviceProps) toJson() []byte {
	sp.SvcTypeRep = sp.svcStrategy.GetType().String()
	return utils.MarshalJsonOrDie(sp)
}

type servicePropsFieldsExistenceChecker struct {
	DoPoller *bool `json:"do_poller"`
}

func (sp *serviceProps) fromJson(data []byte) {
	checker := &servicePropsFieldsExistenceChecker{}
	utils.UnmarshalJsonOrDie(data, checker)

	utils.UnmarshalJsonOrDie(data, sp)
	if checker.DoPoller == nil {
		// for compatibility
		sp.DoPoller = true
	}

	sp.hubmap = utils.MapHubs(sp.Hubs)
	// old embedding server services may be setup without service type
	if sp.SvcTypeRep == "" {
		sp.SvcTypeRep = pb.ServiceType_name[int32(pb.ServiceType_colossusdb_embedding_server)]
	}
	svcType := pb.ServiceType(pb.ServiceType_value[sp.SvcTypeRep])
	st, err := strategy.NewServiceStrategy(svcType)
	logging.Assert(err == nil, "")
	sp.svcStrategy = st
}

type serviceChecker struct {
	serviceName string
	skip        bool
}

func NewServiceChecker(serviceName string, skip bool) *serviceChecker {
	ans := &serviceChecker{
		serviceName: serviceName,
		skip:        skip,
	}
	return ans
}

func (s *serviceChecker) checkHubNames(hubs []*pb.ReplicaHub, usePaz bool) *pb.ErrorStatus {
	if s.skip {
		logging.Info("%s skip check hub name", s.serviceName)
		return pb.ErrStatusOk()
	}
	for _, hub := range hubs {
		hub.Az = utils.KrpAzToStandard(hub.Az)
		if hub.Az == "STAGING" {
			continue
		}
		if usePaz {
			if !utils.PazNameValid(hub.Az) {
				return pb.AdminErrorMsg(
					pb.AdminError_kInvalidParameter,
					"can't recognize %s in hub %s as a valid az, use_paz:%t",
					hub.Az,
					hub.Name,
					usePaz,
				)
			}
		} else {
			if !utils.AzNameValid(hub.Az) || utils.PazNameValid(hub.Az) {
				return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter,
					"can't recognize %s in hub %s as a valid az, use_paz:%t", hub.Az, hub.Name, usePaz)
			}
		}
	}
	return pb.ErrStatusOk()
}

type ServiceStat struct {
	serviceName string
	namespace   string

	zkConn                metastore.MetaStore
	zkPath                string
	zkNodesPath           string
	zkHintsPath           string
	zkWatchersPath        string
	zkScheduleOptionsPath string

	serviceDropped         chan<- string
	tablesManager          *TablesManager
	delayedExecutorManager *delay_execute.DelayedExecutorManager

	rpcPoolBuilder rpc.ConnPoolBuilder
	nodesConn      *rpc.ConnPool

	lock           *utils.LooseLock
	state          utils.DroppableStateHolder
	tables         map[int32]*TableStats
	tableNames     map[string]*TableStats
	deletingTables map[string]bool
	properties     serviceProps
	nodes          *node_mgr.NodeStats

	schedOpts          *pb.ScheduleOptions
	nodeScoreEstimator est.Estimator

	countScheduleLoop       int64
	scheduleIntervalSeconds int64
	quitScheduleLoop        chan bool
	triggerSchedule         chan bool

	detector            *node_mgr.KessBasedNodeDetector
	nodeLivenessChanged <-chan map[string]*node_mgr.NodePing

	collector *node_mgr.NodeCollector
	collected <-chan *pb.GetReplicasResponse

	watchers map[string]watcher.NodeWatcher
	checker  *serviceChecker
}

type serviceOpts func(ss *ServiceStat)

func WithDetector(d *node_mgr.KessBasedNodeDetector) serviceOpts {
	return func(ss *ServiceStat) {
		ss.detector = d
	}
}

func WithCollector(c *node_mgr.NodeCollector) serviceOpts {
	return func(ss *ServiceStat) {
		ss.collector = c
	}
}

func WithRpcPoolBuilder(b rpc.ConnPoolBuilder) serviceOpts {
	return func(ss *ServiceStat) {
		ss.rpcPoolBuilder = b
		ss.nodesConn = b.Build()
	}
}

func WithScheduleIntervalSeconds(intervalSecs int64) serviceOpts {
	return func(ss *ServiceStat) {
		ss.scheduleIntervalSeconds = intervalSecs
	}
}

func WithChecker(s *serviceChecker) serviceOpts {
	return func(ss *ServiceStat) {
		ss.checker = s
	}
}
func NewServiceStat(
	name, namespace, parentZkPath string,
	conn metastore.MetaStore,
	tablesManager *TablesManager,
	delayedExecutorManager *delay_execute.DelayedExecutorManager,
	serviceDropped chan<- string,
	opts ...serviceOpts,
) *ServiceStat {
	zkPath := fmt.Sprintf("%s/%s", parentZkPath, name)
	result := &ServiceStat{
		serviceName:            name,
		namespace:              namespace,
		zkConn:                 conn,
		zkPath:                 zkPath,
		zkNodesPath:            fmt.Sprintf("%s/%s", zkPath, kNodesZNode),
		zkHintsPath:            fmt.Sprintf("%s/%s", zkPath, kHintsZNode),
		zkWatchersPath:         fmt.Sprintf("%s/%s", zkPath, kWatchersZNode),
		zkScheduleOptionsPath:  fmt.Sprintf("%s/%s", zkPath, kScheduleOptionsZNode),
		serviceDropped:         serviceDropped,
		tablesManager:          tablesManager,
		delayedExecutorManager: delayedExecutorManager,

		lock:           utils.NewLooseLock(),
		state:          utils.DroppableStateHolder{State: utils.StateInitializing},
		tables:         make(map[int32]*TableStats),
		tableNames:     make(map[string]*TableStats),
		deletingTables: make(map[string]bool),
		properties: serviceProps{
			Dropped:    false,
			DoSchedule: false,
			DoPoller:   true,
			AdjustHubs: false,
			Hubs:       []*pb.ReplicaHub{},
		},

		scheduleIntervalSeconds: *flagScheduleIntervalSeconds,

		watchers: make(map[string]watcher.NodeWatcher),
	}

	for _, opt := range opts {
		opt(result)
	}
	if result.rpcPoolBuilder == nil {
		WithRpcPoolBuilder(&rpc.GrpcPSClientPoolBuilder{})(result)
	}
	if result.detector == nil {
		WithDetector(node_mgr.NewNodeDetector(name))(result)
	}
	if result.collector == nil {
		WithCollector(
			node_mgr.NewNodeCollector(result.serviceName, result.namespace, result.rpcPoolBuilder),
		)(
			result,
		)
	}
	if result.checker == nil {
		WithChecker(NewServiceChecker(result.serviceName, false))(result)
	}
	return result
}

func (s *ServiceStat) InitializeNew(req *pb.CreateServiceRequest) *pb.ErrorStatus {
	svcStrategy, err := strategy.NewServiceStrategy(req.ServiceType)
	if err != nil {
		return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, err.Error())
	}
	prop := serviceProps{
		AdjustHubs: false,
		Dropped:    false,
		// a new created service's scheduler is disabled by default,
		// as we need to wait all nodes to registered
		DoSchedule:        false,
		DoPoller:          true,
		Hubs:              nil,
		FailureDomainType: req.FailureDomainType,
		StaticIndexed:     req.StaticIndexed,
		UsePaz:            req.UsePaz,
		Ksn:               req.ServiceKsn,
		svcStrategy:       svcStrategy,
		hubmap:            utils.NewHubHandle(),
	}

	if status := s.checker.checkHubNames(req.NodesHubs, req.UsePaz); status.Code != 0 {
		return status
	}

	if status := prop.addHubs(req.NodesHubs); status.Code != 0 {
		return status
	}

	data := prop.toJson()
	logging.Info("%s: initialize new service: %s", s.serviceName, string(data))

	succ := s.zkConn.MultiCreate(
		context.Background(),
		[]string{s.zkPath, s.zkNodesPath, s.zkHintsPath, s.zkWatchersPath},
		[][]byte{data, nil, nil, nil},
	)
	logging.Assert(succ, "")
	s.LoadFromZookeeper()

	return pb.ErrStatusOk()
}

func (s *ServiceStat) LoadFromZookeeper() {
	s.loadServiceProperties()
	if s.properties.Dropped {
		s.cleanState()
	} else {
		s.loadScheduleOptions()
		// only for old services which hint feature hadn't come yet
		succ := s.zkConn.Create(context.Background(), s.zkHintsPath, []byte{})
		logging.Assert(succ, "")
		s.loadNodes()
		s.loadTables()
		s.loadWatchers()
		s.startNodeDetector()
		s.startNodeCollector()
		s.resumeScheduler()
		s.state.Set(utils.StateNormal)
	}
}

func (s *ServiceStat) TriggerSchedule() {
	s.triggerSchedule <- true
}

func (s *ServiceStat) GetScheduleLoopCount() int64 {
	return atomic.LoadInt64(&(s.countScheduleLoop))
}

func (s *ServiceStat) WaitScheduleLoopBeyond(count int64) {
	for {
		if s.GetScheduleLoopCount() > count {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (s *ServiceStat) incScheduleLoopCount() {
	atomic.AddInt64(&(s.countScheduleLoop), 1)
}

func (s *ServiceStat) Teardown() *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if len(s.tableNames) > 0 {
		return pb.AdminErrorMsg(
			pb.AdminError_kTableNotEmpty,
			"There are tables under this service:%s, please delete them first",
			s.serviceName,
		)
	}
	if len(s.deletingTables) > 0 {
		return pb.AdminErrorMsg(
			pb.AdminError_kTableNotEmpty,
			"there are tables in deleting, please wait and retry later",
		)
	}

	if now, ok := s.state.Cas(utils.StateNormal, utils.StateDropping); !ok {
		return pb.AdminErrorMsg(pb.AdminError_kServiceNotInServingState,
			"service is in status: %s", now.String())
	}

	s.properties.Dropped = true
	s.updateServiceProperties()

	go s.clean()
	return pb.AdminErrorCode(pb.AdminError_kOk)
}

func (s *ServiceStat) broadcastHubChanged() {
	defer s.lock.UnlockWrite()

	s.nodes.UpdateHubs(s.properties.hubmap, s.properties.AdjustHubs)
	if s.properties.AdjustHubs {
		s.properties.AdjustHubs = false
		s.updateServiceProperties()
	}
	for _, table := range s.tableNames {
		table.UpdateHubs(s.properties.Hubs)
	}
}

func (s *ServiceStat) ensureServing() *pb.ErrorStatus {
	if state := s.state.Get(); state != utils.StateNormal {
		return pb.AdminErrorMsg(pb.AdminError_kServiceNotInServingState,
			"service is in state: %s", state.String())
	}
	return nil
}

func (s *ServiceStat) AddHubs(hubs []*pb.ReplicaHub, tryGather bool) *pb.ErrorStatus {
	s.lock.LockWrite()

	if err := s.ensureServing(); err != nil {
		s.lock.UnlockWrite()
		return err
	}

	if s.properties.StaticIndexed && tryGather {
		s.lock.UnlockWrite()
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"service %s is static indexed, don't allow to gather nodes from other hubs",
			s.serviceName,
		)
	}

	if ans := s.checker.checkHubNames(hubs, s.properties.UsePaz); ans.Code != 0 {
		s.lock.UnlockWrite()
		return ans
	}
	if ans := s.properties.addHubs(hubs); ans.Code != 0 {
		s.lock.UnlockWrite()
		return ans
	}
	if tryGather {
		ans := s.nodes.AuditUpdateHubs(s.properties.hubmap, s.getCurrentTablesResource())
		if ans != nil {
			s.properties.removeHubs(hubs)
			s.lock.UnlockWrite()
			return ans
		}
	}
	if err := s.detector.UpdateHubs(s.properties.hubmap.ListHubs()); err != nil {
		s.properties.removeHubs(hubs)
		s.lock.UnlockWrite()
		return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, err.Error())
	}
	s.properties.AdjustHubs = tryGather
	s.updateServiceProperties()
	// the lock will be unlocked after broadcast
	go s.broadcastHubChanged()
	return pb.ErrStatusOk()
}

func (s *ServiceStat) RemoveHubs(hubs []*pb.ReplicaHub, tryScatter bool) *pb.ErrorStatus {
	s.lock.LockWrite()
	if err := s.ensureServing(); err != nil {
		s.lock.UnlockWrite()
		return err
	}
	if s.properties.StaticIndexed && tryScatter {
		s.lock.UnlockWrite()
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"service %s is static indexed, don't allow to scatter nodes to others hubs",
			s.serviceName,
		)
	}
	if ans := s.properties.removeHubs(hubs); ans.Code != 0 {
		s.lock.UnlockWrite()
		return ans
	}
	if err := s.detector.UpdateHubs(s.properties.hubmap.ListHubs()); err != nil {
		logging.Fatal("remove hub update az failed: %s", err.Error())
	}

	s.properties.AdjustHubs = tryScatter
	s.updateServiceProperties()
	go s.broadcastHubChanged()
	return pb.ErrStatusOk()
}

func (s *ServiceStat) UpdateHubs(hubs []*pb.ReplicaHub) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		return err
	}
	if ans := s.properties.updateHubProps(hubs); ans.Code != 0 {
		return ans
	}

	s.updateServiceProperties()
	for _, table := range s.tableNames {
		table.UpdateHubs(s.properties.Hubs)
	}
	return pb.ErrStatusOk()
}

func (s *ServiceStat) GiveHints(hints map[string]*pb.NodeHints, matchPort bool) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()
	if err := s.ensureServing(); err != nil {
		return err
	}
	return s.nodes.AddHints(hints, matchPort)
}

func (s *ServiceStat) RecallHints(nodes []*pb.RpcNode, matchPort bool) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()
	if err := s.ensureServing(); err != nil {
		return err
	}
	return s.nodes.RemoveHints(nodes, matchPort)
}

func (s *ServiceStat) sanitizeScheduleOptions(
	input *pb.ScheduleOptions,
	names []string,
	mergedInto *pb.ScheduleOptions,
) (*pb.ErrorStatus, est.Estimator) {
	var newEstimator est.Estimator = nil

	for _, name := range names {
		switch name {
		case "enable_primary_scheduler":
			mergedInto.EnablePrimaryScheduler = input.EnablePrimaryScheduler
		case "max_sched_ratio":
			if input.MaxSchedRatio <= 0 || input.MaxSchedRatio > sched.SCHEDULE_RATIO_MAX_VALUE {
				return pb.AdminErrorMsg(
					pb.AdminError_kInvalidParameter,
					"sched percentile should be within [1,1000], reject %d",
					input.MaxSchedRatio,
				), nil
			} else {
				mergedInto.MaxSchedRatio = input.MaxSchedRatio
			}
		case "estimator":
			newEstimator = est.NewEstimator(input.Estimator)
			if newEstimator == nil {
				return pb.AdminErrorMsg(
					pb.AdminError_kInvalidParameter,
					"estimator %s not exist",
					input.Estimator,
				), nil
			} else {
				mergedInto.Estimator = input.Estimator
				mergedInto.ForceRescoreNodes = true
			}
		case "force_rescore_nodes":
			mergedInto.ForceRescoreNodes = input.ForceRescoreNodes
		case "enable_split_balancer":
			mergedInto.EnableSplitBalancer = input.EnableSplitBalancer
		case "hash_arranger_add_replica_first":
			mergedInto.HashArrangerAddReplicaFirst = input.HashArrangerAddReplicaFirst
		case "hash_arranger_max_overflow_replicas":
			if input.HashArrangerMaxOverflowReplicas < 0 {
				return pb.AdminErrorMsg(
					pb.AdminError_kInvalidParameter,
					"max overflow replicas should be >=0, reject %d",
					input.HashArrangerMaxOverflowReplicas,
				), nil
			} else {
				mergedInto.HashArrangerMaxOverflowReplicas = input.HashArrangerMaxOverflowReplicas
			}
		case "max_learning_parts_per_node":
			mergedInto.MaxLearningPartsPerNode = input.MaxLearningPartsPerNode
		}
	}
	// update estimator will need to rescore all nodes automatically
	if newEstimator != nil {
		mergedInto.ForceRescoreNodes = true
	}
	return nil, newEstimator
}

func (s *ServiceStat) UpdateScheduleOptions(
	opts *pb.ScheduleOptions,
	names []string,
) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		return err
	}
	result := proto.Clone(s.schedOpts).(*pb.ScheduleOptions)
	var err *pb.ErrorStatus
	var newEstimator est.Estimator
	if err, newEstimator = s.sanitizeScheduleOptions(opts, names, result); err != nil {
		return err
	}

	s.lock.AllowRead()
	data := utils.MarshalJsonOrDie(result)
	succ := s.zkConn.Set(context.Background(), s.zkScheduleOptionsPath, data)
	logging.Assert(succ, "")
	s.lock.DisallowRead()
	s.schedOpts = result
	if newEstimator != nil {
		s.nodeScoreEstimator = newEstimator
	} else {
		logging.Assert(
			s.nodeScoreEstimator.Name() == s.schedOpts.Estimator,
			"%s: estimator not match: %s vs %s",
			s.serviceName,
			s.nodeScoreEstimator.Name(),
			s.schedOpts.Estimator,
		)
	}
	s.cancelRunningPlans()
	return pb.ErrStatusOk()
}

func (s *ServiceStat) validateExpandReq(req *pb.ExpandAzsRequest) *pb.ErrorStatus {
	hubSize := s.nodes.GetHubSize()
	azHubs := s.properties.hubmap.DivideByAz()

	for _, opt := range req.AzOptions {
		hubs, ok := azHubs[opt.Az]
		if !ok {
			return pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"az %s not exists in service %s",
				opt.Az,
				req.ServiceName,
			)
		}
		currentSize := 0
		for _, hub := range hubs {
			currentSize += hubSize[hub]
		}
		if opt.NewSize <= int32(currentSize) {
			return pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"new size %d smaller than %s size %d",
				opt.NewSize,
				opt.Az,
				currentSize,
			)
		}
	}
	return nil
}

func (s *ServiceStat) cloneWatcher(name string) watcher.NodeWatcher {
	obj, ok := s.watchers[name]
	if ok {
		return obj.Clone()
	} else {
		return nil
	}
}

func (s *ServiceStat) ExpandAzs(req *pb.ExpandAzsRequest) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()
	if err := s.ensureServing(); err != nil {
		return err
	}

	err := s.validateExpandReq(req)
	if err != nil {
		return err
	}

	w := s.cloneWatcher(watcher.AzSizeWatcherName)
	if w != nil {
		currentReq := w.GetParameters().(*pb.ExpandAzsRequest)
		azSize := currentReq.GetExpandMap()
		for _, opt := range req.AzOptions {
			if lastSize, ok := azSize[opt.Az]; ok {
				logging.Info(
					"%s: update az size watcher for %s %d -> %d",
					s.serviceName,
					opt.Az,
					lastSize,
					opt.NewSize,
				)
				azSize[opt.Az] = opt.NewSize
			} else {
				logging.Info("%s: add new az size watcher for %s: %d", s.serviceName, opt.Az, opt.NewSize)
				azSize[opt.Az] = opt.NewSize
			}
		}

		currentReq.SetExpandMap(azSize)
		w.UpdateParameters(currentReq)
		data := w.Serialize()

		s.lock.AllowRead()
		succ := s.zkConn.Set(context.Background(), s.getWatcherPath(w.Name()), data)
		logging.Assert(succ, "")
		s.lock.DisallowRead()
		s.watchers[w.Name()] = w
	} else {
		w = watcher.NewWatcher(watcher.AzSizeWatcherName)
		w.UpdateParameters(req)
		data := w.Serialize()

		s.lock.AllowRead()
		succ := s.zkConn.Create(context.Background(), s.getWatcherPath(w.Name()), data)
		logging.Assert(succ, "")
		s.lock.DisallowRead()
		s.watchers[w.Name()] = w
	}
	return pb.ErrStatusOk()
}

func (s *ServiceStat) CancelExpandAzs(req *pb.CancelExpandAzsRequest) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		return err
	}

	watcherObj := s.cloneWatcher(watcher.AzSizeWatcherName)
	if watcherObj == nil {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"expand az watcher not exist, no need to cancel",
		)
	}

	currentReq := watcherObj.GetParameters().(*pb.ExpandAzsRequest)
	expandMap := currentReq.GetExpandMap()
	for _, az := range req.Azs {
		delete(expandMap, az)
	}
	if len(expandMap) == 0 {
		logging.Info("%s: no expand az watcher exists, just remove watcher", s.serviceName)
		path := s.getWatcherPath(watcherObj.Name())
		s.lock.AllowRead()
		succ := s.zkConn.Delete(context.Background(), path)
		logging.Assert(succ, "")
		s.lock.DisallowRead()
		delete(s.watchers, watcherObj.Name())
	} else {
		logging.Info("%s: remaining expand az watchers: %v", s.serviceName, expandMap)
		currentReq.SetExpandMap(expandMap)
		watcherObj.UpdateParameters(currentReq)
		data := watcherObj.Serialize()
		s.lock.AllowRead()
		succ := s.zkConn.Set(context.Background(), s.getWatcherPath(watcherObj.Name()), data)
		logging.Assert(succ, "")
		s.lock.DisallowRead()
		s.watchers[watcherObj.Name()] = watcherObj
	}
	return pb.ErrStatusOk()
}

func (s *ServiceStat) ReplaceNodes(req *pb.ReplaceNodesRequest) *pb.ErrorStatus {
	if len(req.SrcNodes) != len(req.DstNodes) {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"src nodes(%d) not match dst nodes(%d)",
			len(req.SrcNodes),
			len(req.DstNodes),
		)
	}

	s.lock.LockWrite()
	defer s.lock.UnlockWrite()
	if err := s.ensureServing(); err != nil {
		return err
	}

	if _, ok := s.watchers[watcher.ReplaceNodesWatcherName]; ok {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"already has a replace node watcher",
		)
	}
	srcMap := map[string]bool{}
	hints := map[string]*pb.NodeHints{}

	for i := range req.SrcNodes {
		src, dst := req.SrcNodes[i], req.DstNodes[i]
		srcNode, dstNode := utils.FromPb(src), utils.FromPb(dst)
		if _, ok := srcMap[srcNode.String()]; ok {
			return pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"duplicate src node %s",
				srcNode.String(),
			)
		}
		info := s.nodes.GetNodeInfoByAddr(srcNode, true)
		if info == nil {
			return pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"can't find src node %s",
				srcNode.String(),
			)
		}
		if _, ok := hints[dstNode.String()]; ok {
			return pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"duplicate node %s in req",
				dstNode.String(),
			)
		}
		dstInfo := s.nodes.GetNodeInfoByAddr(dstNode, true)
		if dstInfo != nil {
			return pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"dst node %s already exists",
				dstNode.String(),
			)
		}
		srcMap[srcNode.String()] = true
		hints[dstNode.String()] = &pb.NodeHints{Hub: info.Hub}
	}

	err := s.nodes.AddHints(hints, true)
	if err.Code != int32(pb.AdminError_kOk) {
		return err
	} else {
		replaceWatcher := watcher.NewWatcher(watcher.ReplaceNodesWatcherName)
		replaceWatcher.UpdateParameters(req)
		s.watchers[replaceWatcher.Name()] = replaceWatcher
		succ := s.zkConn.Create(context.Background(), s.getWatcherPath(replaceWatcher.Name()), replaceWatcher.Serialize())
		logging.Assert(succ, "")
		return pb.ErrStatusOk()
	}
}

func (s *ServiceStat) RemoveWatcher(watcherName string) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		return err
	}

	if watcher, ok := s.watchers[watcherName]; ok {
		logging.Info("%s: remove watcher %s", s.serviceName, watcher.Name())
		path := s.getWatcherPath(watcher.Name())

		s.lock.AllowRead()
		succ := s.zkConn.Delete(context.Background(), path)
		logging.Assert(succ, "")
		s.lock.DisallowRead()

		delete(s.watchers, watcher.Name())
		return pb.ErrStatusOk()
	} else {
		return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, "can't find watcher %s", watcherName)
	}
}

func (s *ServiceStat) validateRestoreParams(kconfPath, restorePath string) error {
	if restorePath == "" {
		return nil
	}
	if kconfPath == "" {
		return errors.New("kconf path shouldn't be empty if restore path isn't empty")
	}
	return nil
}

func (s *ServiceStat) checkAddTableParams(proto *pb.Table, restorePath string) *pb.ErrorStatus {
	if !utils.IsValidName(proto.TableName) {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"invalid table name: %s",
			proto.TableName,
		)
	}
	if proto.PartsCount <= 0 || proto.PartsCount > int32(*flagMaxPartitionCount) {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"partition count should within (0, %d]",
			*flagMaxPartitionCount,
		)
	}
	if proto.BaseTable != "" {
		baseTable := s.tableNames[proto.BaseTable]
		if baseTable == nil {
			return pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"can't schedule as shade of %s as it doesn't exist",
				proto.BaseTable,
			)
		}
		if baseTable.PartsCount != proto.PartsCount {
			return pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"parts count(%d) != base table %s's parts count %d, not allowed",
				proto.PartsCount,
				proto.BaseTable,
				baseTable.PartsCount,
			)
		}
	}
	err := s.validateRestoreParams(proto.KconfPath, restorePath)
	if err != nil {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"invalid create table args: %s",
			err.Error(),
		)
	}
	err = s.properties.svcStrategy.ValidateCreateTableArgs(proto)
	if err != nil {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"invalid create table args: %s",
			err.Error(),
		)
	}

	_, err = s.properties.svcStrategy.MakeStoreOpts(
		utils.RegionsOfAzs(s.properties.hubmap.GetAzs()),
		s.serviceName,
		proto.TableName,
		proto.JsonArgs,
	)
	if err != nil {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"invalid create table args: %s",
			err.Error(),
		)
	}

	return nil
}

func (s *ServiceStat) getCurrentTablesResource() utils.HardwareUnit {
	tableOccupied := utils.HardwareUnit{}
	for _, tbl := range s.tables {
		oneTableOccupy := tbl.GetOccupiedResource()
		tableOccupied.Add(oneTableOccupy)
	}
	return tableOccupied
}

func (s *ServiceStat) auditResource(proto *pb.Table) *pb.ErrorStatus {
	tableOccupied := s.getCurrentTablesResource()
	tableNeed := s.properties.svcStrategy.GetTableResource(proto.JsonArgs)
	tableOccupied.Add(tableNeed)

	hubResources := s.nodes.CountResource()
	return s.nodes.AuditGivenResource(hubResources, tableOccupied)
}

func (s *ServiceStat) registerTrafficManager(trafficKconfPath, tableName string) *pb.ErrorStatus {
	if len(trafficKconfPath) == 0 {
		return nil
	}
	serviceName := s.serviceName
	if !strings.HasPrefix(serviceName, "grpc_") {
		serviceName = "grpc_" + serviceName
	}
	return third_party.AddTrafficConfig(trafficKconfPath, serviceName, tableName)
}

func (s *ServiceStat) AddTable(
	proto *pb.Table,
	restorePath, trafficKconfPath string,
) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		return err
	}
	if err := s.checkAddTableParams(proto, restorePath); err != nil {
		return err
	}
	if err := s.auditResource(proto); err != nil {
		return err
	}

	s.lock.AllowRead()
	id, errStatus := s.tablesManager.RegisterTable(s.serviceName, proto.TableName, proto.BaseTable)
	if errStatus.Code != 0 {
		s.lock.DisallowRead()
		return errStatus
	}
	if err := s.registerTrafficManager(trafficKconfPath, proto.TableName); err != nil {
		s.tablesManager.FinishUnregisterTable(id)
		s.lock.DisallowRead()
		return err
	}

	proto.TableId = id
	proto.BelongToService = s.serviceName

	var baseTable *TableStats = nil
	if proto.BaseTable != "" {
		baseTable = s.tableNames[proto.BaseTable]
	}
	table := NewTableStats(
		s.serviceName,
		s.namespace,
		s.properties.UsePaz,
		s.lock,
		s.nodes,
		s.properties.Hubs,
		s.properties.svcStrategy,
		s.nodesConn,
		s.zkConn,
		s.delayedExecutorManager,
		s.tablesManager.GetZkPath(),
		baseTable,
	)
	table.InitializeNew(proto, restorePath)
	if s.properties.Ksn != "" {
		regions := utils.RegionsOfAzs(table.getHubAzs())
		s.properties.svcStrategy.DisallowServiceAccessOnlyNearestNodes(
			s.properties.Ksn,
			s.serviceName,
			proto.TableName,
			regions,
		)
	}
	s.lock.DisallowRead()

	s.tables[proto.TableId] = table
	s.tableNames[proto.TableName] = table
	return pb.AdminErrorCode(pb.AdminError_kOk)
}

func (s *ServiceStat) RemoveTable(
	tableName string,
	cleanTaskSideEffect bool,
	cleanDelayMinutes int32,
) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		return err
	}
	tbl := s.tableNames[tableName]
	if tbl == nil {
		return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter,
			"can't find table %s", tableName)
	}
	if shades := s.getShades(tableName); len(shades) > 0 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"disallow to delete table %s as it shades %s",
			tableName,
			shades[0],
		)
	}

	s.lock.AllowRead()
	now := time.Now().Unix()
	if err := s.tablesManager.StartUnregisterTable(tbl.TableId, cleanTaskSideEffect, now+int64(cleanDelayMinutes*60)); err != nil {
		s.lock.DisallowRead()
		return err
	}

	if cleanTaskSideEffect {
		tbl.CleanTaskSideEffect(cleanDelayMinutes)
	}

	s.lock.DisallowRead()
	delete(s.tables, tbl.TableId)
	delete(s.tableNames, tableName)
	s.deletingTables[tableName] = true

	go s.stopAndCleanTable(tbl)
	return pb.ErrStatusOk()
}

func (s *ServiceStat) UpdateTable(req *pb.Table) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		return err
	}

	if table, ok := s.tableNames[req.TableName]; !ok {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"table \"%s\" not exist",
			req.TableName,
		)
	} else {
		return table.UpdateTableGrayscale(req)
	}
}

func (s *ServiceStat) UpdateTableJsonArgs(tableName, jsonArgs string) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		return err
	}

	if table, ok := s.tableNames[tableName]; !ok {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"table \"%s\" not exist",
			tableName,
		)
	} else {
		return table.UpdateTableJsonArgs(jsonArgs)
	}
}

func (s *ServiceStat) ListTables(resp *pb.ListTablesResponse) {
	s.lock.LockRead()
	defer s.lock.UnlockRead()

	if err := s.ensureServing(); err != nil {
		resp.Status = err
		return
	}

	resp.Status = pb.ErrStatusOk()
	for _, table := range s.tables {
		ans := &pb.QueryTableResponse{}
		table.QueryTable(false, false, ans)
		if ans.Status.Code == int32(pb.AdminError_kOk) {
			resp.Tables = append(resp.Tables, ans.Table)
		}
	}
	if len(resp.Tables) > 0 {
		sort.Slice(resp.Tables, func(i, j int) bool {
			return resp.Tables[i].TableId < resp.Tables[j].TableId
		})
	}
}

func (s *ServiceStat) QueryTable(req *pb.QueryTableRequest, resp *pb.QueryTableResponse) {
	s.lock.LockRead()
	defer s.lock.UnlockRead()

	if err := s.ensureServing(); err != nil {
		resp.Status = err
		return
	}

	table, ok := s.tableNames[req.TableName]
	if !ok {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kTableNotExists,
			"can't find table %s",
			req.TableName,
		)
		return
	}
	table.QueryTable(req.WithPartitions, req.WithTasks, resp)
}

func (s *ServiceStat) RemoveReplicas(
	req *pb.ManualRemoveReplicasRequest,
	resp *pb.ManualRemoveReplicasResponse,
) {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		resp.Status = err
		return
	}

	table, ok := s.tableNames[req.TableName]
	if !ok {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kTableNotExists,
			"can't find table %s",
			req.TableName,
		)
		return
	}

	table.RemoveReplicas(req, resp)
}

func (s *ServiceStat) RestoreTable(req *pb.RestoreTableRequest) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()
	if err := s.ensureServing(); err != nil {
		return err
	}
	if table, ok := s.tableNames[req.TableName]; !ok {
		return pb.AdminErrorMsg(
			pb.AdminError_kTableNotExists,
			"can't find table %s",
			req.TableName,
		)
	} else {
		return table.StartRestore(req)
	}
}

func (s *ServiceStat) SplitTable(req *pb.SplitTableRequest) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()
	if err := s.ensureServing(); err != nil {
		return err
	}

	if !s.properties.svcStrategy.SupportSplit() {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"%s don't support split currently",
			s.properties.SvcTypeRep,
		)
	}
	table, ok := s.tableNames[req.TableName]
	if !ok {
		return pb.AdminErrorMsg(
			pb.AdminError_kTableNotExists,
			"can't find table %s",
			req.TableName,
		)
	}
	if shades := s.getShades(req.TableName); len(shades) > 0 {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"disallow to split table %s as it shades %s",
			req.TableName, shades[0],
		)
	}
	return table.StartSplit(req)
}

func (s *ServiceStat) QueryPartition(
	req *pb.QueryPartitionRequest,
	resp *pb.QueryPartitionResponse,
) {
	s.lock.LockRead()
	defer s.lock.UnlockRead()

	if err := s.ensureServing(); err != nil {
		resp.Status = err
		return
	}

	table, ok := s.tableNames[req.TableName]
	if !ok {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kTableNotExists,
			"can't find table %s",
			req.TableName,
		)
		return
	}
	table.QueryPartition(req.FromPartition, req.ToPartition, resp)
}

func fillBrief(info *node_mgr.NodeInfo, rec recorder.NodeRecorder, est int) *pb.NodeBrief {
	return &pb.NodeBrief{
		Node:           info.Address.ToPb(),
		Op:             info.Op,
		IsAlive:        info.IsAlive,
		PrimaryCount:   int32(rec.Count(pb.ReplicaRole_kPrimary)),
		SecondaryCount: int32(rec.Count(pb.ReplicaRole_kSecondary)),
		LearnerCount:   int32(rec.Count(pb.ReplicaRole_kLearner)),
		EstimatedCount: int32(est),
		HubName:        info.Hub,
		NodeUniqId:     info.Id,
		NodeIndex:      info.NodeIndex,
		Weight:         info.GetWeight(),
		Score:          info.Score,
	}
}

func (s *ServiceStat) ListNodes(req *pb.ListNodesRequest, resp *pb.ListNodesResponse) {
	s.lock.LockRead()
	defer s.lock.UnlockRead()

	if err := s.ensureServing(); err != nil {
		resp.Status = err
		return
	}

	if req.Az != "" && len(s.properties.hubmap.HubsOnAz(req.Az)) == 0 {
		resp.Status = pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, "invalid az: %s", req.Az)
		return
	}

	if req.HubName != "" {
		_, ok := s.properties.hubmap.GetMap().Get(req.HubName)
		if !ok {
			resp.Status = pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"invalid hub: %s",
				req.HubName,
			)
			return
		}
	}

	matchTable := func(tableName string) bool {
		if req.TableName == "" {
			return true
		}
		return req.TableName == tableName
	}

	rec := recorder.NewAllNodesRecorder(recorder.NewBriefNode)
	estimatedReplicas := map[string]int{}
	for _, table := range s.tableNames {
		if matchTable(table.TableName) {
			table.RecordNodesReplicas(rec, true)
			utils.MapAddTo(estimatedReplicas, table.GetEstimatedReplicas())
		}
	}

	matchNode := func(info *node_mgr.NodeInfo) bool {
		if req.Az != "" {
			return info.Az == req.Az
		}
		if req.HubName != "" {
			return info.Hub == req.HubName
		}
		return true
	}

	resp.Status = &pb.ErrorStatus{Code: 0}
	for _, info := range s.nodes.AllNodes() {
		if matchNode(info) {
			resp.Nodes = append(
				resp.Nodes,
				fillBrief(info, rec.GetNode(info.Id), estimatedReplicas[info.Id]),
			)
		}
	}
	if len(resp.Nodes) > 0 {
		sort.Slice(resp.Nodes, func(i, j int) bool {
			left, right := resp.Nodes[i], resp.Nodes[j]
			if left.HubName != right.HubName {
				return left.HubName < right.HubName
			}
			return left.Node.Compare(right.Node) < 0
		})
	}
}

func (s *ServiceStat) QueryNodesInfo(
	req *pb.QueryNodesInfoRequest,
	resp *pb.QueryNodesInfoResponse,
) {
	s.lock.LockRead()
	defer s.lock.UnlockRead()

	if err := s.ensureServing(); err != nil {
		resp.Status = err
		return
	}

	resp.Status = pb.ErrStatusOk()
	targets := s.nodes.GetNodeInfoByAddrs(utils.FromPbs(req.Nodes), req.MatchPort)
	nodeMap := map[string]bool{}
	for _, target := range targets {
		if target != nil {
			nodeMap[target.Id] = true
		}
	}

	matchTable := func(n string) bool {
		if req.TableName == "" {
			return true
		}
		return n == req.TableName
	}

	var rec recorder.NodesRecorder
	if req.OnlyBrief {
		rec = recorder.RecordGivenNodes(nodeMap, recorder.NewBriefNode)
	} else {
		rec = recorder.RecordGivenNodes(nodeMap, recorder.NewNodePartitions)
	}

	estimatedReplicas := map[string]int{}
	for _, table := range s.tableNames {
		if matchTable(table.TableName) {
			table.RecordNodesReplicas(rec, true)
			utils.MapAddTo(estimatedReplicas, table.GetEstimatedReplicas())
		}
	}

	appendResult := func(m map[utils.TblPartID]int, role pb.ReplicaRole, resp *pb.QueryNodeInfoResponse) {
		for g := range m {
			resp.Replicas = append(resp.Replicas, &pb.ReplicaInfo{
				TableName:   s.tables[g.Table()].TableName,
				PartitionId: g.Part(),
				Role:        role,
			})
		}
	}
	for i, target := range targets {
		if target == nil {
			resp.Nodes = append(resp.Nodes, &pb.QueryNodeInfoResponse{
				Status: pb.AdminErrorMsg(
					pb.AdminError_kNodeNotExisting,
					"can't find node %s",
					req.Nodes[i].ToHostPort(),
				),
			})
		} else {
			nodeRecorder := rec.GetNode(target.Id)
			nodeResp := &pb.QueryNodeInfoResponse{
				Status:   pb.ErrStatusOk(),
				Brief:    fillBrief(target, nodeRecorder, estimatedReplicas[target.Id]),
				Replicas: nil,
				Resource: target.GetResource().Clone(),
			}
			if !req.OnlyBrief {
				np := nodeRecorder.(*recorder.NodePartitions)
				appendResult(np.Primaries, pb.ReplicaRole_kPrimary, nodeResp)
				appendResult(np.Secondaries, pb.ReplicaRole_kSecondary, nodeResp)
				appendResult(np.Learners, pb.ReplicaRole_kLearner, nodeResp)
			}
			resp.Nodes = append(resp.Nodes, nodeResp)
		}
	}
}

func (s *ServiceStat) QueryNodeInfo(
	req *pb.QueryNodeInfoRequest,
	resp *pb.QueryNodeInfoResponse,
) {
	nodesReq := &pb.QueryNodesInfoRequest{
		Nodes:     []*pb.RpcNode{{NodeName: req.NodeName, Port: req.Port}},
		TableName: req.TableName,
		OnlyBrief: req.OnlyBrief,
		MatchPort: req.MatchPort,
	}
	nodesResp := &pb.QueryNodesInfoResponse{}
	s.QueryNodesInfo(nodesReq, nodesResp)
	if nodesResp.Status.Code != int32(pb.AdminError_kOk) {
		resp.Status = nodesResp.Status
		return
	}
	resp.Status = nodesResp.Nodes[0].Status
	resp.Brief = nodesResp.Nodes[0].Brief
	resp.Replicas = nodesResp.Nodes[0].Replicas
	resp.Resource = nodesResp.Nodes[0].Resource
}

func (s *ServiceStat) AdminNodes(
	nodes []*utils.RpcNode,
	matchPort bool,
	op pb.AdminNodeOp,
	resp *pb.AdminNodeResponse,
) {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		resp.Status = err
		return
	}

	resp.Status = pb.ErrStatusOk()
	resp.NodeResults = s.nodes.AdminNodes(nodes, matchPort, op, s.getCurrentTablesResource())
	hasSucceed := 0
	for _, nodeRes := range resp.NodeResults {
		if nodeRes.Code == int32(pb.AdminError_kOk) {
			hasSucceed++
		}
	}
	if hasSucceed > 0 {
		s.cancelRunningPlans()
	}
}

func (s *ServiceStat) UpdateNodeWeight(
	nodes []*utils.RpcNode,
	weights []float32,
	resp *pb.UpdateNodeWeightResponse,
) {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		resp.Status = err
		return
	}
	resp.Status = pb.ErrStatusOk()
	resp.NodeResults = s.nodes.UpdateWeight(nodes, weights)
	s.cancelRunningPlans()
}

func (s *ServiceStat) ShrinkAz(req *pb.ShrinkAzRequest, resp *pb.ShrinkAzResponse) {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		resp.Status = err
		return
	}

	s.nodes.ShrinkAz(
		req.Az,
		req.NewSize,
		s.getCurrentTablesResource(),
		s.properties.StaticIndexed,
		resp,
	)
	if resp.Status.Code == int32(pb.AdminError_kOk) && len(resp.Shrinked) > 0 {
		s.cancelRunningPlans()
	}
}

func (s *ServiceStat) AssignHub(req *pb.AssignHubRequest) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		return err
	}
	if s.properties.StaticIndexed {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"service %s is static indexed, don't allow to manually assign hub for nodes",
			s.serviceName,
		)
	}
	resp := s.nodes.AssignHub(utils.FromPb(req.Node), req.Hub)
	if resp.Code == int32(pb.AdminError_kOk) {
		s.cancelRunningPlans()
	}
	return resp
}

func (s *ServiceStat) SwitchSchedulerStatus(enable bool) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		return err
	}

	s.properties.DoSchedule = enable
	s.updateServiceProperties()
	return pb.ErrStatusOk()
}

func (s *ServiceStat) SwitchKessPollerStatus(enable bool) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		return err
	}

	s.properties.DoPoller = enable
	s.updateServiceProperties()
	s.detector.EnablePoller(enable)
	return pb.ErrStatusOk()
}

func (s *ServiceStat) QueryService(resp *pb.QueryServiceResponse) {
	s.lock.LockRead()
	defer s.lock.UnlockRead()

	if err := s.ensureServing(); err != nil {
		resp.Status = err
		return
	}
	resp.Status = pb.ErrStatusOk()
	resp.ScheduleEnabled = s.properties.DoSchedule
	resp.KessPollerEnabled = s.properties.DoPoller
	resp.NodesHubs = s.properties.hubmap.ListHubs()
	resp.ServiceType = s.properties.svcStrategy.GetType()
	resp.FailureDomainType = s.properties.FailureDomainType
	resp.StaticIndexed = s.properties.StaticIndexed
	resp.SchedOpts = proto.Clone(s.schedOpts).(*pb.ScheduleOptions)
	resp.UsePaz = s.properties.UsePaz
	resp.ServiceKsn = s.properties.Ksn
}

func (s *ServiceStat) CreateTask(tableName string, task *pb.PeriodicTask) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		return err
	}

	table, ok := s.tableNames[tableName]
	if !ok {
		return pb.AdminErrorMsg(
			pb.AdminError_kTableNotExists,
			"can't find table %s",
			tableName,
		)
	}
	return table.CreateTask(task)
}

func (s *ServiceStat) DeleteTask(
	tableName, taskName string,
	cleanTaskSideEffect bool,
	cleanDelayMinutes int32,
) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		return err
	}

	table, ok := s.tableNames[tableName]
	if !ok {
		return pb.AdminErrorMsg(
			pb.AdminError_kTableNotExists,
			"can't find table %s",
			tableName,
		)
	}
	return table.DeleteTask(taskName, cleanTaskSideEffect, cleanDelayMinutes)
}

func (s *ServiceStat) UpdateTask(tableName string, task *pb.PeriodicTask) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	if err := s.ensureServing(); err != nil {
		return err
	}

	table, ok := s.tableNames[tableName]
	if !ok {
		return pb.AdminErrorMsg(
			pb.AdminError_kTableNotExists,
			"can't find table %s",
			tableName,
		)
	}
	return table.UpdateTask(task)
}

func (s *ServiceStat) QueryTask(req *pb.QueryTaskRequest, resp *pb.QueryTaskResponse) {
	s.lock.LockRead()
	defer s.lock.UnlockRead()
	if err := s.ensureServing(); err != nil {
		resp.Status = err
		return
	}
	table, ok := s.tableNames[req.TableName]
	if !ok {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kTableNotExists,
			"can't find table %s",
			req.TableName,
		)
		return
	}
	table.QueryTask(req.TaskName, resp)
}

func (s *ServiceStat) QueryTaskCurrentExecution(
	req *pb.QueryTaskCurrentExecutionRequest,
	resp *pb.QueryTaskCurrentExecutionResponse,
) {
	s.lock.LockRead()
	defer s.lock.UnlockRead()
	if err := s.ensureServing(); err != nil {
		resp.Status = err
		return
	}
	table, ok := s.tableNames[req.TableName]
	if !ok {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kTableNotExists,
			"can't find table %s",
			req.TableName,
		)
		return
	}
	table.QueryTaskCurrentExecution(req.TaskName, resp)
}

func (s *ServiceStat) TriggerDeleteTaskSideEffect(tableName, taskName string) *pb.ErrorStatus {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()
	if err := s.ensureServing(); err != nil {
		return err
	}
	table, ok := s.tableNames[tableName]
	if !ok {
		return pb.AdminErrorMsg(
			pb.AdminError_kTableNotExists,
			"can't find table %s",
			tableName,
		)
	}
	return table.TriggerDeleteTaskSideEffect(taskName)
}

func (s *ServiceStat) stop() {
	s.detector.Stop()
	s.collector.Stop()
	s.quitScheduleLoop <- true
	logging.Info("%s: schedule loop stopped", s.serviceName)

	for _, table := range s.tableNames {
		table.Stop(true)
	}
}

func (s *ServiceStat) clean() {
	s.stop()
	s.cleanState()
}

func (s *ServiceStat) cleanTableState(tableName string, tableId int32) {
	path := s.tablesManager.GetTableZkPath(tableName)
	logging.Assert(s.zkConn.RecursiveDelete(context.Background(), path),
		"clean table states %s failed",
		tableName,
	)
	s.tablesManager.FinishUnregisterTable(tableId)
}

func (s *ServiceStat) stopAndCleanTable(tbl *TableStats) {
	tbl.Stop(true)
	s.cleanTableState(tbl.TableName, tbl.TableId)
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()
	delete(s.deletingTables, tbl.TableName)
}

func (s *ServiceStat) cleanState() {
	for table := range s.tableNames {
		logging.Assert(
			s.zkConn.RecursiveDelete(context.Background(), s.tablesManager.GetTableZkPath(table)),
			"",
		)
	}
	s.tablesManager.CleanService(s.serviceName)
	logging.Assert(s.zkConn.RecursiveDelete(context.Background(), s.zkPath), "")
	s.state.Set(utils.StateDropped)
	s.serviceDropped <- s.serviceName
}

func (s *ServiceStat) loadServiceProperties() {
	propData, exists, succ := s.zkConn.Get(context.Background(), s.zkPath)
	logging.Assert(succ && exists, "")

	s.properties.fromJson(propData)
	logging.Info("%s: load service properties: %s", s.serviceName, string(propData))
}

func (s *ServiceStat) updateServiceProperties() {
	data := s.properties.toJson()
	succ := s.zkConn.Set(context.Background(), s.zkPath, data)
	logging.Assert(succ, "")
}

func (s *ServiceStat) getTableScheduleOrder(tableBase map[string]string) []string {
	next := map[string][]string{}
	prevCount := map[string]int{}

	output := []string{}
	for shadedTable, baseTable := range tableBase {
		if baseTable != "" {
			next[baseTable] = append(next[baseTable], shadedTable)
			prevCount[shadedTable] = 1
		} else {
			output = append(output, shadedTable)
		}
	}

	head := 0
	for head < len(output) {
		currNext := next[output[head]]
		if len(currNext) > 0 {
			for _, tableName := range currNext {
				prevCount[tableName]--
				if prevCount[tableName] == 0 {
					output = append(output, tableName)
					delete(prevCount, tableName)
				}
			}
		}
		head++
	}
	for tableName, prevs := range prevCount {
		logging.Error(
			"%s: table %s has %d prevs, shouldn't happen",
			s.serviceName,
			tableName,
			prevs,
		)
		output = append(output, tableName)
	}
	logging.Assert(
		len(output) == len(tableBase),
		"%s: output tables %d vs input tables %d",
		s.serviceName,
		len(output),
		len(tableBase),
	)
	return output
}

func (s *ServiceStat) loadTables() {
	tables := s.tablesManager.GetTablesByService(s.serviceName)
	tableBase := map[string]string{}
	for tableName, tbl := range tables {
		tableBase[tableName] = tbl.BaseTable
	}
	orderedTable := s.getTableScheduleOrder(tableBase)

	for _, tableName := range orderedTable {
		info := tables[tableName]
		if s.tablesManager.IsUnregistered(info.TableId) {
			s.addTableTaskSideEffectCleaner(tableName)
			s.cleanTableState(tableName, info.TableId)
		} else {
			var baseTable *TableStats = nil
			if info.BaseTable != "" {
				baseTable = s.tableNames[info.BaseTable]
				logging.Assert(baseTable != nil, "can't find base table %s for %s", info.BaseTable, tableName)
			}
			table := NewTableStats(
				s.serviceName,
				s.namespace,
				s.properties.UsePaz,
				s.lock,
				s.nodes,
				s.properties.Hubs,
				s.properties.svcStrategy,
				s.nodesConn,
				s.zkConn,
				s.delayedExecutorManager,
				s.tablesManager.GetZkPath(),
				baseTable,
			)
			table.LoadFromZookeeper(tableName)
			s.tables[table.TableId] = table
			s.tableNames[table.TableName] = table
		}
	}
}

func (s *ServiceStat) loadScheduleOptions() {
	data, exists, succ := s.zkConn.Get(context.Background(), s.zkScheduleOptionsPath)
	logging.Assert(succ, "")
	if exists {
		s.schedOpts = &pb.ScheduleOptions{}
		utils.UnmarshalJsonOrDie(data, s.schedOpts)
		if s.schedOpts.Estimator == "" {
			s.schedOpts.Estimator = est.DEFAULT_ESTIMATOR
			s.schedOpts.ForceRescoreNodes = true
		}
		logging.Info("%s: load existing schedule options: %v", s.serviceName, s.schedOpts)
	} else {
		s.schedOpts = &pb.ScheduleOptions{
			EnablePrimaryScheduler:  true,
			MaxSchedRatio:           10,
			Estimator:               est.DEFAULT_ESTIMATOR,
			ForceRescoreNodes:       false,
			EnableSplitBalancer:     false,
			MaxLearningPartsPerNode: 0,
		}
		data = utils.MarshalJsonOrDie(s.schedOpts)
		succ = s.zkConn.Create(context.Background(), s.zkScheduleOptionsPath, data)
		logging.Assert(succ, "")
		logging.Info("%s: no schedule options exists, create default one: %v", s.serviceName, s.schedOpts)
	}
	s.nodeScoreEstimator = est.NewEstimator(s.schedOpts.Estimator)
	logging.Assert(
		s.nodeScoreEstimator != nil,
		"%s: can't find estimator %s",
		s.serviceName,
		s.schedOpts.Estimator,
	)
}

func (s *ServiceStat) getShades(tableName string) []string {
	output := []string{}
	for _, tableStat := range s.tableNames {
		if tableStat.BaseTable == tableName {
			output = append(output, tableStat.TableName)
		}
	}
	return output
}

func (s *ServiceStat) getWatcherPath(node string) string {
	return fmt.Sprintf("%s/%s", s.zkWatchersPath, node)
}

func (s *ServiceStat) loadWatchers() {
	logging.Info("%s: start to load watchers", s.serviceName)
	succ := s.zkConn.Create(context.Background(), s.zkWatchersPath, []byte(""))
	logging.Assert(succ, "")

	watcherNodes, exists, succ := s.zkConn.Children(context.Background(), s.zkWatchersPath)
	logging.Assert(exists && succ, "")

	for _, watcherName := range watcherNodes {
		watcher := watcher.NewWatcher(watcherName)
		data, exists, succ := s.zkConn.Get(
			context.Background(),
			s.getWatcherPath(watcherName),
		)
		logging.Assert(exists && succ, "")
		logging.Info("%s: load watcher %s: %s", s.serviceName, watcherName, string(data))
		watcher.Deserialize(data)
		s.watchers[watcherName] = watcher
	}
}

func (s *ServiceStat) addTableTaskSideEffectCleaner(tableName string) {
	tablePath := fmt.Sprintf("%s/%s", s.tablesManager.GetZkPath(), tableName)
	tasksPath := fmt.Sprintf("%s/%s", tablePath, kTasksZNode)
	// load table
	tableProps, exists, succ := s.zkConn.Get(context.Background(), tablePath)
	logging.Assert(succ, "")
	if !exists {
		return
	}
	tablePb := &pb.Table{}
	utils.UnmarshalJsonOrDie(tableProps, tablePb)
	cleanTaskSideEffect, cleanExecuteTime := s.tablesManager.GetRecyclingIdContext(tablePb.TableId)
	if !cleanTaskSideEffect {
		logging.Info(
			"Table:%s is in the recycle, but does not need to clean task side effect",
			tableName,
		)
		return
	}

	// load task
	children, exists, succ := s.zkConn.Children(context.Background(), tasksPath)
	logging.Assert(succ, "")
	if !exists {
		logging.Info(
			"Table:%s is in the recycle, and needs to clean task side effect. But it has no task",
			tableName,
		)
		return
	}

	var regions []string
	for region := range utils.RegionsOfAzs(s.properties.hubmap.GetAzs()) {
		regions = append(regions, region)
	}

	logging.Info(
		"Table:%s is in the recycle, start add delayed executor task, task num:%d",
		tableName,
		len(children),
	)
	for _, child := range children {
		taskPath := fmt.Sprintf("%s/%s", tasksPath, child)
		data, exists, succ := s.zkConn.Get(context.Background(), taskPath)
		logging.Assert(succ && exists, "")
		property := &pb.PeriodicTask{}
		utils.UnmarshalJsonOrDie(data, property)
		executeName := getTaskExecutorName(tablePb.TableId, property.TaskName)
		if s.delayedExecutorManager.ExecutorExist(executeName) {
			logging.Info(
				"This task:%s has been created delayed executor, so it does not need to be created",
				executeName,
			)
			continue
		}
		taskContext := cmd_base.HandlerParams{
			TableId:     tablePb.TableId,
			ServiceName: s.serviceName,
			TableName:   tablePb.TableName,
			TaskName:    property.TaskName,
			Regions:     regions,
			KconfPath:   tablePb.KconfPath,
			Args:        property.Args,
		}

		delayedExecuteInfo := delay_execute.ExecuteInfo{
			ExecuteType:    property.TaskName,
			ExecuteName:    executeName,
			ExecuteTime:    cleanExecuteTime,
			ExecuteContext: taskContext,
		}

		s.delayedExecutorManager.AddTask(delayedExecuteInfo)
		logging.Info(
			"Add delayed executor, service:%s, table:%s, task:%s, executor time:%d",
			s.serviceName,
			tablePb.TableName,
			property.TaskName,
			cleanExecuteTime,
		)
	}
}

func (s *ServiceStat) loadNodes() {
	s.nodes = node_mgr.NewNodeStats(s.serviceName, s.lock, s.zkNodesPath, s.zkHintsPath, s.zkConn).
		WithFailureDomainType(s.properties.FailureDomainType)
	s.nodes.LoadFromZookeeper(s.properties.hubmap, s.properties.AdjustHubs)
	if s.properties.AdjustHubs {
		s.properties.AdjustHubs = false
		s.updateServiceProperties()
	}
}

func (s *ServiceStat) cleanNodes() {
	conflict := s.nodes.CheckNodeIndex(s.properties.StaticIndexed)
	third_party.PerfLog("reco", "reco.colossusdb.conflict_nodes", s.serviceName, uint64(conflict))

	rec := recorder.NewAllNodesRecorder(recorder.NewBriefNode)
	for _, table := range s.tableNames {
		table.RecordNodesReplicas(rec, false)
	}
	s.nodes.RemoveUnusedNodeInfos(rec)
}

func (s *ServiceStat) cancelRunningPlans() {
	for _, table := range s.tableNames {
		table.CancelPlan()
	}
}

func (s *ServiceStat) checkWatchers() bool {
	allSatisfied := true

	for name, watcher := range s.watchers {
		ops, satisfied := watcher.StatSatisfied(s.serviceName, s.nodes)
		if satisfied {
			logging.Assert(
				len(ops) == 0,
				"%s: watcher %s satisfied but has op %v",
				s.serviceName,
				name,
				ops,
			)
			watcherPath := fmt.Sprintf("%s/%s", s.zkWatchersPath, name)
			succ := s.zkConn.Delete(context.Background(), watcherPath)
			logging.Assert(succ, "")
			delete(s.watchers, name)
		} else {
			allSatisfied = false
			for op, nodes := range ops {
				results := s.nodes.AdminNodes(nodes, true, op, s.getCurrentTablesResource())
				for _, result := range results {
					if result.Code != int32(pb.AdminError_kOk) {
						logging.Error("%s: admin node got error: %d %s", s.serviceName, result.Code, result.Message)
					}
				}
			}
		}
	}
	return allSatisfied
}

func (s *ServiceStat) checkScores() bool {
	if !s.nodes.RefreshScore(s.nodeScoreEstimator, s.schedOpts.ForceRescoreNodes) {
		return false
	}
	if s.schedOpts.ForceRescoreNodes {
		s.schedOpts.ForceRescoreNodes = false
		data := utils.MarshalJsonOrDie(s.schedOpts)
		s.lock.AllowRead()
		succ := s.zkConn.Set(context.Background(), s.zkScheduleOptionsPath, data)
		logging.Assert(succ, "")
		s.lock.DisallowRead()
	}
	return true
}

func (s *ServiceStat) scheduleTables() {
	if !s.checkWatchers() {
		logging.Info("%s: check watchers failed, don't run scheduler", s.serviceName)
		return
	}
	if !s.checkScores() {
		logging.Info("%s: check node scores failed, don't run scheduler", s.serviceName)
		return
	}

	tableBase := map[string]string{}
	for tableName, tableStat := range s.tableNames {
		tableBase[tableName] = tableStat.BaseTable
	}
	schedOrder := s.getTableScheduleOrder(tableBase)
	for _, tableName := range schedOrder {
		table := s.tableNames[tableName]
		table.Schedule(s.properties.DoSchedule, s.schedOpts)
	}
	for tableName, tableStat := range s.tableNames {
		tableStat.partsCountMonitor(tableName)
	}
}

func (s *ServiceStat) schedule() {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	// TODO: refactor the scheduler, as the node recorders are necessary for
	// every stage of schedule. so we can precalculate one and reuse it
	s.cleanNodes()
	s.scheduleTables()
}

func (s *ServiceStat) changeNodeLiveNess(changedMap map[string]*node_mgr.NodePing) {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	deadNodes := make(map[string]bool)
	for id, rep := range changedMap {
		if !rep.IsAlive {
			deadNodes[id] = true
		}
	}
	for _, table := range s.tables {
		table.MarkDeadNodes(deadNodes)
	}

	s.nodes.UpdateStats(changedMap)
}

func (s *ServiceStat) notifyRemovePartition(
	addr *utils.RpcNode,
	info *pb.ReplicaReportInfo,
	maxTableReplicas int,
) {
	s.nodesConn.Run(addr, func(c interface{}) error {
		client := c.(pb.PartitionServiceClient)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		request := &pb.RemoveReplicaRequest{
			PartitionId:       info.PartitionId,
			TableId:           info.TableId,
			PeerInfo:          info.PeerInfo,
			EstimatedReplicas: int32(maxTableReplicas),
			AuthKey:           s.namespace,
		}
		request.PeerInfo.MembershipVersion++
		status, err := client.RemoveReplica(ctx, request)
		if err != nil {
			logging.Error(
				"[change_state] %s: notify node %s to remove replica t%d.p%d request failed, err: %s, estimated replicas: %d",
				s.serviceName,
				addr.String(),
				info.TableId,
				info.PartitionId,
				err.Error(),
				request.EstimatedReplicas,
			)
		} else if status.Code != int32(pb.PartitionError_kOK) {
			logging.Error("[change_state] %s: notify node %s to remove replica t%d.p%d request response: %s, estimated replicas: %d",
				s.serviceName,
				addr.String(),
				info.TableId,
				info.PartitionId,
				status.Message,
				request.EstimatedReplicas,
			)
		} else {
			logging.Info("[change_state] %s: notify node %s to remove replica t%d.p%d succeed, estimated replicas: %d",
				s.serviceName,
				addr.String(),
				info.TableId,
				info.PartitionId,
				request.EstimatedReplicas,
			)
		}
		return err
	})
}

func (s *ServiceStat) handleCollectServerInfo(
	report *pb.GetReplicasResponse,
	info *node_mgr.NodeInfo,
) {
	var err error
	resource, err := s.properties.svcStrategy.GetServerResource(
		report.ServerInfo.StatisticsInfo,
		info.Id,
	)
	if err != nil {
		logging.Verbose(1, "%s: can't get %s server resource: %s",
			s.serviceName, info.LogStr(), err.Error(),
		)
		third_party.PerfLog1(
			"reco",
			"reco.colossusdb.invalid_server_info",
			s.serviceName,
			info.Address.String(),
			1,
		)
	} else {
		info.SetResource(s.serviceName, resource)
	}
}

func (s *ServiceStat) handleCollect(report *pb.GetReplicasResponse) {
	s.lock.LockWrite()
	defer s.lock.UnlockWrite()

	nodeInfo := s.nodes.GetNodeInfo(report.ServerInfo.NodeId)
	if nodeInfo == nil {
		logging.Warning(
			"%s: can't find node info for %s, just skip",
			s.serviceName,
			report.ServerInfo.NodeId,
		)
		return
	}
	if !nodeInfo.IsAlive {
		logging.Info(
			"%s: node %s has dead, skip collection as it may stale",
			s.serviceName,
			nodeInfo.LogStr(),
		)
		return
	}
	s.handleCollectServerInfo(report, nodeInfo)

	for i := 0; i < len(report.Results); i++ {
		status := report.Results[i]
		info := report.Infos[i]
		if status.Code != int32(pb.PartitionError_kOK) {
			logging.Error(
				"%s: node %s report partition with invalid status result %s",
				s.serviceName,
				nodeInfo.LogStr(),
				status.Message,
			)
			continue
		}
		if !pb.RemoveOtherPeers(info.PeerInfo, nodeInfo.Address.ToPb()) {
			logging.Warning(
				"%s: can't find node %v in peer info %v, skip",
				s.serviceName,
				nodeInfo.Address,
				info.PeerInfo,
			)
			continue
		}
		table, ok := s.tables[info.TableId]
		if !ok {
			if s.tablesManager.IsUnregistered(info.TableId) {
				go s.notifyRemovePartition(nodeInfo.Address, info, 0)
			} else {
				logging.Error(
					"%s: can't find table meta %d, don't remove for safety. please remove it manually if indeed unused",
					s.serviceName,
					info.TableId,
				)
			}
			continue
		}
		if !table.UpdateNodeFact(nodeInfo.Id, info) {
			shouldSchedule := !dbg.RunInSafeMode() && table.shouldScheduleNode(nodeInfo.Id)
			logging.Info(
				"[change_state] %s: table %d can't recognize %s as partition %d member, notify remove, should schedule: %v",
				s.serviceName,
				table.TableId,
				nodeInfo.Id,
				info.PartitionId,
				shouldSchedule,
			)
			if shouldSchedule {
				go s.notifyRemovePartition(
					nodeInfo.Address,
					info,
					table.GetEstimatedReplicasOnNode(nodeInfo.Id),
				)
			}
		} else {
			logging.Verbose(1, "%s: update node fact of t%d.p%d at %s",
				s.serviceName, info.TableId,
				info.PartitionId, nodeInfo.LogStr(),
			)
		}
	}
}

func (s *ServiceStat) runScheduleLoop() {
	ticker := time.NewTicker(time.Duration(s.scheduleIntervalSeconds) * time.Second)

	for {
		select {
		case <-ticker.C:
			s.schedule()
		case <-s.triggerSchedule:
			s.schedule()
		case changedNodes := <-s.nodeLivenessChanged:
			s.changeNodeLiveNess(changedNodes)
		case nodeServed := <-s.collected:
			s.handleCollect(nodeServed)
			s.collector.NotifyNodeCollectingHandled(nodeServed.ServerInfo.NodeId)
		case <-s.quitScheduleLoop:
			ticker.Stop()
			return
		}
		s.incScheduleLoopCount()
	}
}

func (s *ServiceStat) resumeScheduler() {
	s.quitScheduleLoop = make(chan bool)
	s.triggerSchedule = make(chan bool)
	go s.runScheduleLoop()
}

func (s *ServiceStat) startNodeDetector() {
	s.nodeLivenessChanged = s.detector.Start(
		s.nodes.GetNodeLivenessMap(false),
		s.properties.StaticIndexed,
		s.properties.DoPoller,
		s.properties.UsePaz,
	)
	err := s.detector.UpdateHubs(s.properties.hubmap.ListHubs())
	logging.Assert(err == nil, "")
}

func (s *ServiceStat) startNodeCollector() {
	s.collected = s.collector.Start(s.nodes)
}
