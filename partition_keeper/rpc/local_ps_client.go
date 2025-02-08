package rpc

import (
	"context"
	"flag"
	"strings"
	"sync"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var (
	flagsReplicaStatistics = flag.String("replica_statistics", "", "replica_statistics")
)

type MockLocalReplicasInfo struct {
	allReplicasInfo map[string]*pb.GetReplicasResponse
	mu              sync.Mutex
}

func (m *MockLocalReplicasInfo) Add(k string, v *pb.GetReplicasResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.allReplicasInfo[k] = v
}

func (m *MockLocalReplicasInfo) Delete(k string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.allReplicasInfo[k]
	if ok {
		delete(m.allReplicasInfo, k)
	}
}

func (m *MockLocalReplicasInfo) Get(k string) (*pb.GetReplicasResponse, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	resp, ok := m.allReplicasInfo[k]
	if ok {
		return resp, true
	} else {
		return nil, false
	}
}

var MockAllReplicasInfo = MockLocalReplicasInfo{
	allReplicasInfo: make(map[string]*pb.GetReplicasResponse),
}

type LocalPartitionServiceClient struct {
	*mockedRpcClient
	mu                 sync.Mutex
	replicas           map[utils.TblPartID]*pb.AddReplicaRequest
	removedReplicas    map[utils.TblPartID]*pb.RemoveReplicaRequest
	lastCommands       map[utils.TblPartID]*pb.CustomCommandRequest
	splitReqs          map[utils.TblPartID]*pb.ReplicaSplitRequest
	splitCleanupReqs   map[utils.TblPartID]*pb.ReplicaSplitCleanupRequest
	lastGetReplicasReq *pb.GetReplicasRequest

	statistics     map[string]string
	repStatistics  map[string]string
	readyToPromote bool
	DuplicateReq   bool
}

func NewLocalPSClient(hostPort string, behaviors *RpcMockBehaviors) *LocalPartitionServiceClient {
	localClient := &LocalPartitionServiceClient{
		mockedRpcClient:  newMockedRpcClient(behaviors, hostPort),
		replicas:         make(map[utils.TblPartID]*pb.AddReplicaRequest),
		removedReplicas:  make(map[utils.TblPartID]*pb.RemoveReplicaRequest),
		lastCommands:     make(map[utils.TblPartID]*pb.CustomCommandRequest),
		splitReqs:        make(map[utils.TblPartID]*pb.ReplicaSplitRequest),
		splitCleanupReqs: make(map[utils.TblPartID]*pb.ReplicaSplitCleanupRequest),
		statistics:       make(map[string]string),
		repStatistics:    make(map[string]string),
		readyToPromote:   true,
	}

	for _, statisticsInfo := range strings.Split(*flagsReplicaStatistics, ",") {
		kv := strings.SplitN(statisticsInfo, ":", 2)
		if len(kv) == 2 {
			localClient.repStatistics[kv[0]] = kv[1]
		}
	}

	resp, ok := MockAllReplicasInfo.Get(hostPort)
	if ok {
		localClient.prepareMockReplicas(resp)
		MockAllReplicasInfo.Delete(hostPort)
	}
	return localClient
}

func (c *LocalPartitionServiceClient) prepareMockReplicas(resp *pb.GetReplicasResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if resp.ServerInfo != nil {
		c.statistics = resp.ServerInfo.StatisticsInfo
	}

	for _, info := range resp.Infos {
		partitionId := info.PartitionId
		tableId := info.TableId
		peerInfo := info.PeerInfo

		globalId := utils.MakeTblPartID(tableId, partitionId)
		if _, ok := c.replicas[globalId]; ok {
			continue
		}

		req := &pb.AddReplicaRequest{
			Part: &pb.PartitionInfo{
				PartitionId: partitionId,
				TableId:     tableId,
			},
			PeerInfo: &pb.PartitionPeerInfo{
				Peers: []*pb.PartitionReplica{},
			},
		}
		req.PeerInfo = proto.Clone(peerInfo).(*pb.PartitionPeerInfo)

		c.replicas[globalId] = proto.Clone(req).(*pb.AddReplicaRequest)
		if c.replicas[globalId].PeerInfo != nil && c.replicas[globalId].PeerInfo.Peers != nil {
			for _, p := range c.replicas[globalId].PeerInfo.Peers {
				if p != nil {
					p.ReadyToPromote = true
				}
			}
		}
	}
}

func (c *LocalPartitionServiceClient) CleanMetrics() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.DuplicateReq = false
	c.cleanMetrics()
}

func (c *LocalPartitionServiceClient) CleanEstimatedReplicas() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, rep := range c.replicas {
		rep.EstimatedReplicas = -1
	}
	for _, rep := range c.removedReplicas {
		rep.EstimatedReplicas = -1
	}
}

func (c *LocalPartitionServiceClient) KeptReplicas() map[utils.TblPartID]*pb.AddReplicaRequest {
	return c.replicas
}

func (c *LocalPartitionServiceClient) RemovedReplicas() map[utils.TblPartID]*pb.RemoveReplicaRequest {
	return c.removedReplicas
}

func (c *LocalPartitionServiceClient) LastGetReplicasReq() *pb.GetReplicasRequest {
	return c.lastGetReplicasReq
}

func (c *LocalPartitionServiceClient) ReceivedSplitReqs() map[utils.TblPartID]*pb.ReplicaSplitRequest {
	return c.splitReqs
}

func (c *LocalPartitionServiceClient) ReceivedSplitCleanupReqs() map[utils.TblPartID]*pb.ReplicaSplitCleanupRequest {
	return c.splitCleanupReqs
}

func (c *LocalPartitionServiceClient) SetServerStatistics(s map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.statistics = s
}

func (c *LocalPartitionServiceClient) SetReplicaStatistics(s map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.repStatistics = s
}

func (c *LocalPartitionServiceClient) SetReadyToPromote(flag bool) {
	c.readyToPromote = flag
}

func (c *LocalPartitionServiceClient) Reset() {
	c.cleanMetrics()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.replicas = map[utils.TblPartID]*pb.AddReplicaRequest{}
	c.removedReplicas = map[utils.TblPartID]*pb.RemoveReplicaRequest{}
	c.lastCommands = map[utils.TblPartID]*pb.CustomCommandRequest{}
	c.lastGetReplicasReq = nil
	c.statistics = map[string]string{}
	c.repStatistics = map[string]string{}
	c.readyToPromote = true
	c.DuplicateReq = false
}

func (c *LocalPartitionServiceClient) AddReplica(
	ctx context.Context,
	in *pb.AddReplicaRequest,
	opts ...grpc.CallOption,
) (*pb.ErrorStatus, error) {
	reqName := "add_replica"
	c.incMetric(reqName)
	logging.Verbose(1, "%s: add replica: %v", c.hostPort, in)
	if err := c.applyMockBehaviors(reqName); err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	globalId := utils.MakeTblPartID(in.Part.TableId, in.Part.PartitionId)
	if _, ok := c.replicas[globalId]; ok {
		c.DuplicateReq = true
		return pb.PartitionErrorCode(pb.PartitionError_kReplicaExists), nil
	}
	c.replicas[globalId] = proto.Clone(in).(*pb.AddReplicaRequest)
	delete(c.removedReplicas, globalId)
	for _, p := range c.replicas[globalId].PeerInfo.Peers {
		p.ReadyToPromote = true
	}
	return pb.ErrStatusOk(), nil
}

func (c *LocalPartitionServiceClient) Reconfigure(
	ctx context.Context,
	in *pb.ReconfigPartitionRequest,
	opts ...grpc.CallOption,
) (status *pb.ErrorStatus, err error) {
	reqName := "reconfig"
	c.incMetric(reqName)
	logging.Verbose(1, "%s: reconfig: %v", c.hostPort, in)
	if err := c.applyMockBehaviors(reqName); err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	globalId := utils.MakeTblPartID(in.TableId, in.PartitionId)
	rep := c.replicas[globalId]
	if rep == nil {
		status = pb.PartitionErrorCode(pb.PartitionError_kReplicaNotExists)
		return
	}

	if rep.PeerInfo.MembershipVersion > in.PeerInfo.MembershipVersion {
		c.DuplicateReq = true
		return
	}
	oldSplitVersion := rep.PeerInfo.SplitVersion
	rep.PeerInfo = in.PeerInfo
	if in.PeerInfo.SplitVersion > oldSplitVersion {
		for _, peer := range rep.PeerInfo.Peers {
			peer.SplitCleanupVersion = in.PeerInfo.SplitVersion - 1
		}
	}
	return pb.ErrStatusOk(), nil
}

func (c *LocalPartitionServiceClient) RemoveReplica(
	ctx context.Context,
	in *pb.RemoveReplicaRequest,
	opts ...grpc.CallOption,
) (status *pb.ErrorStatus, err error) {
	reqName := "remove_replica"
	c.incMetric(reqName)

	logging.Verbose(1, "%s: remove replica: %v", c.hostPort, in)
	if err := c.applyMockBehaviors(reqName); err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	globalId := utils.MakeTblPartID(in.TableId, in.PartitionId)
	if _, ok := c.replicas[globalId]; !ok {
		c.DuplicateReq = true
		return
	}
	delete(c.replicas, globalId)
	c.removedReplicas[globalId] = proto.Clone(in).(*pb.RemoveReplicaRequest)
	return pb.ErrStatusOk(), nil
}

func (c *LocalPartitionServiceClient) GetReplicas(
	ctx context.Context,
	in *pb.GetReplicasRequest,
	opts ...grpc.CallOption,
) (res *pb.GetReplicasResponse, err error) {
	reqName := "get_replicas"
	c.incMetric(reqName)
	logging.Verbose(1, "%s: get replica: %v", c.hostPort, in)

	if err := c.applyMockBehaviors(reqName); err != nil {
		return nil, err
	}

	c.lastGetReplicasReq = proto.Clone(in).(*pb.GetReplicasRequest)

	c.mu.Lock()
	defer c.mu.Unlock()

	res = &pb.GetReplicasResponse{}
	res.ServerResult = pb.ErrStatusOk()
	res.ServerInfo = &pb.ServerInfo{
		Node:           utils.FromHostPort(c.hostPort).ToPb(),
		StatisticsInfo: c.statistics,
	}

	for globalId, rep := range c.replicas {
		res.Results = append(res.Results, pb.ErrStatusOk())
		report := &pb.ReplicaReportInfo{
			PartitionId: globalId.Part(),
			TableId:     globalId.Table(),
			PeerInfo:    proto.Clone(rep.PeerInfo).(*pb.PartitionPeerInfo),
		}
		for _, peer := range report.PeerInfo.Peers {
			peer.ReadyToPromote = c.readyToPromote
			peer.StatisticsInfo = c.repStatistics
		}
		res.Infos = append(res.Infos, report)
	}
	return
}

func (c *LocalPartitionServiceClient) HandleCustomCommand(
	ctx context.Context,
	in *pb.CustomCommandRequest,
	opt ...grpc.CallOption,
) (resp *pb.CustomCommandResponse, err error) {
	reqName := "command"
	c.incMetric(reqName)

	logging.Verbose(1, "%s: custom command: %v", c.hostPort, in)
	if err := c.applyMockBehaviors(reqName); err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	resp = &pb.CustomCommandResponse{}
	tpid := utils.MakeTblPartID(in.TableId, in.PartitionId)
	comm := c.lastCommands[tpid]
	if comm == nil {
		c.lastCommands[tpid] = in
		resp.Status = pb.ErrStatusOk()
		resp.Progress = 0
	} else {
		if comm.CommandSessionId < in.CommandSessionId {
			c.lastCommands[tpid] = in
			resp.Status = pb.ErrStatusOk()
			resp.Progress = 0
		} else {
			resp.Status = pb.ErrStatusOk()
			resp.Progress = 100
		}
	}
	return
}

func (c *LocalPartitionServiceClient) ChangeAuthentication(
	ctx context.Context,
	in *pb.ChangeAuthenticationRequest,
	opt ...grpc.CallOption,
) (resp *pb.ErrorStatus, err error) {
	return pb.ErrStatusOk(), nil
}

func (c *LocalPartitionServiceClient) ReplicaSplit(
	ctx context.Context,
	in *pb.ReplicaSplitRequest,
	opts ...grpc.CallOption,
) (resp *pb.ErrorStatus, err error) {
	reqName := "replica_split"
	c.incMetric(reqName)

	c.splitReqs[utils.MakeTblPartID(in.TableId, in.PartitionId)] = proto.Clone(in).(*pb.ReplicaSplitRequest)

	if err := c.applyMockBehaviors(reqName); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	childTpid := utils.MakeTblPartID(in.TableId, in.PartitionId+in.NewTableInfo.PartitionNum/2)
	childRep, ok := c.replicas[childTpid]
	if ok {
		logging.Assert(childRep.PeerInfo.SplitVersion >= in.ChildPeers.SplitVersion, "")
		peer := childRep.PeerInfo.FindPeer(c.getRpcNode())
		logging.Assert(peer != nil, "")
		return pb.PartitionErrorMsg(
			pb.PartitionError_kReplicaExists,
			"partition %s already split",
			childTpid.String(),
		), nil
	}

	tpid := utils.MakeTblPartID(in.TableId, in.PartitionId)
	rep, ok := c.replicas[tpid]
	if !ok {
		return pb.PartitionErrorMsg(
			pb.PartitionError_kReplicaNotExists,
			"replica %s not exist",
			tpid.String(),
		), nil
	}
	if rep.PeerInfo.MembershipVersion != in.Peers.MembershipVersion {
		return pb.PartitionErrorMsg(
			pb.PartitionError_kUnknown,
			"membership version not match: local %d vs given %d",
			rep.PeerInfo.MembershipVersion,
			in.Peers.MembershipVersion,
		), nil
	}
	if rep.PeerInfo.SplitVersion != in.ChildPeers.SplitVersion {
		return pb.PartitionErrorMsg(
			pb.PartitionError_kUnknown,
			"split version not match: local %d vs given %d",
			rep.PeerInfo.SplitVersion,
			in.ChildPeers.SplitVersion,
		), nil
	}

	childRep = proto.Clone(rep).(*pb.AddReplicaRequest)
	childRep.Part.PartitionId = in.PartitionId + in.NewTableInfo.PartitionNum/2
	childRep.Part.PartitionNum = in.NewTableInfo.PartitionNum

	childRep.ParentInfo = proto.Clone(rep.PeerInfo).(*pb.PartitionPeerInfo)
	childRep.PeerInfo = proto.Clone(in.ChildPeers).(*pb.PartitionPeerInfo)
	for _, peer := range childRep.PeerInfo.Peers {
		peer.ReadyToPromote = true
		peer.StatisticsInfo = c.repStatistics
	}

	c.replicas[childTpid] = childRep
	return pb.PartitionErrorMsg(pb.PartitionError_kOK, ""), nil
}

func (c *LocalPartitionServiceClient) ReplicaSplitCleanup(
	ctx context.Context,
	in *pb.ReplicaSplitCleanupRequest,
	opts ...grpc.CallOption,
) (resp *pb.ErrorStatus, err error) {
	reqName := "replica_split_cleanup"
	c.incMetric(reqName)

	c.splitCleanupReqs[utils.MakeTblPartID(in.TableId, in.PartitionId)] = proto.Clone(in).(*pb.ReplicaSplitCleanupRequest)

	if err := c.applyMockBehaviors(reqName); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tpid := utils.MakeTblPartID(in.TableId, in.PartitionId)
	rep, ok := c.replicas[tpid]
	if !ok {
		return pb.PartitionErrorMsg(pb.PartitionError_kReplicaNotExists, ""), nil
	}
	if rep.PeerInfo.SplitVersion > in.PartitionSplitVersion {
		return pb.PartitionErrorMsg(pb.PartitionError_kUnknown, ""), nil
	}
	for _, peer := range rep.PeerInfo.Peers {
		peer.SplitCleanupVersion = in.PartitionSplitVersion
	}
	return pb.PartitionErrorMsg(pb.PartitionError_kOK, ""), nil
}

func (c *LocalPartitionServiceClient) PrepareSwitchPrimary(
	ctx context.Context,
	in *pb.PrepareSwitchPrimaryRequest,
	opt ...grpc.CallOption,
) (resp *pb.ErrorStatus, err error) {
	return pb.ErrStatusOk(), nil
}

func (c *LocalPartitionServiceClient) GetReplicateInfo(
	ctx context.Context,
	in *pb.ReplicateInfoRequest,
	opt ...grpc.CallOption,
) (resp *pb.ReplicateInfoResponse, err error) {
	resp = &pb.ReplicateInfoResponse{}
	return
}
