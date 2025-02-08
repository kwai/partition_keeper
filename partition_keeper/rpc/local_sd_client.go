package rpc

import (
	"context"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type LocalSdClient struct {
	*mockedRpcClient
	response pb.GetNodesResponse
	request  *pb.GetNodesRequest
}

func NewLocalSdClient(hostPort string, behaviors *RpcMockBehaviors) *LocalSdClient {
	return &LocalSdClient{
		mockedRpcClient: newMockedRpcClient(behaviors, hostPort),
	}
}

func (l *LocalSdClient) CleanMetrics() {
	l.cleanMetrics()
}

func (l *LocalSdClient) ReceivedRequest() *pb.GetNodesRequest {
	return l.request
}

func (l *LocalSdClient) SetResponse(r *pb.GetNodesResponse) {
	proto.Merge(&(l.response), r)
}

func (l *LocalSdClient) GetNodes(
	ctx context.Context,
	in *pb.GetNodesRequest,
	opts ...grpc.CallOption,
) (*pb.GetNodesResponse, error) {
	reqName := "get_nodes"
	l.incMetric(reqName)
	l.request = proto.Clone(in).(*pb.GetNodesRequest)
	if err := l.applyMockBehaviors(reqName); err != nil {
		return nil, err
	}
	return proto.Clone(&l.response).(*pb.GetNodesResponse), nil
}
