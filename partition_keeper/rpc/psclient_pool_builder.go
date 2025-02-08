package rpc

import (
	"sync"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"

	"google.golang.org/grpc"
)

type GrpcPSClientPoolBuilder struct {
}

func (b *GrpcPSClientPoolBuilder) Build() *ConnPool {
	return NewRpcPool(func(conn *grpc.ClientConn) interface{} {
		return pb.NewPartitionServiceClient(conn)
	})
}

type RpcMockBehaviors struct {
	Delay   time.Duration
	RpcFail bool
}

type LocalPSClientPoolBuilder struct {
	initialTargets []string
	mockBehaviors  RpcMockBehaviors
	mu             sync.Mutex
	LocalClients   map[string]*LocalPartitionServiceClient
}

func (l *LocalPSClientPoolBuilder) WithBehaviors(b *RpcMockBehaviors) *LocalPSClientPoolBuilder {
	l.mockBehaviors = *b
	return l
}

func (l *LocalPSClientPoolBuilder) WithInitialTargets(targets []string) *LocalPSClientPoolBuilder {
	l.initialTargets = targets
	for _, t := range targets {
		l.LocalClients[t] = NewLocalPSClient(t, &l.mockBehaviors)
	}
	return l
}

func NewLocalPSClientPoolBuilder() *LocalPSClientPoolBuilder {
	ans := &LocalPSClientPoolBuilder{
		LocalClients: make(map[string]*LocalPartitionServiceClient),
	}
	return ans
}

func (lb *LocalPSClientPoolBuilder) Build() *ConnPool {
	return NewRpcPool(func(conn *grpc.ClientConn) interface{} {
		return lb.GetOrNewClient(conn.Target())
	})
}

func (lb *LocalPSClientPoolBuilder) GetOrNewClient(target string) *LocalPartitionServiceClient {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if res, ok := lb.LocalClients[target]; ok {
		return res
	} else {
		res = NewLocalPSClient(target, &lb.mockBehaviors)
		lb.LocalClients[target] = res
		return res
	}
}

func (lb *LocalPSClientPoolBuilder) GetClient(target string) *LocalPartitionServiceClient {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.LocalClients[target]
}
