package rpc

import (
	"sync"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"

	"google.golang.org/grpc"
)

type GrpcSDClientPoolBuilder struct {
}

func (b *GrpcSDClientPoolBuilder) Build() *ConnPool {
	return NewRpcPool(func(conn *grpc.ClientConn) interface{} {
		return pb.NewUniversalDiscoveryClient(conn)
	})
}

type LocalSDClientPoolBuilder struct {
	mockBehaviors RpcMockBehaviors
	mu            sync.Mutex
	LocalClients  map[string]*LocalSdClient
}

func (l *LocalSDClientPoolBuilder) WithBehaviors(b *RpcMockBehaviors) *LocalSDClientPoolBuilder {
	l.mockBehaviors = *b
	return l
}

func NewLocalSDClientPoolBuilder() *LocalSDClientPoolBuilder {
	ans := &LocalSDClientPoolBuilder{
		LocalClients: make(map[string]*LocalSdClient),
	}
	return ans
}

func (lb *LocalSDClientPoolBuilder) Build() *ConnPool {
	return NewRpcPool(func(conn *grpc.ClientConn) interface{} {
		return lb.GetClient(conn.Target())
	})
}

func (lb *LocalSDClientPoolBuilder) GetClient(target string) *LocalSdClient {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if res, ok := lb.LocalClients[target]; ok {
		return res
	} else {
		res = NewLocalSdClient(target, &lb.mockBehaviors)
		lb.LocalClients[target] = res
		return res
	}
}
