package rpc

import (
	"errors"
	"sync"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type mockedRpcClient struct {
	sharedBehaviors *RpcMockBehaviors
	reqBehaviors    map[string]*RpcMockBehaviors
	hostPort        string
	metricMu        sync.Mutex
	metrics         map[string]int
}

func newMockedRpcClient(b *RpcMockBehaviors, hp string) *mockedRpcClient {
	return &mockedRpcClient{
		sharedBehaviors: b,
		reqBehaviors:    make(map[string]*RpcMockBehaviors),
		hostPort:        hp,
		metrics:         make(map[string]int),
	}
}

func (m *mockedRpcClient) getRpcNode() *pb.RpcNode {
	return utils.FromHostPort(m.hostPort).ToPb()
}

func (m *mockedRpcClient) getRpcBehaviors(reqName string) *RpcMockBehaviors {
	if b, ok := m.reqBehaviors[reqName]; ok {
		return b
	}
	return m.sharedBehaviors
}

func (m *mockedRpcClient) applyMockBehaviors(reqName string) error {
	b := m.getRpcBehaviors(reqName)
	if b == nil {
		return nil
	}
	if b.Delay > 0 {
		time.Sleep(m.sharedBehaviors.Delay)
	}
	if b.RpcFail {
		return errors.New("mock: rpc failed")
	}
	return nil
}

func (m *mockedRpcClient) incMetric(val string) {
	m.metricMu.Lock()
	defer m.metricMu.Unlock()

	m.metrics[val]++
}

func (m *mockedRpcClient) GetMetric(val string) int {
	m.metricMu.Lock()
	defer m.metricMu.Unlock()

	return m.metrics[val]
}

func (c *mockedRpcClient) SetSharedBehaviors(behaviors *RpcMockBehaviors) {
	c.sharedBehaviors = behaviors
}

func (c *mockedRpcClient) SetReqBehaviors(name string, behaviors *RpcMockBehaviors) {
	c.reqBehaviors[name] = behaviors
}

func (m *mockedRpcClient) cleanMetrics() {
	m.metricMu.Lock()
	defer m.metricMu.Unlock()

	m.metrics = map[string]int{}
}
