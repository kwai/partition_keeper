package sd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
)

type DiscoveryMockServer struct {
	httpServer *http.Server
	mu         sync.Mutex
	resp       kessResponse
}

func NewDiscoveryMockServer() *DiscoveryMockServer {
	ans := &DiscoveryMockServer{}
	ans.resp.Result.Original = make(map[string]*nodeDescription)
	return ans
}

func (m *DiscoveryMockServer) readRouteTable(w http.ResponseWriter, r *http.Request) {
	logging.Info("got route request")
	m.mu.Lock()
	defer m.mu.Unlock()
	json.NewEncoder(w).Encode(&(m.resp))
}

func (m *DiscoveryMockServer) Start(port int) {
	addr := fmt.Sprintf(":%v", port)
	m.httpServer = &http.Server{
		Addr:    addr,
		Handler: http.HandlerFunc(m.readRouteTable),
	}
	logging.Info("start at port %d", port)
	go m.httpServer.ListenAndServe()
}

func (m *DiscoveryMockServer) Stop() {
	m.httpServer.Close()
}

func (m *DiscoveryMockServer) getAddrKey(ipPort string) string {
	return "grpc:" + ipPort
}

func (m *DiscoveryMockServer) CleanAllEntries() {
	m.resp.Result.Original = make(map[string]*nodeDescription)
}

func (m *DiscoveryMockServer) UpdateServiceNode(node *pb.ServiceNode) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.resp.Result.Original[node.Id] = newNodeDescription(node)
}

func (m *DiscoveryMockServer) UpdatePayload(ipPort string, updater func(old string) string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	node := m.resp.Result.Original[m.getAddrKey(ipPort)]

	node.Payload = updater(node.Payload)
}

func (m *DiscoveryMockServer) UpdateLocation(ipPort string, loc string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	node := m.resp.Result.Original[m.getAddrKey(ipPort)]
	node.Location = loc
}

func (m *DiscoveryMockServer) UpdateKwsInfo(ipPort string, kws *pb.KwsInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	node := m.resp.Result.Original[m.getAddrKey(ipPort)]
	node.Kws.Region = kws.Region
	node.Kws.Az = kws.Az
	node.Kws.Dc = kws.Dc
}

func (m *DiscoveryMockServer) RemoveNode(hostPort string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.getAddrKey(hostPort)
	if _, ok := m.resp.Result.Original[key]; ok {
		delete(m.resp.Result.Original, key)
		return true
	} else {
		return false
	}
}

func (m *DiscoveryMockServer) CleanRpcEntries(hostPort string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.resp.Result.Original[m.getAddrKey(hostPort)].Entries = nil
}
