package rodis_store

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/go-zookeeper/zk"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

const (
	kClusterEnable  = "ENABLE"
	kClusterDisable = "DISABLE"
	kDomainsZNode   = "domains"
	kNodesZNode     = "nodes"
	kFramesZNode    = "frames"
)

type clusterKey struct {
	clusterPrefix string
	zkHosts       string
}

type clusterPool struct {
	mu       sync.RWMutex
	clusters map[clusterKey]*clusterResource
}

func (c *clusterPool) findClusterResource(key *clusterKey) *clusterResource {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clusters[*key]
}

func (c *clusterPool) newClusterResource(key *clusterKey, zkHosts []string) *clusterResource {
	c.mu.Lock()
	defer c.mu.Unlock()

	if res := c.clusters[*key]; res != nil {
		return res
	}
	res := newClusterResource(key.clusterPrefix, zkHosts)
	c.clusters[*key] = res
	return res
}

func (c *clusterPool) getClusterResource(clusterPrefix string, zkHosts []string) *clusterResource {
	key := clusterKey{
		clusterPrefix: clusterPrefix,
		zkHosts:       utils.StringsKey(zkHosts),
	}
	if res := c.findClusterResource(&key); res != nil {
		return res
	}
	ans := c.newClusterResource(&key, zkHosts)
	return ans
}

var pool *clusterPool

func init() {
	pool = &clusterPool{
		clusters: make(map[clusterKey]*clusterResource),
	}
}

type clusterResource struct {
	mu sync.Mutex

	initializeOnce sync.Once
	clusterPrefix  string
	store          *zkStore

	domainsDir string
	framesDir  string
	nodesDir   string

	nodes map[string]*rodisNodeMeta
	frame *rodisFrameMeta
}

func newClusterResource(clusterPrefix string, zkHosts []string) *clusterResource {
	output := &clusterResource{
		clusterPrefix: clusterPrefix,
		store: metastore.GetGlobalPool().
			Get(zkHosts, zk.WorldACL(zk.PermAll), "", []byte("")),
	}
	return output
}

func (r *clusterResource) initialize() {
	r.initializeOnce.Do(func() {
		r.prepareZkPath()
		r.initializeNodes()
		r.initializeFrame()
	})
}

func (r *clusterResource) prepareZkPath() {
	succ := r.store.RecursiveCreate(context.Background(), r.clusterPrefix)
	logging.Assert(succ, "")

	// mark cluster disabled by default, so any mistakenly started schedulers will
	// be prevented from doing scheduling
	succ = r.store.Set(context.Background(), r.clusterPrefix, []byte(kClusterDisable))
	logging.Assert(succ, "")

	r.domainsDir = r.clusterPrefix + "/" + kDomainsZNode
	succ = r.store.Create(context.Background(), r.domainsDir, []byte{})
	logging.Assert(succ, "")

	r.framesDir = r.clusterPrefix + "/" + kFramesZNode
	succ = r.store.Create(context.Background(), r.framesDir, []byte{})
	logging.Assert(succ, "")

	r.nodesDir = r.clusterPrefix + "/" + kNodesZNode
	succ = r.store.Create(context.Background(), r.nodesDir, []byte{})
	logging.Assert(succ, "")
}

func (r *clusterResource) initializeNodes() {
	r.nodes = make(map[string]*rodisNodeMeta)
	children, exist, succ := r.store.Children(context.Background(), r.nodesDir)
	logging.Assert(exist && succ, "")

	for _, child := range children {
		host := strings.SplitN(child, ":", 2)[0]
		rms, err := third_party.QueryHostInfo(host)
		logging.Assert(err == nil, "query rms for host %s failed: %v", host, err)
		if rms == nil {
			logging.Error("%s can't find rms info for %s", r.clusterPrefix, host)
		} else {
			nodeMeta := newRodisNodeMeta(r.nodesDir, child)
			r.nodes[child] = nodeMeta
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(len(r.nodes))
	for _, meta := range r.nodes {
		go func(m *rodisNodeMeta) {
			defer wg.Done()
			m.loadFromZk(r.store)
		}(meta)
	}
	wg.Wait()
}

func (r *clusterResource) initializeFrame() {
	r.frame = newRodisFrameMeta(r.store, r.framesDir)
	for hp, node := range r.nodes {
		host := strings.SplitN(hp, ":", 2)[0]
		rms, err := third_party.QueryHostInfo(host)
		logging.Assert(err == nil, "query rms for host %s failed: %v", host, err)
		logging.Assert(
			rms != nil,
			"%s: no rms info node %s should be stripped in intializeNodes",
			r.clusterPrefix,
			host,
		)
		r.frame.initializeAz2Idc(r.store, node.frame, rms.Az, node.idc)
	}
}

func (r *clusterResource) updateNodes(entries *pb.RouteEntries) {
	activeNodes := map[string]bool{}

	wg := sync.WaitGroup{}
	for _, server := range entries.Servers {
		hp := server.ToHostPort()
		activeNodes[hp] = true
		nodeMeta, ok := r.nodes[hp]
		if !ok {
			nodeMeta = newRodisNodeMeta(r.nodesDir, hp)
			r.nodes[hp] = nodeMeta
		}
		wg.Add(1)
		go func(n *rodisNodeMeta, sv *pb.ServerLocation) {
			defer wg.Done()
			hub := entries.ReplicaHubs[sv.HubId]
			n.update(r.store, sv, hub, r.frame.getIdc(hub.Name))
		}(nodeMeta, server)
	}

	var toRemove []string
	for hp, nodeMeta := range r.nodes {
		if _, ok := activeNodes[hp]; !ok {
			toRemove = append(toRemove, hp)
			wg.Add(1)
			go func(n *rodisNodeMeta) {
				defer wg.Done()
				n.remove(r.store)
			}(nodeMeta)
		}
	}
	wg.Wait()

	for _, hp := range toRemove {
		delete(r.nodes, hp)
	}
}

func (r *clusterResource) updateFrame(entries *pb.RouteEntries) {
	r.frame.update(r.store, entries.ReplicaHubs)
}

func (r *clusterResource) update(entries *pb.RouteEntries) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.updateFrame(entries)
	r.updateNodes(entries)
}

func (r *clusterResource) getSortedServers(hubs []*pb.ReplicaHub) ([]*pb.ServerLocation, error) {
	getHubIndex := func(name string) int {
		for i, hub := range hubs {
			if hub.Name == name {
				return i
			}
		}
		return -1
	}

	output := []*pb.ServerLocation{}

	for hp, node := range r.nodes {
		rpcNode := utils.FromHostPort(hp)
		sl := &pb.ServerLocation{
			Host:  rpcNode.NodeName,
			Port:  rpcNode.Port,
			HubId: int32(getHubIndex(node.frame)),
		}
		node.status.translateTo(sl)
		if sl.HubId == -1 {
			logging.Error("%s can't find frame for %s, skip it", r.clusterPrefix, hp)
			return nil, ErrDataCorrupted
		}
		output = append(output, sl)
	}

	sort.Slice(output, func(i, j int) bool {
		l, r := output[i], output[j]
		if l.Host != r.Host {
			return l.Host < r.Host
		}
		return l.Port < r.Port
	})

	return output, nil
}

func (r *clusterResource) getSortedServersHubs(entries *pb.RouteEntries) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error = nil
	entries.ReplicaHubs = r.frame.getSortedFrames()
	entries.Servers, err = r.getSortedServers(entries.ReplicaHubs)
	return err
}
