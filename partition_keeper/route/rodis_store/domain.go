package rodis_store

import (
	"context"
	"fmt"
	"strconv"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
)

const (
	kConsistencyZNode = "consistency"
	kMapMethodZNode   = "map_methed"
	kMasterIDCZNode   = "master_idc"
	kNumReplicasZNode = "num_replicas"
	kNumShardsZNode   = "num_shards"
)

type rodisConsistencyStrategy string

const (
	rodisEventualConsistency rodisConsistencyStrategy = "EVENTUAL"
	rodisStrongConsistency   rodisConsistencyStrategy = "STRONG"
)

type rodisMapMethod string

const (
	rodisHashMapping rodisMapMethod = "hash"
)

type rodisDomainState string

const (
	rodisDomainEnable  rodisDomainState = "DOMAIN_ENABLE"
	rodisDomainDisable rodisDomainState = "DOMAIN_DISABLE"
)

type rodisDomainMeta struct {
	zkPath      string
	hasZkEntry  bool
	domainState rodisDomainState
	consistency rodisConsistencyStrategy
	mapMethod   rodisMapMethod
	masterIdc   string
	numShards   int
	numReplicas int
}

func newDomainMeta(store *zkStore, parentPath, domainName string) *rodisDomainMeta {
	domain := &rodisDomainMeta{}
	domain.zkPath = fmt.Sprintf("%s/%s", parentPath, domainName)
	domain.initializeZkNodes(store)
	return domain
}

func (r *rodisDomainMeta) subpath(node string) string {
	return r.zkPath + "/" + node
}

func (r *rodisDomainMeta) initializeZkNodes(store *zkStore) {
	data, exists, succ := store.Get(context.Background(), r.zkPath)
	logging.Assert(succ, "")
	if !exists {
		r.hasZkEntry = false
		r.domainState = rodisDomainEnable
		r.consistency = rodisEventualConsistency
		r.mapMethod = rodisHashMapping
		// TODO: set master idc
		r.masterIdc = ""
		r.numShards = 0
		r.numReplicas = 0
		return
	}
	// TODO: check subpaths are partially created
	r.hasZkEntry = true
	r.domainState = rodisDomainState(string(data))

	data, exists, succ = store.Get(context.Background(), r.subpath(kConsistencyZNode))
	logging.Assert(succ && exists, "")
	r.consistency = rodisConsistencyStrategy(string(data))

	data, exists, succ = store.Get(context.Background(), r.subpath(kMapMethodZNode))
	logging.Assert(succ && exists, "")
	r.mapMethod = rodisMapMethod(string(data))

	data, exists, succ = store.Get(context.Background(), r.subpath(kMasterIDCZNode))
	logging.Assert(succ && exists, "")
	r.masterIdc = string(data)

	data, exists, succ = store.Get(context.Background(), r.subpath(kNumReplicasZNode))
	logging.Assert(succ && exists, "")
	replicas, err := strconv.Atoi(string(data))
	logging.Assert(err == nil, "")
	r.numReplicas = replicas

	data, exists, succ = store.Get(context.Background(), r.subpath(kNumShardsZNode))
	logging.Assert(succ && exists, "")
	shards, err := strconv.Atoi(string(data))
	logging.Assert(err == nil, "")
	r.numShards = shards
}

func (r *rodisDomainMeta) updateZk(store *zkStore) {
	path := []string{
		r.zkPath,
		r.subpath(kConsistencyZNode),
		r.subpath(kMapMethodZNode),
		r.subpath(kMasterIDCZNode),
		r.subpath(kNumReplicasZNode),
		r.subpath(kNumShardsZNode),
	}

	replicas := fmt.Sprintf("%d", r.numReplicas)
	shards := fmt.Sprintf("%d", r.numShards)
	data := [][]byte{
		[]byte(rodisDomainEnable),
		[]byte(r.consistency),
		[]byte(r.mapMethod),
		[]byte(r.masterIdc),
		[]byte(replicas),
		[]byte(shards),
	}

	succ := false
	if r.hasZkEntry {
		succ = store.MultiSet(context.Background(), path, data)
	} else {
		succ = store.MultiCreate(context.Background(), path, data)
		r.hasZkEntry = true
	}
	logging.Assert(succ, "")
}

func (r *rodisDomainMeta) update(store *zkStore, entries *pb.RouteEntries) {
	needUpdate := false
	if r.numReplicas != len(entries.ReplicaHubs) {
		needUpdate = true
		r.numReplicas = len(entries.ReplicaHubs)
	}
	if r.numShards != len(entries.Partitions) {
		needUpdate = true
		r.numShards = len(entries.Partitions)
	}
	if needUpdate {
		r.updateZk(store)
	}
}
