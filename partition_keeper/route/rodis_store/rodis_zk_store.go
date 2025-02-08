package rodis_store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

const (
	kShardsZNode = "shards"
)

var (
	ErrDataCorrupted = errors.New("rodis_store: data corrupted")
)

type rodisZkStore struct {
	initializeOnce sync.Once
	cluster        *clusterResource
	domainName     string
	shardsDir      string

	domain *rodisDomainMeta
	shards []*rodisShardMeta
}

func (r *rodisZkStore) prepareZkPath() {
	r.shardsDir = fmt.Sprintf("%s/%s/%s", r.cluster.clusterPrefix, kShardsZNode, r.domainName)
	succ := r.cluster.store.RecursiveCreate(context.Background(), r.shardsDir)
	logging.Assert(succ, "")
}

func (r *rodisZkStore) initializeDomain() {
	r.domain = newDomainMeta(r.cluster.store, r.cluster.domainsDir, r.domainName)
}

func (r *rodisZkStore) initializeShards() {
	children, exist, succ := r.cluster.store.Children(context.Background(), r.shardsDir)
	logging.Assert(exist && succ, "")

	if r.domain.numShards <= 0 {
		logging.Info("%s: empty domain, skip to load shards", r.shardsDir)
		return
	}

	r.shards = make([]*rodisShardMeta, r.domain.numShards)
	shards := []int{}
	for _, child := range children {
		shardId, err := strconv.Atoi(child)
		if err != nil {
			logging.Warning("%s: parse %s to shard id failed: %s", r.shardsDir, child, err.Error())
		} else if shardId < 0 {
			logging.Warning("%s: invalid shard id %s", r.shardsDir, child)
		} else if shardId >= r.domain.numShards {
			logging.Error("%s: shard id %d exceeds max shards %d of this domain", r.shardsDir, shardId, r.domain.numShards)
		} else {
			shards = append(shards, shardId)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(len(shards))

	for _, sid := range shards {
		shardMeta := newRodisShardMeta(r.shardsDir, sid)
		r.shards[sid] = shardMeta
		go func() {
			defer wg.Done()
			shardMeta.loadFromZk(r.cluster.store)
		}()
	}
	wg.Wait()
}

func (r *rodisZkStore) updateShards(store *zkStore, entries *pb.RouteEntries) {
	logging.Assert(
		len(entries.Partitions) >= len(r.shards),
		"don't support shards count decreased",
	)
	for len(entries.Partitions) > len(r.shards) {
		shards := make([]*rodisShardMeta, len(entries.Partitions))
		copy(shards, r.shards)
		r.shards = shards
	}

	wg := sync.WaitGroup{}
	wg.Add(len(entries.Partitions))

	for i, s := range r.shards {
		shard := s
		if shard == nil {
			shard = newRodisShardMeta(r.shardsDir, i)
			r.shards[i] = shard
		}
		go func(partId int) {
			defer wg.Done()
			shard.update(store, entries, partId)
		}(i)
	}
	wg.Wait()
}

func (r *rodisZkStore) initialize() {
	r.cluster.initialize()
	r.prepareZkPath()
	r.initializeDomain()
	r.initializeShards()
}

func (r *rodisZkStore) update(entries *pb.RouteEntries) {
	r.cluster.update(entries)
	r.domain.update(r.cluster.store, entries)
	r.updateShards(r.cluster.store, entries)
}

// TODO: dump isn't concurrently safe with update.
// currently it't not necessary as dump is only used for
//  1. unit-test
//  2. debugging
//  3. migration rodis route message to partition keeper
func (r *rodisZkStore) dump() (*pb.RouteEntries, error) {
	output := &pb.RouteEntries{}
	output.TableInfo = &pb.TableInfo{
		HashMethod: "crc32",
	}
	if err := r.cluster.getSortedServersHubs(output); err != nil {
		return nil, err
	}
	if r.domain.numReplicas != len(output.ReplicaHubs) {
		logging.Error(
			"%s: domain %s replica count %d not match with frame count: %d",
			r.cluster.clusterPrefix,
			r.domainName,
			r.domain.numReplicas,
			len(output.ReplicaHubs),
		)
		return nil, ErrDataCorrupted
	}

	getServerIndex := func(hp string) int {
		node := utils.FromHostPort(hp)
		for i, s := range output.Servers {
			if s.Host == node.NodeName && s.Port == node.Port {
				return i
			}
		}
		return -1
	}

	for _, shard := range r.shards {
		partition := &pb.PartitionLocation{Version: shard.version}
		for hp, state := range shard.replicas {
			index := getServerIndex(hp)
			if index == -1 {
				logging.Error("%s can't find %s in nodes, skip it", shard.zkPath, hp)
				return nil, ErrDataCorrupted
			}
			rep := &pb.ReplicaLocation{
				ServerIndex: int32(index),
				Role:        pb.ReplicaRole_kInvalid,
			}
			switch state {
			case rodisReplicaServing:
				if hp == shard.master {
					rep.Role = pb.ReplicaRole_kPrimary
				} else {
					rep.Role = pb.ReplicaRole_kSecondary
				}
			case rodisReplicaPreparing, rodisReplicaMiss:
				rep.Role = pb.ReplicaRole_kLearner
			}
			partition.Replicas = append(partition.Replicas, rep)
		}
		output.Partitions = append(output.Partitions, partition)
	}

	return output, nil
}

func (r *rodisZkStore) Put(entries *pb.RouteEntries) error {
	r.initializeOnce.Do(func() {
		r.initialize()
	})
	r.update(entries)
	return nil
}

func (r *rodisZkStore) Del() error {
	r.initializeOnce.Do(func() {
		r.initialize()
	})
	domainsDir := fmt.Sprintf("%s/%s", r.cluster.domainsDir, r.domainName)
	logging.Assert(r.cluster.store.RecursiveDelete(context.Background(), domainsDir),
		"clean rodis domain path failed, domain name : %s",
		r.domainName,
	)
	logging.Assert(r.cluster.store.RecursiveDelete(context.Background(), r.shardsDir),
		"clean rodis shards path failed, domain name : %s",
		r.domainName,
	)
	return nil
}

func (r *rodisZkStore) Get() (*pb.RouteEntries, error) {
	r.initializeOnce.Do(func() {
		r.initialize()
	})
	return r.dump()
}

const MediaType = "zk"

func NewRodisZkStore(args *base.StoreBaseOption) *rodisZkStore {
	zkHosts := strings.Split(args.Url, ",")
	clusterPrefix := args.Args
	ans := &rodisZkStore{
		cluster:    pool.getClusterResource(clusterPrefix, zkHosts),
		domainName: args.Table,
	}
	return ans
}

func init() {
	base.RegisterStoreCreator(MediaType, func(args *base.StoreBaseOption) base.StoreBase {
		return NewRodisZkStore(args)
	})
}
