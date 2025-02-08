package rodis_store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"

	"github.com/go-zookeeper/zk"
)

const (
	kMasterZNode    = "master"
	kMasterSeqZNode = "master_seq_num"
	kReplicasZNode  = "replicas"
)

type rodisReplicaStatus string

const (
	rodisReplicaServing   rodisReplicaStatus = "SERVING"
	rodisReplicaPreparing rodisReplicaStatus = "PREPARING"
	rodisReplicaMiss      rodisReplicaStatus = "MISS"
)

type rodisShardMeta struct {
	zkPath       string
	hasZkEntry   bool
	version      int64
	master       string
	masterSeqNum int64
	replicas     map[string]rodisReplicaStatus
}

func newRodisShardMeta(parentPath string, partId int) *rodisShardMeta {
	output := &rodisShardMeta{
		zkPath: fmt.Sprintf(
			"%s/%d",
			parentPath,
			partId,
		),
		hasZkEntry:   false,
		version:      0,
		master:       "",
		masterSeqNum: 0,
		replicas:     make(map[string]rodisReplicaStatus),
	}
	return output
}

func (r *rodisShardMeta) loadFromZk(store *zkStore) {
	r.hasZkEntry = true

	data, exists, succ := store.Get(context.Background(), r.zkPath)
	logging.Assert(exists && succ, "")
	version, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		logging.Info(
			"%s can't parse version from data %s, set version to 1",
			r.zkPath,
			string(data),
		)
		version = 1
	}
	r.version = version

	data, exists, succ = store.Get(context.Background(), r.subpath(kMasterZNode))
	logging.Assert(succ, "")
	if exists {
		r.master = string(data)
	} else {
		succ = store.Create(context.Background(), r.subpath(kMasterZNode), []byte{})
		logging.Assert(succ, "")
	}

	data, exists, succ = store.Get(context.Background(), r.subpath(kMasterSeqZNode))
	logging.Assert(succ, "")
	if exists {
		seq, err := strconv.ParseInt(string(data), 10, 64)
		logging.Assert(err == nil, "")
		r.masterSeqNum = seq
	} else {
		succ = store.Create(context.Background(), r.subpath(kMasterSeqZNode), []byte("0"))
		logging.Assert(succ, "")
	}

	children, exists, succ := store.Children(context.Background(), r.subpath(kReplicasZNode))
	logging.Assert(exists && succ, "")
	for _, child := range children {
		data, exists, succ := store.Get(context.Background(), r.subpath(kReplicasZNode, child))
		logging.Assert(exists && succ, "")
		r.replicas[child] = rodisReplicaStatus(string(data))
	}
}

func (r *rodisShardMeta) putOp(path string, data []byte) interface{} {
	if r.hasZkEntry {
		return &zk.SetDataRequest{
			Path:    path,
			Data:    data,
			Version: -1,
		}
	} else {
		return &zk.CreateRequest{
			Path:  path,
			Data:  data,
			Acl:   zk.WorldACL(zk.PermAll),
			Flags: 0,
		}
	}
}

func (r *rodisShardMeta) updateMasterOp(master string, masterSeq int64) []interface{} {
	output := []interface{}{}
	if r.master != master {
		output = append(output, &zk.SetDataRequest{
			Path:    r.subpath(kMasterZNode),
			Data:    []byte(master),
			Version: -1,
		})
		r.master = master
	}
	if r.masterSeqNum != masterSeq {
		output = append(output, &zk.SetDataRequest{
			Path:    r.subpath(kMasterSeqZNode),
			Data:    []byte(fmt.Sprintf("%d", masterSeq)),
			Version: -1,
		})
		r.masterSeqNum = masterSeq
	}
	return output
}

func (r *rodisShardMeta) cleanMaster() interface{} {
	if r.master != "" {
		logging.Assert(r.hasZkEntry, "")
		r.master = ""
		return &zk.SetDataRequest{
			Path:    r.subpath(kMasterZNode),
			Data:    []byte(""),
			Version: -1,
		}
	}
	return nil
}

func (r *rodisShardMeta) updateReplicaOp(
	rep string,
	role pb.ReplicaRole,
	newReplicaSet map[string]rodisReplicaStatus,
) interface{} {
	newStatus := rodisReplicaServing
	if role == pb.ReplicaRole_kLearner {
		newStatus = rodisReplicaPreparing
	}
	if oldStatus, ok := r.replicas[rep]; !ok {
		newReplicaSet[rep] = newStatus
		return &zk.CreateRequest{
			Path:  r.subpath(kReplicasZNode, rep),
			Data:  []byte(newStatus),
			Acl:   zk.WorldACL(zk.PermAll),
			Flags: 0,
		}
	} else if oldStatus != newStatus {
		newReplicaSet[rep] = newStatus
		return &zk.SetDataRequest{
			Path:    r.subpath(kReplicasZNode, rep),
			Data:    []byte(newStatus),
			Version: -1,
		}
	} else {
		newReplicaSet[rep] = oldStatus
		return nil
	}
}

func (r *rodisShardMeta) updateMasterSeq(
	store *zkStore,
	servers []*pb.ServerLocation,
	part *pb.PartitionLocation,
) {
	if !r.hasZkEntry {
		logging.Info("%s: skip to update master seq as shard hasn't created on zk", r.zkPath)
		return
	}

	var master *pb.ReplicaLocation = nil
	for _, replica := range part.Replicas {
		if replica.Role == pb.ReplicaRole_kPrimary {
			master = replica
			break
		}
	}
	if master == nil {
		logging.Verbose(1, "%s: skip to update master seq as no master", r.zkPath)
		return
	}
	seq := GetSequenceNumber(master.Info)
	if seq == -1 {
		hp := servers[master.ServerIndex].ToHostPort()
		logging.Warning(
			"%s: skip to update master seq as can't find seqid from master %s",
			r.zkPath,
			hp,
		)
		return
	}
	if r.masterSeqNum == seq {
		logging.Verbose(1, "%s: skip to update master seq as it unchanged with %d", r.zkPath, seq)
		return
	}
	r.masterSeqNum = seq
	ans := store.Set(
		context.Background(),
		r.subpath(kMasterSeqZNode),
		[]byte(fmt.Sprintf("%d", r.masterSeqNum)),
	)
	logging.Assert(ans, "")
}

func (r *rodisShardMeta) updateWholeShard(
	store *zkStore,
	entries *pb.RouteEntries,
	part *pb.PartitionLocation,
) {
	ops := []interface{}{}
	ops = append(ops, r.putOp(r.zkPath, []byte(fmt.Sprintf("%d", part.Version))))
	if !r.hasZkEntry {
		ops = append(ops, r.putOp(r.subpath(kReplicasZNode), []byte{}))
		ops = append(ops, r.putOp(r.subpath(kMasterZNode), []byte{}))
		ops = append(ops, r.putOp(r.subpath(kMasterSeqZNode), []byte("0")))
	}
	r.version = part.Version

	hasPrimary := false
	newReplicas := map[string]rodisReplicaStatus{}
	for _, replica := range part.Replicas {
		hp := entries.Servers[replica.ServerIndex].ToHostPort()
		if replica.Role == pb.ReplicaRole_kPrimary {
			hasPrimary = true
			ops = append(ops, r.updateMasterOp(hp, GetSequenceNumber(replica.Info))...)
		}
		// for rodis, master is also in replicas dir
		op := r.updateReplicaOp(hp, replica.Role, newReplicas)
		if op != nil {
			ops = append(ops, op)
		}
	}
	if !hasPrimary {
		if op := r.cleanMaster(); op != nil {
			ops = append(ops, op)
		}
	}

	for hp := range r.replicas {
		if _, ok := newReplicas[hp]; !ok {
			ops = append(ops, &zk.DeleteRequest{
				Path:    r.subpath(kReplicasZNode, hp),
				Version: -1,
			})
		}
	}
	r.replicas = newReplicas
	r.hasZkEntry = true
	r.updateZk(store, ops)
}

func (r *rodisShardMeta) update(
	store *zkStore,
	entries *pb.RouteEntries,
	partId int,
) {
	part := entries.Partitions[partId]
	if r.version > part.Version {
		return
	}
	if r.version == part.Version {
		r.updateMasterSeq(store, entries.Servers, part)
		return
	}
	r.updateWholeShard(store, entries, part)
}

func (r *rodisShardMeta) updateZk(store *zkStore, ops []interface{}) {
	logging.Verbose(1, "%s: update zk request: %q", r.zkPath, ops)
	newVersion := fmt.Sprintf("%d", r.version)
	for {
		resp, err := store.GetRawSession().Multi(ops...)
		data, exists, succ := store.Get(context.Background(), r.zkPath)
		logging.Assert(succ, "")
		if exists && string(data) == newVersion {
			return
		}
		logging.Warning(
			"%s: update zk failed, check exists: %v, version data: %s, multiop err: %v, req: %+q, resp: %v",
			r.zkPath,
			exists,
			string(data),
			err,
			ops,
			resp,
		)
		time.Sleep(time.Second)
	}
}

func (r *rodisShardMeta) subpath(nodes ...string) string {
	output := r.zkPath
	for _, node := range nodes {
		output = output + "/" + node
	}
	return output
}
