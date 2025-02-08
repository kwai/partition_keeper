package rodis_store

import (
	"context"
	"fmt"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
)

type rodisNodeStatus string

const (
	rodisNodeError      rodisNodeStatus = "NODE_ERROR"
	rodisNodeEnable     rodisNodeStatus = "NODE_ENABLE"
	rodisNodeTempDown   rodisNodeStatus = "NODE_TEMPDOWN"
	rodisNodePreDisable rodisNodeStatus = "NODE_PREDISABLE"
	rodisNodeDisable    rodisNodeStatus = "NODE_DISABLE"
)

func (ns *rodisNodeStatus) translateFrom(server *pb.ServerLocation) {
	switch server.Op {
	case pb.AdminNodeOp_kNoop:
		if server.Alive {
			*ns = rodisNodeEnable
		} else {
			*ns = rodisNodeError
		}
	case pb.AdminNodeOp_kRestart:
		*ns = rodisNodeTempDown
	case pb.AdminNodeOp_kOffline:
		*ns = rodisNodePreDisable
	default:
		logging.Assert(false, "not reachable %s", server.Op.String())
	}
}

func (ns rodisNodeStatus) translateTo(server *pb.ServerLocation) {
	if ns == rodisNodeError {
		server.Alive = false
		server.Op = pb.AdminNodeOp_kNoop
		return
	}
	switch ns {
	case rodisNodeEnable:
		server.Alive = true
		server.Op = pb.AdminNodeOp_kNoop
	case rodisNodeTempDown:
		server.Alive = true
		server.Op = pb.AdminNodeOp_kRestart
	case rodisNodePreDisable, rodisNodeDisable:
		server.Alive = true
		server.Op = pb.AdminNodeOp_kOffline
	}
}

type rodisNodeMeta struct {
	zkPath     string
	hasZkEntry bool
	status     rodisNodeStatus
	frame      string
	idc        string
}

func newRodisNodeMeta(parentPath, hostport string) *rodisNodeMeta {
	output := &rodisNodeMeta{
		zkPath:     fmt.Sprintf("%s/%s", parentPath, hostport),
		hasZkEntry: false,
		status:     "",
		frame:      "",
		idc:        "",
	}
	return output
}

func (r *rodisNodeMeta) subpath(n string) string {
	return r.zkPath + "/" + n
}

func (r *rodisNodeMeta) loadFromZk(store *zkStore) {
	r.hasZkEntry = true

	data, exists, succ := store.Get(context.Background(), r.zkPath)
	logging.Assert(exists && succ, "")
	r.status = rodisNodeStatus(string(data))

	data, exists, succ = store.Get(context.Background(), r.subpath(kFrameZNode))
	logging.Assert(exists && succ, "")
	r.frame = string(data)

	data, exists, succ = store.Get(context.Background(), r.subpath(kIDCZNode))
	logging.Assert(exists && succ, "")
	r.idc = string(data)
}

func (r *rodisNodeMeta) updateZk(store *zkStore) {
	path := []string{
		r.zkPath,
		r.subpath(kFrameZNode),
		r.subpath(kIDCZNode),
	}
	data := [][]byte{
		[]byte(r.status),
		[]byte(r.frame),
		[]byte(r.idc),
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

func (r *rodisNodeMeta) update(
	store *zkStore,
	server *pb.ServerLocation,
	hub *pb.ReplicaHub,
	idc string,
) {
	needUpdate := false
	var newStatus rodisNodeStatus
	newStatus.translateFrom(server)
	if r.status != newStatus {
		r.status = newStatus
		needUpdate = true
	}
	if hub.Name != r.frame {
		r.frame = hub.Name
		needUpdate = true
	}
	if idc != r.idc {
		r.idc = idc
		needUpdate = true
	}
	if needUpdate {
		r.updateZk(store)
	}
}

func (r *rodisNodeMeta) remove(store *zkStore) {
	path := []string{
		r.subpath(kFrameZNode),
		r.subpath(kIDCZNode),
		r.zkPath,
	}
	succ := store.MultiDelete(context.Background(), path)
	logging.Assert(succ, "")
}
