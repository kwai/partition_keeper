package recorder

import "github.com/kuaishou/open_partition_keeper/partition_keeper/pb"

type AllNodesRecorder struct {
	nodes map[string]NodeRecorder
	build BuildRecorder
}

func NewAllNodesRecorder(builder BuildRecorder) *AllNodesRecorder {
	return &AllNodesRecorder{
		nodes: make(map[string]NodeRecorder),
		build: builder,
	}
}

func (d *AllNodesRecorder) Clone() NodesRecorder {
	output := NewAllNodesRecorder(d.build)
	for name, rec := range d.nodes {
		output.nodes[name] = rec.Clone()
	}
	return output
}

func (d *AllNodesRecorder) GetNode(node string) NodeRecorder {
	if res, ok := d.nodes[node]; ok {
		return res
	} else {
		ans := d.build(node)
		d.nodes[node] = ans
		return ans
	}
}

func (d *AllNodesRecorder) GetNodes() map[string]NodeRecorder {
	return d.nodes
}

func (d *AllNodesRecorder) Add(
	node string,
	tableId, partId int32,
	role pb.ReplicaRole,
	weight int,
) {
	nt := d.GetNode(node)
	nt.Add(tableId, partId, role, weight)
}

func (d *AllNodesRecorder) Transform(
	node string,
	tableId, partId int32,
	from, to pb.ReplicaRole,
	weight int,
) {
	nt := d.GetNode(node)
	nt.Transform(tableId, partId, from, to, weight)
}

func (d *AllNodesRecorder) Remove(
	node string,
	tableId, partId int32,
	role pb.ReplicaRole,
	weight int,
) {
	nt := d.GetNode(node)
	nt.Remove(tableId, partId, role, weight)
}

func (d *AllNodesRecorder) Count(node string, role pb.ReplicaRole) int {
	return d.GetNode(node).Count(role)
}

func (d *AllNodesRecorder) CountAll(node string) int {
	return d.GetNode(node).CountAll()
}
