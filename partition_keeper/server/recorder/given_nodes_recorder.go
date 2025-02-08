package recorder

import "github.com/kuaishou/open_partition_keeper/partition_keeper/pb"

type GivenNodesRecorder struct {
	nodes map[string]NodeRecorder
}

func RecordGivenNodes(nodes map[string]bool, builder BuildRecorder) *GivenNodesRecorder {
	ans := &GivenNodesRecorder{
		nodes: make(map[string]NodeRecorder),
	}
	for node := range nodes {
		ans.nodes[node] = builder(node)
	}
	return ans
}

func (b *GivenNodesRecorder) Clone() NodesRecorder {
	output := &GivenNodesRecorder{
		nodes: map[string]NodeRecorder{},
	}
	for name, rec := range b.nodes {
		output.nodes[name] = rec.Clone()
	}
	return output
}

func (b *GivenNodesRecorder) GetNode(n string) NodeRecorder {
	return b.nodes[n]
}

func (b *GivenNodesRecorder) Add(n string, tbl, part int32, r pb.ReplicaRole, weight int) {
	if record, ok := b.nodes[n]; ok {
		record.Add(tbl, part, r, weight)
	}
}
