package watcher

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
	"gotest.tools/assert/cmp"
)

func TestAzSizeWatcher(t *testing.T) {
	hubs := []*pb.ReplicaHub{
		{Name: "YZ_1", Az: "YZ"},
		{Name: "YZ_2", Az: "YZ"},
		{Name: "ZW_1", Az: "ZW"},
	}
	AzSize := []int{4, 4, 2}
	te := setupNodeWatcherTestEnv(t, hubs, AzSize)
	defer te.teardown()

	watcher := NewWatcher(AzSizeWatcherName)
	assert.Equal(t, watcher.Name(), AzSizeWatcherName)

	req := &pb.ExpandAzsRequest{
		ServiceName: "",
		AzOptions: []*pb.ExpandAzsRequest_AzOption{
			{Az: "YZ", NewSize: 12},
			{Az: "ZW", NewSize: 4},
		},
	}

	watcher.UpdateParameters(req)
	data := watcher.Serialize()

	req2 := &pb.ExpandAzsRequest{}
	utils.UnmarshalJsonOrDie(data, req2)
	assert.Assert(t, proto.Equal(req, req2))

	watcher.Deserialize(data)
	data2 := watcher.Serialize()
	assert.DeepEqual(t, data, data2)

	// Cloned value update parameters won't affect old ones
	watcher2 := watcher.Clone()
	watcher2.UpdateParameters(&pb.ExpandAzsRequest{
		ServiceName: "",
		AzOptions: []*pb.ExpandAzsRequest_AzOption{
			{Az: "YZ", NewSize: 10},
		},
	})
	assert.Assert(t, proto.Equal(req, watcher.GetParameters().(*pb.ExpandAzsRequest)))

	ops, satisfied := watcher.StatSatisfied("test", te.nodeStats)
	assert.Assert(t, cmp.Len(ops, 0))
	assert.Equal(t, satisfied, false)

	req.AzOptions[0].NewSize = 8
	watcher.UpdateParameters(req)
	ops, satisfied = watcher.StatSatisfied("test", te.nodeStats)
	assert.Assert(t, cmp.Len(ops, 0))
	assert.Equal(t, satisfied, false)

	req.AzOptions[1].NewSize = 2
	watcher.UpdateParameters(req)
	ops, satisfied = watcher.StatSatisfied("test", te.nodeStats)
	assert.Assert(t, cmp.Len(ops, 0))
	assert.Equal(t, satisfied, true)
}
