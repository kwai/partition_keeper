package utils

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"google.golang.org/protobuf/proto"

	"github.com/emirpasic/gods/maps/treemap"
	"gotest.tools/assert"
)

func treemapFromMap(m map[string]string) *treemap.Map {
	ans := treemap.NewWithStringComparator()
	for key, value := range m {
		ans.Put(key, &pb.ReplicaHub{
			Name: key,
			Az:   value,
		})
	}
	return ans
}

func TestHubHandle(t *testing.T) {
	hubs := []*pb.ReplicaHub{
		{Name: "gz1", Az: "gz"},
		{Name: "yz1", Az: "yz"},
		{Name: "yz2", Az: "yz"},
		{Name: "zw1", Az: "zw"},
	}
	handle := MapHubs(hubs)

	expectMap := treemapFromMap(
		map[string]string{"yz1": "yz", "yz2": "yz", "zw1": "zw", "gz1": "gz"},
	)
	assert.Assert(
		t,
		TreemapEqual(
			handle.GetMap(),
			expectMap,
		),
	)

	assert.Assert(t, TreemapEqual(handle.cloneMap(), expectMap))
	assert.Assert(t, HubsEqual(hubs, handle.ListHubs()))

	assert.DeepEqual(t, handle.GetAzs(), map[string]bool{"gz": true, "yz": true, "zw": true})

	ans := handle.AddHubs([]*pb.ReplicaHub{
		{Name: "gz1", Az: "gz2"},
	})
	assert.Assert(t, ans != nil)
	assert.Assert(t, TreemapEqual(handle.GetMap(), expectMap))

	ans = handle.AddHubs([]*pb.ReplicaHub{
		{Name: "gz1", Az: "gz"},
		{Name: "gz2", Az: "gz"},
	})
	assert.Assert(t, ans != nil)
	assert.Assert(t, TreemapEqual(handle.GetMap(), expectMap))

	ans = handle.AddHubs([]*pb.ReplicaHub{
		{Name: "gz2", Az: "gz"},
	})
	assert.NilError(t, ans)

	expectMap.Put("gz2", &pb.ReplicaHub{Name: "gz2", Az: "gz"})
	assert.Assert(t, TreemapEqual(handle.GetMap(), expectMap))

	ans = handle.RemoveHubs([]*pb.ReplicaHub{
		{Name: "gz1", Az: "gz"},
		{Name: "gz3", Az: "gz"},
	})
	assert.Assert(t, ans != nil)
	assert.Assert(t, TreemapEqual(handle.GetMap(), expectMap))

	ans = handle.RemoveHubs([]*pb.ReplicaHub{
		{Name: "gz3", Az: "gz"},
	})
	assert.Assert(t, ans != nil)
	assert.Assert(t, TreemapEqual(handle.GetMap(), expectMap))

	ans = handle.RemoveHubs([]*pb.ReplicaHub{
		{Name: "gz1", Az: "gz"},
	})
	assert.NilError(t, ans)
	expectMap.Remove("gz1")
	assert.Assert(t, TreemapEqual(handle.GetMap(), expectMap))

	assert.DeepEqual(t, handle.HubsOnAz("yz"), []string{"yz1", "yz2"})

	assert.DeepEqual(t, handle.DivideByAz(), map[string][]string{
		"gz": {"gz2"},
		"yz": {"yz1", "yz2"},
		"zw": {"zw1"},
	})

	handle2 := MapHubs([]*pb.ReplicaHub{
		{Name: "yz1", Az: "yz"},
		{Name: "sgp1", Az: "sgp"},
	})

	handle1Hubs := treemapFromMap(map[string]string{"gz2": "gz", "yz2": "yz", "zw1": "zw"})
	handle2Hubs := treemapFromMap(map[string]string{"sgp1": "sgp"})

	left, right := handle.Diff(handle2)
	assert.Assert(t, TreemapEqual(left.hubmap, handle1Hubs))
	assert.Assert(t, TreemapEqual(right.hubmap, handle2Hubs))

	left, right = handle2.Diff(handle)
	assert.Assert(t, TreemapEqual(right.hubmap, handle1Hubs))
	assert.Assert(t, TreemapEqual(left.hubmap, handle2Hubs))

	assert.Assert(t, TreemapEqual(handle.GetMap(), expectMap))
	expectMap2 := treemapFromMap(map[string]string{"yz1": "yz", "sgp1": "sgp"})
	assert.Assert(t, TreemapEqual(handle2.GetMap(), expectMap2))

	handle3 := handle.Clone()
	left, right = handle.Diff(handle3)
	assert.Equal(t, left.hubmap.Size(), 0)
	assert.Equal(t, right.hubmap.Size(), 0)
}

func TestUpdateHubProps(t *testing.T) {
	hubs := []*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ"},
		{Name: "zw1", Az: "ZW"},
	}
	handle := MapHubs(hubs)

	err := handle.UpdateHubProps([]*pb.ReplicaHub{
		{Name: "yz1", Az: "YZ", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kPrimary}},
		{Name: "gz1", Az: "GZ1", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kSecondary}},
	})
	assert.Assert(t, err != nil)
	gotHubs := handle.ListHubs()
	assert.Equal(t, len(hubs), len(gotHubs))
	for i := 0; i < len(hubs); i++ {
		assert.Assert(t, proto.Equal(hubs[i], gotHubs[i]))
	}

	err = handle.UpdateHubProps(
		[]*pb.ReplicaHub{
			{Name: "yz1", Az: "YZ", DisallowedRoles: []pb.ReplicaRole{pb.ReplicaRole_kPrimary}},
		},
	)
	assert.NilError(t, err)

	hubs[0].DisallowedRoles = []pb.ReplicaRole{pb.ReplicaRole_kPrimary}
	gotHubs = handle.ListHubs()
	assert.Equal(t, len(hubs), len(gotHubs))
	for i := 0; i < len(hubs); i++ {
		assert.Assert(t, proto.Equal(hubs[i], gotHubs[i]))
	}
}
