package utils

import (
	"fmt"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"google.golang.org/protobuf/proto"

	"github.com/emirpasic/gods/maps/treemap"
)

type HubHandle struct {
	// hubname -> *pb.ReplicaHub
	hubmap *treemap.Map
}

func NewHubHandle() *HubHandle {
	return &HubHandle{
		hubmap: treemap.NewWithStringComparator(),
	}
}

func MapHubs(hubs []*pb.ReplicaHub) *HubHandle {
	ans := NewHubHandle()
	for _, h := range hubs {
		ans.hubmap.Put(h.Name, proto.Clone(h))
	}
	return ans
}

func (hh *HubHandle) GetMap() *treemap.Map {
	return hh.hubmap
}

func (hh *HubHandle) Clone() *HubHandle {
	return &HubHandle{hh.cloneMap()}
}

func (hh *HubHandle) cloneMap() *treemap.Map {
	output := treemap.NewWithStringComparator()
	for iter := hh.hubmap.Iterator(); iter.Next(); {
		output.Put(iter.Key(), proto.Clone(iter.Value().(*pb.ReplicaHub)))
	}
	return output
}

func (hh *HubHandle) ListHubs() (ans []*pb.ReplicaHub) {
	for iter := hh.hubmap.Iterator(); iter.Next(); {
		hub := iter.Value().(*pb.ReplicaHub)
		ans = append(ans, proto.Clone(hub).(*pb.ReplicaHub))
	}
	return
}

func (hh *HubHandle) GetAzs() map[string]bool {
	output := map[string]bool{}
	for iter := hh.hubmap.Iterator(); iter.Next(); {
		output[iter.Value().(*pb.ReplicaHub).Az] = true
	}
	return output
}

func (hh *HubHandle) AddHub(hub *pb.ReplicaHub) error {
	if _, ok := hh.hubmap.Get(hub.Name); ok {
		return fmt.Errorf("hub %s already in handle", hub.Name)
	}
	hh.hubmap.Put(hub.Name, proto.Clone(hub))
	return nil
}

func (hh *HubHandle) AddHubs(hubs []*pb.ReplicaHub) error {
	tmp := hh.cloneMap()
	for _, h := range hubs {
		if az, ok := tmp.Get(h.Name); ok {
			return fmt.Errorf("conflict hubname %s in az %s and %s",
				h.Name,
				az.(*pb.ReplicaHub).Az,
				h.Az,
			)
		}
		tmp.Put(h.Name, proto.Clone(h))
	}
	hh.hubmap = tmp
	return nil
}

func (hh *HubHandle) RemoveHubs(hubs []*pb.ReplicaHub) error {
	tmp := hh.cloneMap()
	for _, h := range hubs {
		if _, ok := tmp.Get(h.Name); !ok {
			return fmt.Errorf("can't find hubname %s to remove", h.Name)
		}
		tmp.Remove(h.Name)
	}
	hh.hubmap = tmp
	return nil
}

func (hh *HubHandle) UpdateHubProps(hubs []*pb.ReplicaHub) error {
	for _, h := range hubs {
		if _, ok := hh.hubmap.Get(h.Name); !ok {
			return fmt.Errorf("can't find hubname %s to update", h.Name)
		}
	}
	for _, h := range hubs {
		value, _ := hh.hubmap.Get(h.Name)
		hub := value.(*pb.ReplicaHub)
		// well, we will keep name & azs unchanged
		hub.DisallowedRoles = nil
		hub.DisallowedRoles = append(hub.DisallowedRoles, h.DisallowedRoles...)
	}
	return nil
}

func (hh *HubHandle) HubsOnAz(az string) []string {
	output := []string{}
	for iter := hh.hubmap.Iterator(); iter.Next(); {
		if iter.Value().(*pb.ReplicaHub).Az == az {
			output = append(output, iter.Key().(string))
		}
	}
	return output
}

func (hh *HubHandle) GetAz(hub string) string {
	val, ok := hh.hubmap.Get(hub)
	if !ok {
		return ""
	}
	return val.(*pb.ReplicaHub).Az
}

func (hh *HubHandle) DivideByAz() map[string][]string {
	output := map[string][]string{}
	for iter := hh.hubmap.Iterator(); iter.Next(); {
		name, az := iter.Key().(string), iter.Value().(*pb.ReplicaHub).Az
		output[az] = append(output[az], name)
	}
	return output
}

func (hh *HubHandle) Diff(h2 *HubHandle) (*HubHandle, *HubHandle) {
	left := hh.cloneMap()
	right := h2.cloneMap()

	shared := []string{}
	for iter := left.Iterator(); iter.Next(); {
		key := iter.Key().(string)
		if _, ok := right.Get(key); ok {
			shared = append(shared, key)
		}
	}

	for _, key := range shared {
		left.Remove(key)
		right.Remove(key)
	}

	return &HubHandle{left}, &HubHandle{right}
}
