package rodis_store

import (
	"context"
	"flag"
	"sort"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

const (
	kFrameZNode = "frame"
	kIDCZNode   = "idc"
)

var (
	flagSkipAzMatch = flag.Bool("skip_az_match", false, "skip az match")
)

type rodisFrameMeta struct {
	zkPath   string
	frame2Az map[string]string
	az2Idc   map[string]string
}

func newRodisFrameMeta(store *zkStore, path string) *rodisFrameMeta {
	output := &rodisFrameMeta{
		zkPath:   path,
		frame2Az: make(map[string]string),
		az2Idc:   make(map[string]string),
	}
	output.loadFromZk(store)
	return output
}

func (r *rodisFrameMeta) subpath(node string) string {
	return r.zkPath + "/" + node
}

func (r *rodisFrameMeta) loadFromZk(store *metastore.ZookeeperStore) {
	child, exists, succ := store.Children(context.Background(), r.zkPath)
	logging.Assert(succ && exists, "")
	for _, c := range child {
		data, exists, succ := store.Get(context.Background(), r.subpath(c))
		logging.Assert(exists && succ, "")
		r.frame2Az[c] = string(data)
	}
}

func (r *rodisFrameMeta) initializeAz2Idc(store *metastore.ZookeeperStore, frame, az, idc string) {
	if recordedAz, ok := r.frame2Az[frame]; ok {
		if !*flagSkipAzMatch {
			logging.Assert(
				recordedAz == az,
				"frame %s record in different az %s vs %s",
				frame,
				recordedAz,
				az,
			)
			r.az2Idc[az] = idc
		} else {
			logging.Error("frame %s record in az %s, but in %s by rms node detection, use record one", frame, recordedAz, az)
			r.az2Idc[recordedAz] = idc
		}
	} else {
		r.frame2Az[frame] = az
		r.az2Idc[az] = idc
		succ := store.Create(context.Background(), r.subpath(frame), []byte(az))
		logging.Assert(succ, "")
	}
}

func (r *rodisFrameMeta) getIdc(frame string) string {
	az, ok := r.frame2Az[frame]
	logging.Assert(ok, "can't find frame %s", frame)
	if idc, ok := r.az2Idc[az]; ok {
		return idc
	}
	region := utils.RegionOfAz(az)
	logging.Assert(region != "", "don't support az %s", az)
	return region
}

func (r *rodisFrameMeta) getSortedFrames() []*pb.ReplicaHub {
	var output []*pb.ReplicaHub
	for frame, az := range r.frame2Az {
		output = append(output, &pb.ReplicaHub{Name: frame, Az: az})
	}
	sort.Slice(output, func(i, j int) bool {
		return output[i].Name < output[j].Name
	})
	return output
}

func (r *rodisFrameMeta) addFrame(store *metastore.ZookeeperStore, frame, az string) {
	succ := store.Create(context.Background(), r.subpath(frame), []byte(az))
	logging.Assert(succ, "path %s add frame %s az %s failed", r.zkPath, frame, az)
	r.frame2Az[frame] = az
}

func (r *rodisFrameMeta) removeFrame(store *metastore.ZookeeperStore, frame string) {
	succ := store.Delete(context.Background(), r.subpath(frame))
	logging.Assert(succ, "")
	delete(r.frame2Az, frame)
}

func (r *rodisFrameMeta) update(store *metastore.ZookeeperStore, hubs []*pb.ReplicaHub) {
	newMap := utils.MapHubs(hubs)
	for iter := newMap.GetMap().Iterator(); iter.Next(); {
		hub, az := iter.Key().(string), iter.Value().(*pb.ReplicaHub).Az
		if val, ok := r.frame2Az[hub]; ok {
			logging.Assert(val == az, "frame %s az change from %s to %s", hub, val, az)
		} else {
			r.addFrame(store, hub, az)
		}
	}

	toRemove := []string{}
	for frame := range r.frame2Az {
		if _, ok := newMap.GetMap().Get(frame); !ok {
			toRemove = append(toRemove, frame)
		}
	}

	for _, frame := range toRemove {
		r.removeFrame(store, frame)
	}
}
