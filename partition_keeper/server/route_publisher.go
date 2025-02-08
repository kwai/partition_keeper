package server

import (
	"errors"
	"flag"
	"net"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/dbg"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"

	"github.com/emirpasic/gods/maps/treemap"
)

var (
	flagPublishMinIntervalSecs = flag.Int64(
		"publish_min_interval_secs",
		5,
		"publish min interval seconds",
	)
	flagPublishMaxIntervalSecs = flag.Int64(
		"publish_max_interval_secs",
		600,
		"publish max interval seconds",
	)
)

type RoutePublisher struct {
	logName         string
	table           *TableStats
	minIntervalSecs int64
	maxIntervalSecs int64
	routeStore      *route.CascadeRouteStore
	routeChanged    chan bool
	quit            chan bool
}

type publishOpts func(r *RoutePublisher)

func WithMinIntervalSecs(i int64) publishOpts {
	return func(r *RoutePublisher) {
		r.minIntervalSecs = i
	}
}

func WithMaxIntervalSecs(i int64) publishOpts {
	return func(r *RoutePublisher) {
		r.maxIntervalSecs = i
	}
}

func NewRoutePublisher(
	table *TableStats,
	storeOpts []*route.StoreOption,
	opts ...publishOpts,
) *RoutePublisher {
	ans := &RoutePublisher{
		logName:         table.TableName,
		table:           table,
		minIntervalSecs: *flagPublishMinIntervalSecs,
		maxIntervalSecs: *flagPublishMaxIntervalSecs,
		routeStore:      route.NewCascadeStore(storeOpts),
		quit:            make(chan bool),
		routeChanged:    make(chan bool, 1),
	}
	for _, opt := range opts {
		opt(ans)
	}
	return ans
}

func (n *RoutePublisher) NotifyChange() {
	select {
	case n.routeChanged <- true:
		logging.Verbose(1, "%s: change has notified", n.logName)
	default:
		logging.Verbose(1, "%s: has pending event to handle, skip new one", n.logName)
	}
}

func (n *RoutePublisher) Start() {
	go n.publishLoop()
}

func (n *RoutePublisher) Stop(clean bool) {
	logging.Info("%s: notify publisher to stop", n.logName)
	n.quit <- true
	if clean {
		n.tryRoute("Del", nil)
	}
	logging.Info("%s: finish notifying publisher to stop", n.logName)
}

func (n *RoutePublisher) tryRoute(tryType string, entries *pb.RouteEntries) *pb.RouteEntries {
	var tryCount uint64 = 1
	var result *pb.RouteEntries = nil
	for {
		var err error = nil
		switch tryType {
		case "Put":
			logging.Assert(entries != nil, "")
			if dbg.RunInSafeMode() {
				err = dbg.ErrSkipRunAsInSafeMode
			} else {
				err = n.routeStore.Put(entries)
			}
		case "Del":
			if dbg.RunInSafeMode() {
				err = dbg.ErrSkipRunAsInSafeMode
			} else {
				err = n.routeStore.Del()
			}
		case "getRoute":
			result = n.getRoute()
			if result == nil {
				err = errors.New("getRoute fail")
			}
		default:
			logging.Fatal("tryRoute type : %s error", tryType)
			return nil
		}

		if err != nil {
			logging.Warning(
				"%s: %s route to remote failed: %s. has try %d times",
				n.logName,
				tryType,
				err.Error(),
				tryCount,
			)
			if tryCount > 3 {
				third_party.PerfLog2(
					"reco",
					"reco.colossusdb.publish_route_fail",
					n.table.serviceName,
					n.logName,
					tryType,
					tryCount,
				)
			}
			time.Sleep(time.Second * 2)
		} else {
			break
		}
		tryCount++
	}
	return result
}

func (n *RoutePublisher) UpdateStores(storeOpts []*route.StoreOption) {
	n.routeStore.UpdateStores(storeOpts)
	n.NotifyChange()
}

func (n *RoutePublisher) getHubId(hubs []*pb.ReplicaHub, name string) int32 {
	for i, h := range hubs {
		if h.Name == name {
			return int32(i)
		}
	}
	return -1
}

func (n *RoutePublisher) getRoute() *pb.RouteEntries {
	n.table.serviceLock.LockRead()
	defer n.table.serviceLock.UnlockRead()

	output := pb.RouteEntries{
		TableInfo: &pb.TableInfo{
			HashMethod:   n.table.HashMethod,
			SplitVersion: n.table.SplitVersion,
			UsePaz:       n.table.usePaz,
		},
		Partitions:  []*pb.PartitionLocation{},
		Servers:     []*pb.ServerLocation{},
		ReplicaHubs: []*pb.ReplicaHub{},
	}

	idTree := treemap.NewWithStringComparator()
	for id, info := range n.table.nodes.AllNodes() {
		idTree.Put(id, info)
	}
	idIndex := map[string]int{}

	iter := idTree.Iterator()
	index := 0
	for iter.Next() {
		key, value := iter.Key().(string), iter.Value().(*node_mgr.NodeInfo)
		hubId := n.getHubId(n.table.hubs, value.Hub)
		if hubId == -1 {
			logging.Info(
				"%s: can't find node %s hub %s in table's hubs, this should be due to hub removal",
				n.logName,
				value.LogStr(),
				value.Hub,
			)
			logging.Assert(value.Op == pb.AdminNodeOp_kOffline, "")
		} else {
			idIndex[key] = index
			ips, err := net.LookupIP(value.Address.NodeName)
			if err != nil {
				logging.Error("%s: parse ip of %s failed: %s", n.logName, value.Address.NodeName, err.Error())
				return nil
			}
			output.Servers = append(output.Servers, &pb.ServerLocation{
				Host:  value.Address.NodeName,
				Port:  value.BizPort,
				HubId: n.getHubId(n.table.hubs, value.Hub),
				Info:  nil,
				Ip:    ips[0].String(),
				Alive: value.IsAlive,
				Op:    value.Op,
			})
			index++
		}
	}

	cloneStatistics := func(p *pb.PartitionReplica) map[string]string {
		if p == nil {
			return nil
		}
		if len(p.StatisticsInfo) == 0 {
			return nil
		}
		output := map[string]string{}
		for k, v := range p.StatisticsInfo {
			output[k] = v
		}
		return output
	}

	for i := range n.table.currParts {
		partition := &(n.table.currParts[i])

		replicas := []*pb.ReplicaLocation{}
		for node, role := range partition.members.Peers {
			idx, ok := idIndex[node]
			if ok {
				replicas = append(replicas, &pb.ReplicaLocation{
					ServerIndex: int32(idx),
					Role:        role,
					Info:        cloneStatistics(partition.getPartitionReplica(node)),
				})
			} else {
				logging.Verbose(1, "%s: skip to add node %s as can't find its index", n.logName, node)
			}
		}

		output.Partitions = append(output.Partitions, &pb.PartitionLocation{
			Replicas:     replicas,
			Version:      partition.members.MembershipVersion,
			SplitVersion: partition.members.SplitVersion,
		})
	}

	for _, hub := range n.table.hubs {
		output.ReplicaHubs = append(output.ReplicaHubs, &pb.ReplicaHub{
			Name: hub.Name,
			Az:   hub.Az,
		})
	}

	return &output
}

func (n *RoutePublisher) publishRoute(entries *pb.RouteEntries) {
	logging.Info("[change_state] %s: get route data, push to route store", n.logName)
	n.tryRoute("Put", entries)
}

func (n *RoutePublisher) runPublish() bool {
	var entries *pb.RouteEntries = n.tryRoute("getRoute", nil)
	n.publishRoute(entries)

	t := time.NewTimer(time.Second * time.Duration(n.minIntervalSecs))
	select {
	case <-t.C:
		logging.Verbose(1, "%s: time has elapsed, start to handle next event", n.logName)
		return false
	case <-n.quit:
		return true
	}
}

func (n *RoutePublisher) publishLoop() {
	ticker := time.NewTicker(time.Second * time.Duration(n.maxIntervalSecs))
	quit := false
	for !quit {
		select {
		case <-ticker.C:
			quit = n.runPublish()
		case <-n.routeChanged:
			quit = n.runPublish()
		case <-n.quit:
			quit = true
		}
	}
	ticker.Stop()
}
