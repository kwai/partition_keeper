package proxy

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/proxy/meta"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

var (
	flagZkHosts  utils.StrlistFlag
	flagZkPrefix = flag.String("proxy_zk_prefix", "/ks/reco/keeper", "zk prefix")
)

func init() {
	flag.Var(&flagZkHosts, "proxy_zk_hosts", "zk hosts with format <ip:port>,<ip:port>...")
}

type Watcher struct {
	meta    *meta.Meta
	zkStore *metastore.ZookeeperStore
	quits   []chan bool
	stop    chan bool
}

type serviceProps struct {
	SvcTypeRep string `json:"service_type"`
}

func NewWatcher(meta *meta.Meta) *Watcher {
	ans := &Watcher{
		meta: meta,
		stop: make(chan bool),
	}
	return ans
}

func (w *Watcher) Start() {
	logging.Info("start init zk")
	w.zkStore = metastore.CreateZookeeperStore(
		flagZkHosts,
		time.Second*60,
		zk.WorldACL(zk.PermRead),
		"",
		[]byte(""),
	)

	go w.checkNamespaceChange()
}

func (w *Watcher) Stop() {
	w.stop <- true
	for _, quit := range w.quits {
		quit <- true
	}
	w.zkStore.Close()
}

func (w *Watcher) WatchChildren(
	conn *zk.Conn,
	path string,
) ([]string, *zk.Stat, <-chan zk.Event, error) {
	return conn.ChildrenW(path)
}

func (w *Watcher) updateService(
	serviceChildren map[string]pb.ServiceType,
	services []string,
	path string,
	namespace string,
) {
	allServices := make(map[string]bool)
	for _, service := range services {
		allServices[service] = true
	}

	delServices := make(map[string]pb.ServiceType)
	addServices := make(map[string]pb.ServiceType)
	for _, service := range services {
		if _, ok := serviceChildren[service]; !ok {
			data, exist, succ := w.zkStore.Get(context.Background(), path+"/"+service)
			logging.Assert(succ, "")
			if !exist {
				logging.Warning(
					"%s children get new service :%s, but get service data failed, it does not exist. skip it",
					namespace,
					service,
				)
				continue
			}

			prop := &serviceProps{}
			utils.UnmarshalJsonOrDie(data, prop)
			// old embedding server services may be setup without service type
			if prop.SvcTypeRep == "" {
				prop.SvcTypeRep = pb.ServiceType_name[int32(pb.ServiceType_colossusdb_embedding_server)]
			}
			addServices[service] = pb.ServiceType(pb.ServiceType_value[prop.SvcTypeRep])
			serviceChildren[service] = pb.ServiceType(pb.ServiceType_value[prop.SvcTypeRep])
			logging.Info("%s add service:%s type:%s", namespace, service, prop.SvcTypeRep)
		}
	}

	for service, serviceType := range serviceChildren {
		if _, ok := allServices[service]; !ok {
			delServices[service] = serviceType
			delete(serviceChildren, service)
			logging.Info(
				"%s delete service:%s type:%s",
				namespace,
				service,
				pb.ServiceType_name[int32(serviceType)],
			)
		}
	}

	w.meta.AddServices(addServices, namespace)
	w.meta.DelServices(delServices, namespace)
}

func (w *Watcher) watchService(path string, namespace string, quit chan bool) {
	logging.Info("start service watch path : %s", path)
	serviceChildren := make(map[string]pb.ServiceType)
	for {
		services, _, event, err := w.WatchChildren(w.zkStore.GetRawSession(), path)
		logging.Assert(err == nil, "watch zk path %s got err: %v", path, err)

		w.updateService(serviceChildren, services, path, namespace)
		select {
		case e := <-event:
			if e.Err != nil {
				logging.Error(
					"service watch children path %s, got error event:%s",
					path,
					e.Err.Error(),
				)
			}
		case <-quit:
			return
		}
	}
}

func (w *Watcher) updateTable(
	tableChildren map[string]bool,
	tables []string,
	path string,
	namespace string,
) {
	allTables := make(map[string]bool)
	for _, table := range tables {
		if table == "__RECYCLE_IDS__" {
			continue
		}
		allTables[table] = true
	}

	var delTables []string
	var addTables []string
	for _, table := range tables {
		if table == "__RECYCLE_IDS__" {
			continue
		}
		if _, ok := tableChildren[table]; !ok {
			addTables = append(addTables, table)
			tableChildren[table] = true
			logging.Info("%s add table:%s", namespace, table)
		}
	}
	for table := range tableChildren {
		if _, ok := allTables[table]; !ok {
			delTables = append(delTables, table)
			delete(tableChildren, table)
			logging.Info("%s delete table:%s", namespace, table)
		}
	}

	w.meta.AddTables(addTables)
	w.meta.DelTables(delTables)

}

func (w *Watcher) watchTable(path string, namespace string, quit chan bool) {
	logging.Info("start table watch path : %s", path)
	tableChildren := make(map[string]bool)

	for {
		tables, _, event, err := w.WatchChildren(w.zkStore.GetRawSession(), path)
		logging.Assert(err == nil, "watch zk path %s got err: %v", path, err)

		w.updateTable(tableChildren, tables, path, namespace)
		select {
		case e := <-event:
			if e.Err != nil {
				logging.Error(
					"table watch children path %s, got error event:%s",
					path,
					e.Err.Error(),
				)
			}
		case <-quit:
			return
		}
	}
}

func (w *Watcher) updateLeader(path string, children []string, namespace string) {
	minSeqIndex := -1
	minSeq := -1
	for i, child := range children {
		parts := strings.Split(child, "-")
		if len(parts) == 1 {
			parts = strings.Split(child, "__")
		}
		seq, err := strconv.Atoi(parts[len(parts)-1])
		if err != nil {
			logging.Info("%s: got invalid seq node %s: %s", namespace, child, err.Error())
		} else {
			if minSeq == -1 || minSeq > seq {
				minSeq = seq
				minSeqIndex = i
			}
		}
	}
	if minSeqIndex == -1 {
		logging.Error("%s: can't find valid child", namespace)
		return
	}

	leaderPath := fmt.Sprintf("%s/%s", path, children[minSeqIndex])
	data, _, err := w.zkStore.GetRawSession().Get(leaderPath)
	if err != nil {
		logging.Error("%s: read zk %s failed: %s", namespace, leaderPath, err.Error())
		return
	}

	logging.Info("%s host change to %s", namespace, string(data))
	w.meta.HostChange(string(data), namespace)
}

func (w *Watcher) watchLeader(path string, namespace string, quit chan bool) {
	logging.Info("start leader watch path : %s", path)

	for {
		children, _, event, err := w.WatchChildren(w.zkStore.GetRawSession(), path)
		logging.Assert(err == nil, "watch zk path %s got err: %v", path, err)
		w.updateLeader(path, children, namespace)

		select {
		case e := <-event:
			if e.Err != nil {
				logging.Error(
					"leader watch children path %s, got error event:%s",
					path,
					e.Err.Error(),
				)
			}
		case <-quit:
			return
		}
	}
}

func (w *Watcher) checkNamespaceChange() {
	tick := time.NewTicker(time.Second * 2)
	for {
		select {
		case <-tick.C:
			if w.meta.GetKconfChange() {
				w.meta.SetKconfChange(0)
				logging.Info("kconf change, start reset watch. set kconf change false")
				for _, quit := range w.quits {
					quit <- true
				}
				w.quits = []chan bool{}

				namespaces := w.meta.GetNamespaces()
				for _, n := range namespaces {
					leaderPath := *flagZkPrefix + "/" + n + "/LOCK"
					quitLeader := make(chan bool)
					w.quits = append(w.quits, quitLeader)
					go w.watchLeader(leaderPath, n, quitLeader)

					servicePath := *flagZkPrefix + "/" + n + "/service"
					quitService := make(chan bool)
					w.quits = append(w.quits, quitService)
					go w.watchService(servicePath, n, quitService)

					tablePath := *flagZkPrefix + "/" + n + "/tables"
					quitTable := make(chan bool)
					w.quits = append(w.quits, quitTable)
					go w.watchTable(tablePath, n, quitTable)
				}
				logging.Info("reset watch succ")
			}
		case <-w.stop:
			return
		}
	}
}
