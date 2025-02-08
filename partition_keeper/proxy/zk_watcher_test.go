package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/proxy/meta"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

var (
	proxyWatchTestZkPrefix = utils.SpliceZkRootPath("/test/proxy/watch")
	namespaces             = []string{"test_default", "test_rodis", "test_rodis2"}
	hosts                  = []string{"test_host:1000", "test_host:1001", "test_host:1002"}
)

func InitProxyTestZk(t *testing.T) *metastore.ZookeeperStore {
	flagZkHosts = []string{"127.0.0.1:2181"}
	*flagZkPrefix = proxyWatchTestZkPrefix

	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	zkStore := metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"},
		time.Second*10,
		acl,
		scheme,
		auth,
	)
	// init zk path
	ans := zkStore.RecursiveDelete(context.Background(), proxyWatchTestZkPrefix)
	assert.Assert(t, ans)
	ans = zkStore.RecursiveCreate(context.Background(), proxyWatchTestZkPrefix)
	assert.Assert(t, ans)
	for i, namespace := range namespaces {
		leaderPath := proxyWatchTestZkPrefix + "/" + namespace + "/LOCK/_c_xxxxxxxxxxxx-lock-000000000"
		ans = zkStore.RecursiveCreate(
			context.Background(),
			leaderPath,
		)
		assert.Assert(t, ans)
		ans = zkStore.Set(context.Background(), leaderPath, []byte(hosts[i]))
		assert.Assert(t, ans)
		ans = zkStore.RecursiveCreate(
			context.Background(),
			proxyWatchTestZkPrefix+"/"+namespace+"/service",
		)
		assert.Assert(t, ans)
		ans = zkStore.RecursiveCreate(
			context.Background(),
			proxyWatchTestZkPrefix+"/"+namespace+"/tables",
		)
		assert.Assert(t, ans)
	}
	return zkStore
}
func TestWatchService(t *testing.T) {
	zkStore := InitProxyTestZk(t)
	*meta.FlagKconfPath = "reco.partitionKeeper.proxy_test"
	m := meta.NewMeta()
	m.Initialize()

	watcher := NewWatcher(m)
	watcher.Start()
	time.Sleep(time.Second)

	//add service
	services := m.GetServices()
	assert.Equal(t, len(services), 0)

	rodisServicePrefix := proxyWatchTestZkPrefix + "/test_rodis/service"
	var prop serviceProps
	prop.SvcTypeRep = "colossusdb_rodis"
	data := utils.MarshalJsonOrDie(prop)
	ok := zkStore.Create(context.Background(), rodisServicePrefix+"/rodis_service_1", data)
	assert.Equal(t, ok, true)
	ok = zkStore.Create(context.Background(), rodisServicePrefix+"/rodis_service_2", data)
	assert.Equal(t, ok, true)

	defaultServicePrefix := proxyWatchTestZkPrefix + "/test_default/service"
	type tmpServiceProps struct {
		Dropped    bool `json:"dropped"`
		DoSchedule bool `json:"do_schedule"`
	}
	tmpProp := tmpServiceProps{
		Dropped:    false,
		DoSchedule: true,
	}
	data = utils.MarshalJsonOrDie(tmpProp)
	// old embedding server services may be setup without service type
	ok = zkStore.Create(context.Background(), defaultServicePrefix+"/embedding_service_1", data)
	assert.Equal(t, ok, true)
	time.Sleep(time.Second * 2)

	services = m.GetServices()
	assert.Equal(t, len(services), 3)
	url := m.GetUrlByService("rodis_service_1")
	assert.Assert(t, url == "http://test_host:1001")
	url = m.GetUrlByService("embedding_service_1")
	assert.Assert(t, url == "http://test_host:1000")

	// delete service
	ok = zkStore.Delete(context.Background(), rodisServicePrefix+"/rodis_service_1")
	assert.Equal(t, ok, true)
	time.Sleep(time.Second)
	services = m.GetServices()
	assert.Equal(t, len(services), 2)
	assert.Assert(t, services[0] == "rodis_service_2" || services[0] == "embedding_service_1")
	url = m.GetUrlByService("rodis_service_1")
	assert.Assert(t, url == "")

	watcher.Stop()
	time.Sleep(time.Second)
}

func TestWatchTable(t *testing.T) {
	zkStore := InitProxyTestZk(t)
	*meta.FlagKconfPath = "reco.partitionKeeper.proxy_test"
	m := meta.NewMeta()
	m.Initialize()

	watcher := NewWatcher(m)
	watcher.Start()
	time.Sleep(time.Second)

	//add table
	ok := m.TableExist("rodis_table_1")
	assert.Equal(t, ok, false)

	rodisTablePrefix := proxyWatchTestZkPrefix + "/test_rodis/tables"
	ok = zkStore.Create(context.Background(), rodisTablePrefix+"/rodis_table_1", []byte{})
	assert.Equal(t, ok, true)
	ok = zkStore.Create(context.Background(), rodisTablePrefix+"/rodis_table_2", []byte{})
	assert.Equal(t, ok, true)
	ok = zkStore.Create(context.Background(), rodisTablePrefix+"/__RECYCLE_IDS__", []byte{})
	assert.Equal(t, ok, true)
	time.Sleep(time.Second * 2)

	ok = m.TableExist("rodis_table_1")
	assert.Equal(t, ok, true)
	ok = m.TableExist("rodis_table_2")
	assert.Equal(t, ok, true)
	ok = m.TableExist("__RECYCLE_IDS__")
	assert.Equal(t, ok, false)

	// delete service
	ok = zkStore.Delete(context.Background(), rodisTablePrefix+"/rodis_table_1")
	assert.Equal(t, ok, true)
	time.Sleep(time.Second)
	ok = m.TableExist("rodis_table_1")
	assert.Equal(t, ok, false)
	ok = m.TableExist("rodis_table_2")
	assert.Equal(t, ok, true)

	watcher.Stop()
	time.Sleep(time.Second)
}

func TestWatchLeader(t *testing.T) {
	zkStore := InitProxyTestZk(t)
	*meta.FlagKconfPath = "reco.partitionKeeper.proxy_test"
	m := meta.NewMeta()
	m.Initialize()

	watcher := NewWatcher(m)
	watcher.Start()

	rodisLeaderPath := proxyWatchTestZkPrefix + "/test_rodis/LOCK/_c_xxxxxxxxxxxx-lock-000000000"
	rodisLeaderPath1 := proxyWatchTestZkPrefix + "/test_rodis/LOCK/_c_xxxxxxxxxxxx-lock-000000001"
	ans := zkStore.RecursiveCreate(
		context.Background(),
		rodisLeaderPath1,
	)
	assert.Assert(t, ans)
	ans = zkStore.Set(context.Background(), rodisLeaderPath1, []byte("test_host:9999"))
	assert.Assert(t, ans)
	//add service
	services := m.GetServices()
	assert.Equal(t, len(services), 0)

	rodisServicePrefix := proxyWatchTestZkPrefix + "/test_rodis/service"
	var prop serviceProps
	prop.SvcTypeRep = "colossusdb_rodis"
	data := utils.MarshalJsonOrDie(prop)
	ok := zkStore.Create(context.Background(), rodisServicePrefix+"/rodis_service_1", data)
	assert.Equal(t, ok, true)
	ok = zkStore.Create(context.Background(), rodisServicePrefix+"/rodis_service_2", data)
	assert.Equal(t, ok, true)
	time.Sleep(time.Second * 2)

	services = m.GetServices()
	assert.Equal(t, len(services), 2)
	url := m.GetUrlByService("rodis_service_1")
	assert.Assert(t, url == "http://test_host:1001")

	ans = zkStore.Delete(context.Background(), rodisLeaderPath)
	assert.Equal(t, ans, true)
	time.Sleep(time.Second * 2)

	services = m.GetServices()
	assert.Equal(t, len(services), 2)
	url = m.GetUrlByService("rodis_service_1")
	assert.Assert(t, url == "http://test_host:9999")
	watcher.Stop()
	time.Sleep(time.Second)
}
