package meta

import (
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"gotest.tools/assert"
)

func TestInit(t *testing.T) {
	*FlagKconfPath = "reco.partitionKeeper.proxy_test"
	meta := NewMeta()
	meta.Initialize()
	time.Sleep(time.Second)
	assert.Equal(t, len(meta.namespaces), 3)
	assert.Equal(t, len(meta.types), 4)
}

func TestNamespace(t *testing.T) {
	meta := NewMeta()
	namespace1 := "invalid_namespace"
	ok := meta.addNamespace(namespace1)
	assert.Equal(t, ok, false)

	namespace1 = "invalid_namespace:"
	ok = meta.addNamespace(namespace1)
	assert.Equal(t, ok, false)

	namespace2 := "colossusdb_invalid:test_rodis"
	ok = meta.addNamespace(namespace2)
	assert.Equal(t, ok, false)

	namespace3 := "colossusdb_rodis:test_rodis,test_rodis2"
	ok = meta.addNamespace(namespace3)
	assert.Equal(t, ok, true)
	assert.Equal(t, len(meta.namespaces), 2)
	assert.Equal(t, len(meta.types), 1)
	namespaces := meta.GetNamespaces()
	assert.Equal(t, len(namespaces), 2)
	assert.Assert(t, namespaces[0] == "test_rodis" || namespaces[0] == "test_rodis2")

	namespace4 := "colossusdb_embedding_server:test_default"
	ok = meta.addNamespace(namespace4)
	assert.Equal(t, ok, true)
	assert.Equal(t, len(meta.namespaces), 3)
	assert.Equal(t, len(meta.types), 2)
}

func TestService(t *testing.T) {
	meta := NewMeta()
	namespace1 := "colossusdb_embedding_server:test_default"
	ok := meta.addNamespace(namespace1)
	assert.Equal(t, ok, true)
	namespace2 := "colossusdb_rodis:test_rodis,test_rodis2"
	ok = meta.addNamespace(namespace2)
	assert.Equal(t, ok, true)
	meta.grayNamespace = "test_rodis2"

	host1 := "test_host:0000"
	meta.HostChange(host1, "test_rodis")
	assert.Equal(t, ok, true)

	host2 := "test_host:0001"
	meta.HostChange(host2, "test_rodis2")
	assert.Equal(t, ok, true)

	host3 := "test_host:0002"
	meta.HostChange(host3, "test_default")
	assert.Equal(t, ok, true)

	urls := meta.GetAllUrls()
	assert.Equal(t, len(urls), 3)

	url := meta.GetUrlByType(pb.ServiceType_colossusdb_rodis)
	assert.Assert(t, url == "http://test_host:0001") // gray namespace

	meta.AddServices(
		map[string]pb.ServiceType{},
		"test_default",
	)
	assert.Equal(t, len(meta.services), 0)

	meta.AddServices(
		map[string]pb.ServiceType{"rodis_invalid_type": pb.ServiceType_invalid},
		"test_default",
	)
	assert.Equal(t, len(meta.services), 1)

	meta.AddServices(
		map[string]pb.ServiceType{"rodis_service_1": pb.ServiceType_colossusdb_rodis},
		"test_rodis",
	)
	assert.Equal(t, len(meta.services), 2)
	url = meta.GetUrlByService("rodis_service_1")
	assert.Assert(t, url == "http://test_host:0000")

	url = meta.GetUrlByType(pb.ServiceType_colossusdb_rodis)
	assert.Assert(t, url == "http://test_host:0001")

	meta.AddServices(
		map[string]pb.ServiceType{"rodis_service_2": pb.ServiceType_colossusdb_rodis},
		"test_rodis2",
	)
	assert.Equal(t, len(meta.services), 3)

	services := meta.GetServices()
	assert.Equal(t, len(services), 3)
	url = meta.GetUrlByType(pb.ServiceType_colossusdb_rodis)
	assert.Assert(t, url == "http://test_host:0000")

	meta.DelServices(
		map[string]pb.ServiceType{"rodis_service_1": pb.ServiceType_colossusdb_rodis},
		"test_rodis",
	)
	assert.Equal(t, len(meta.services), 2)
	url = meta.GetUrlByService("rodis_service_1")
	assert.Assert(t, url == "")

	url = meta.GetUrlByType(pb.ServiceType_colossusdb_rodis)
	assert.Assert(t, url == "http://test_host:0000")

	meta.DelServices(
		map[string]pb.ServiceType{"rodis_service_2": pb.ServiceType_colossusdb_rodis},
		"test_rodis2",
	)
	assert.Equal(t, len(meta.services), 1)
	url = meta.GetUrlByService("rodis_service_2")
	assert.Assert(t, url == "")

	url = meta.GetUrlByType(pb.ServiceType_colossusdb_rodis)
	assert.Assert(t, url == "http://test_host:0001")

	meta.DelServices(
		map[string]pb.ServiceType{"rodis_invalid_type": pb.ServiceType_invalid},
		"test_default",
	)
	assert.Equal(t, len(meta.services), 0)
}

func TestTable(t *testing.T) {
	meta := NewMeta()
	tables := []string{"table1", "table2"}
	meta.AddTables(tables)
	assert.Equal(t, len(meta.tables), 2)
	meta.AddTables([]string{})
	assert.Equal(t, len(meta.tables), 2)

	assert.Equal(t, meta.TableExist("table1"), true)
	assert.Equal(t, meta.TableExist("table3"), false)
	meta.DelTables(tables)
	assert.Equal(t, len(meta.tables), 0)
	meta.DelTables([]string{})
	assert.Equal(t, len(meta.tables), 0)
	assert.Equal(t, meta.TableExist("table1"), false)
}
