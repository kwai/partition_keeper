package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"gotest.tools/assert"
)

type testTablesManagerEnv struct {
	t       *testing.T
	zkStore metastore.MetaStore
	zkPath  string
}

func setupTestTablesManagerEnv(t *testing.T) *testTablesManagerEnv {
	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	ans := &testTablesManagerEnv{
		t: t,
		zkStore: metastore.CreateZookeeperStore(
			[]string{"127.0.0.1:2181"},
			time.Second*10,
			acl,
			scheme,
			auth,
		),
		zkPath: utils.SpliceZkRootPath("/test/pk/tables"),
	}

	ans.zkStore.RecursiveDelete(context.Background(), ans.zkPath)
	return ans
}

func (te *testTablesManagerEnv) teardown() {
	ans := te.zkStore.RecursiveDelete(
		context.Background(),
		te.zkPath,
	)
	assert.Assert(te.t, ans)
	te.zkStore.Close()
}

func (te *testTablesManagerEnv) createTable(desc *pb.Table) {
	tablePath := fmt.Sprintf("%s/%s", te.zkPath, desc.TableName)
	data := utils.MarshalJsonOrDie(desc)
	succ := te.zkStore.Create(context.Background(), tablePath, data)
	assert.Assert(te.t, succ)
}

func TestTablesManagerByHost(t *testing.T) {
	te := setupTestTablesManagerEnv(t)
	defer te.teardown()

	tm := NewTablesManager(te.zkStore, te.zkPath)
	tm.InitFromZookeeper()
	tm.WithGlobalManagerServiceName("host_127.0.0.1:20030")

	_, err := tm.RegisterTable("test_service", "test_table", "")
	assert.Assert(t, err.Code != 0)

	tm.WithGlobalManagerServiceName(
		"host_public-xm-c-kce-node518.idchb1az1.hb1.kwaidc.com:21155",
	)
	ans, err := tm.RegisterTable("test_service", "test_table", "")
	assert.Equal(t, err.Code, int32(0))
	assert.Equal(t, ans+1, tm.prop.NextTableID)
}

func TestTablesManager(t *testing.T) {
	te := setupTestTablesManagerEnv(t)
	defer te.teardown()

	tm := NewTablesManager(te.zkStore, te.zkPath)
	tm.InitFromZookeeper()

	tbl1 := &pb.Table{
		TableId:         1,
		TableName:       "a",
		HashMethod:      "crc32",
		PartsCount:      4,
		JsonArgs:        `{"a":"b"}`,
		BelongToService: "service1",
		KconfPath:       "/reco/embedding",
	}
	te.createTable(tbl1)

	tbl2 := &pb.Table{
		TableId:         2,
		TableName:       "b",
		HashMethod:      "crc32",
		PartsCount:      4,
		JsonArgs:        `{"a":"b"}`,
		BelongToService: "service2",
		KconfPath:       "/reco/embedding",
	}
	te.createTable(tbl2)

	tbl3 := &pb.Table{
		TableId:         3,
		TableName:       "c",
		HashMethod:      "crc32",
		PartsCount:      4,
		JsonArgs:        `{"a":"b"}`,
		BelongToService: "service2,service1",
		KconfPath:       "/reco/embedding",
	}
	te.createTable(tbl3)

	tbl4 := &pb.Table{
		TableId:         4,
		TableName:       "d",
		HashMethod:      "crc32",
		PartsCount:      4,
		JsonArgs:        `{"a":"b"}`,
		BelongToService: "service1,service2",
		KconfPath:       "/reco/embedding",
	}
	te.createTable(tbl4)

	tbl5 := &pb.Table{
		TableId:         5,
		TableName:       "e",
		HashMethod:      "crc32",
		PartsCount:      4,
		JsonArgs:        `{"a":"b"}`,
		BelongToService: "service1",
		KconfPath:       "/reco/embedding",
		BaseTable:       "a",
	}
	te.createTable(tbl5)

	tm2 := NewTablesManager(te.zkStore, te.zkPath)
	tm2.InitFromZookeeper()

	assert.Equal(t, len(tm2.groupByServices), 2)
	service1List := tm2.GetTablesByService("service1")
	service2List := tm2.GetTablesByService("service2")

	assert.DeepEqual(
		t,
		service1List,
		map[string]TableRegistryInfo{"a": {1, ""}, "c": {3, ""}, "e": {5, "a"}},
	)
	assert.DeepEqual(t, service2List, map[string]TableRegistryInfo{"b": {2, ""}, "d": {4, ""}})
	assert.Equal(t, len(tm2.GetTablesByService("service3")), 0)

	assert.DeepEqual(t, tm2.tableNames, map[string]TableRegistryInfo{
		"a": {1, ""},
		"b": {2, ""},
		"c": {3, ""},
		"d": {4, ""},
		"e": {5, "a"},
	})
	assert.DeepEqual(t, tm2.tableIds, map[int32]string{
		1: "a",
		2: "b",
		3: "c",
		4: "d",
		5: "e",
	})

	assert.Equal(t, int32(1), tm2.prop.NextTableID)

	ans, err := tm2.RegisterTable("service3", "a", "")
	assert.Equal(t, int32(-1), ans)
	assert.Equal(t, err.Code, int32(pb.AdminError_kTableExists))

	tm2.CleanService("service1")

	tm2.prop.NextTableID = 5
	ans, err = tm2.RegisterTable("service3", "a", "")
	assert.Equal(t, ans+1, tm2.prop.NextTableID)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))

	ans, err = tm2.RegisterTable("service4", "b", "")
	assert.Equal(t, int32(-1), ans)
	assert.Equal(t, err.Code, int32(pb.AdminError_kTableExists))

	tm3 := NewTablesManager(te.zkStore, te.zkPath)
	tm3.InitFromZookeeper()
	assert.Equal(t, tm3.prop.NextTableID, tm2.prop.NextTableID)
}

func TestUnregisterTable(t *testing.T) {
	te := setupTestTablesManagerEnv(t)
	defer te.teardown()

	tm := NewTablesManager(te.zkStore, te.zkPath)
	tm.InitFromZookeeper()

	tbl1 := &pb.Table{
		TableId:         1,
		TableName:       "a",
		HashMethod:      "crc32",
		PartsCount:      4,
		JsonArgs:        `{"a":"b"}`,
		BelongToService: "service1",
		KconfPath:       "/reco/embedding",
	}
	te.createTable(tbl1)

	tbl2 := &pb.Table{
		TableId:         2,
		TableName:       "b",
		HashMethod:      "crc32",
		PartsCount:      4,
		JsonArgs:        `{"a":"b"}`,
		BelongToService: "service2",
		KconfPath:       "/reco/embedding",
	}
	te.createTable(tbl2)

	tbl3 := &pb.Table{
		TableId:         3,
		TableName:       "c",
		HashMethod:      "crc32",
		PartsCount:      4,
		JsonArgs:        `{"a":"b"}`,
		BelongToService: "service2,service1",
		KconfPath:       "/reco/embedding",
	}
	te.createTable(tbl3)

	tm2 := NewTablesManager(te.zkStore, te.zkPath)
	tm2.InitFromZookeeper()

	assert.Equal(t, len(tm2.groupByServices), 2)
	service1List := tm2.GetTablesByService("service1")
	service2List := tm2.GetTablesByService("service2")

	assert.DeepEqual(t, service1List, map[string]TableRegistryInfo{"a": {1, ""}, "c": {3, ""}})
	assert.DeepEqual(t, service2List, map[string]TableRegistryInfo{"b": {2, ""}})

	err := tm2.StartUnregisterTable(1, false, 0)
	assert.Assert(t, err == nil)

	err = tm2.StartUnregisterTable(1, false, 0)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))

	assert.Assert(t, tm2.tableNames["a"].TableId == 1)
	assert.Assert(t, tm2.tableIds[1] == "a")

	tm2.FinishUnregisterTable(1)
	_, ok := tm2.tableNames["a"]
	assert.Assert(t, !ok)
	assert.Assert(t, tm2.tableIds[1] == "")
	service1List = tm2.GetTablesByService("service1")
	assert.DeepEqual(t, service1List, map[string]TableRegistryInfo{"c": {3, ""}})
}
