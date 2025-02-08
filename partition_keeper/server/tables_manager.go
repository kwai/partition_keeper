package server

import (
	"context"
	"flag"
	"fmt"

	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/grpc"
)

const (
	kIdRecycleZkNode = "__RECYCLE_IDS__"
)

var (
	flagIdRecycleDuration = flag.Duration(
		"id_recycle_duration",
		7*24*time.Hour,
		"id recycle duration",
	)
	flagIdRecycleIntervalMinute = flag.Duration(
		"id_recycle_interval_minute",
		5*time.Minute,
		"id_recycle_interval_minute",
	)
	flagGlobalManagerService = flag.String(
		"global_manager_service_name",
		"grpc_testOnlyTablesManager",
		"global_manager_service_name",
	)
)

type tableManagerProperty struct {
	NextTableID int32 `json:"next_table_id"`
}

type TableRegistryInfo struct {
	TableId   int32
	BaseTable string
}

type TablesManager struct {
	mu sync.Mutex

	metaStore                metastore.MetaStore
	connPool                 *rpc.ConnPool
	prop                     tableManagerProperty
	globalManagerServiceName string
	zkPath                   string
	idRecyclePath            string
	groupByServices          map[string][]string
	tableNames               map[string]TableRegistryInfo
	tableIds                 map[int32]string
	// tableID -> deadline
	recyclingIds map[int32]recyclingIdContext
	quitRecycler chan bool
}

type recyclingIdContext struct {
	Deadline            int64 `json:"deadline"`
	CleanTaskSideEffect bool  `json:"clean_task_side_effect"`
	CleanExecuteTime    int64 `json:"clean_execute_time"`
}

func NewTablesManager(zkStore metastore.MetaStore, zkPath string) *TablesManager {
	ans := &TablesManager{
		metaStore: zkStore,
		connPool: rpc.NewRpcPool(func(conn *grpc.ClientConn) interface{} {
			return pb.NewTablesManagerClient(conn)
		}),
		globalManagerServiceName: *flagGlobalManagerService,
		zkPath:                   zkPath,
		idRecyclePath:            fmt.Sprintf("%s/%s", zkPath, kIdRecycleZkNode),
		groupByServices:          make(map[string][]string),
		tableNames:               make(map[string]TableRegistryInfo),
		tableIds:                 make(map[int32]string),
		recyclingIds:             make(map[int32]recyclingIdContext),
		quitRecycler:             make(chan bool),
	}
	return ans
}

func (t *TablesManager) WithGlobalManagerServiceName(serviceName string) {
	t.globalManagerServiceName = serviceName
}

func (t *TablesManager) allocateIdFromGlobalManager() (int32, *pb.ErrorStatus) {
	var stub pb.TablesManagerClient = nil
	var failHandler func() = nil

	if strings.HasPrefix(t.globalManagerServiceName, "host_") {
		ipPort := t.globalManagerServiceName[5:]
		rpcNode := utils.FromHostPort(ipPort)
		conn := t.connPool.Get(rpcNode)
		stub = pb.NewTablesManagerClient(conn)
		failHandler = func() {
			if err := conn.Close(); err != nil {
				logging.Error("close connection %s failed: %s", rpcNode.String(), err.Error())
			}
			t.connPool.Remove(rpcNode, conn)
		}
	} else {
		client, err := third_party.GetRpcClient(t.globalManagerServiceName)
		if err != nil {
			return -1, pb.AdminErrorMsg(
				pb.AdminError_kDependencyServiceError,
				"access %s error: %s",
				t.globalManagerServiceName,
				err.Error(),
			)
		}
		stub = pb.NewTablesManagerClient(client)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	resp, err := stub.AllocateId(ctx, &pb.AllocateIdRequest{})
	if err != nil {
		if failHandler != nil {
			failHandler()
		}
		return -1, pb.AdminErrorMsg(
			pb.AdminError_kDependencyServiceError,
			"access %s error: %s",
			t.globalManagerServiceName,
			err.Error(),
		)
	}
	if resp.Status.Code != int32(pb.TablesManagerError_kOk) {
		logging.Warning(
			"allocate table id from %s failed: %s",
			t.globalManagerServiceName,
			resp.Status.Message,
		)
		return -1, pb.AdminErrorMsg(
			pb.AdminError_kDependencyServiceError,
			"access %s error: %s",
			t.globalManagerServiceName,
			resp.Status.Message,
		)
	}
	logging.Assert(resp.Id > 0, "")
	return resp.Id, nil
}

func (t *TablesManager) addTable(
	tableId int32,
	tableName string,
	belongTo string,
	baseTable string,
) {
	serviceList := strings.Split(belongTo, ",")
	if len(serviceList) == 0 || len(serviceList) > 2 {
		logging.Fatal("table %s belong to service %s should be a list with len 1 or 2",
			tableName, belongTo)
	}
	last := serviceList[len(serviceList)-1]
	if last == "" {
		logging.Fatal("table %s belong to an empty service list", tableName)
	}
	t.groupByServices[last] = append(t.groupByServices[last], tableName)
	_, ok := t.tableNames[tableName]
	logging.Assert(!ok, "")
	logging.Assert(t.tableIds[tableId] == "", "")
	t.tableNames[tableName] = TableRegistryInfo{TableId: tableId, BaseTable: baseTable}
	t.tableIds[tableId] = tableName
}

func (t *TablesManager) loadOrInitMaxTableID() {
	data, exists, succ := t.metaStore.Get(context.Background(), t.zkPath)
	logging.Assert(succ, "")
	if exists {
		utils.UnmarshalJsonOrDie(data, &(t.prop))
	} else {
		t.prop.NextTableID = 1
		data = utils.MarshalJsonOrDie(&(t.prop))
		succ := t.metaStore.Create(context.Background(), t.zkPath, data)
		logging.Assert(succ, "")
	}
}

func (t *TablesManager) loadTables() {
	tableNames, exists, succ := t.metaStore.Children(context.Background(), t.zkPath)
	logging.Assert(exists && succ, "")
	for _, tbl := range tableNames {
		if tbl == kIdRecycleZkNode {
			// The __RECYCLE_IDS__ is loaded by loadOrInitIdRecycler
			continue
		}
		tablePath := t.GetTableZkPath(tbl)
		prop, exists, succ := t.metaStore.Get(context.Background(), tablePath)
		logging.Assert(exists && succ, "")

		tableDesc := pb.Table{}
		utils.UnmarshalJsonOrDie(prop, &tableDesc)
		t.addTable(
			tableDesc.TableId,
			tableDesc.TableName,
			tableDesc.BelongToService,
			tableDesc.BaseTable,
		)
	}
}

func (t *TablesManager) loadOrInitIdRecycler() {
	ids, exists, succ := t.metaStore.Children(context.Background(), t.idRecyclePath)
	logging.Assert(succ, "read %s failed", t.idRecyclePath)
	if exists {
		for _, id := range ids {
			tableId, err := strconv.Atoi(id)
			logging.Assert(err == nil, "convert %s to int failed: %v", id, err)

			bytes, exists, succ := t.metaStore.Get(
				context.Background(),
				t.getTableIdRecyclePath(int32(tableId)),
			)
			logging.Assert(exists && succ, "")
			context := recyclingIdContext{}
			utils.UnmarshalJsonOrDie(bytes, &context)
			t.recyclingIds[int32(tableId)] = context
		}
	} else {
		if !t.metaStore.Create(context.Background(), t.idRecyclePath, []byte{}) {
			logging.Fatal("create %s failed", t.idRecyclePath)
		}
	}
	go t.scheduleRecycleIds()
}

func (t *TablesManager) doIdRecycle() {
	var deletingIds []int32 = nil
	t.mu.Lock()
	now := time.Now().Unix()

	for id, context := range t.recyclingIds {
		if context.Deadline <= now && t.tableIds[id] == "" {
			deletingIds = append(deletingIds, id)
			delete(t.recyclingIds, id)
		}
	}
	t.mu.Unlock()
	for _, id := range deletingIds {
		path := t.getTableIdRecyclePath(id)
		if !t.metaStore.Delete(context.Background(), path) {
			logging.Warning("delete %s failed", path)
		}
	}
}

func (t *TablesManager) scheduleRecycleIds() {
	tick := time.NewTicker(*flagIdRecycleIntervalMinute)
	for {
		select {
		case <-tick.C:
			t.doIdRecycle()
		case <-t.quitRecycler:
			tick.Stop()
			return
		}
	}
}

func (t *TablesManager) InitFromZookeeper() {
	t.loadOrInitMaxTableID()
	t.loadTables()
	t.loadOrInitIdRecycler()
}

func (t *TablesManager) Stop() {
	logging.Info("start to stop tables manager")
	t.quitRecycler <- true
	logging.Info("tables managers has stopped")
}

func (t *TablesManager) GetZkPath() string {
	return t.zkPath
}

func (t *TablesManager) getTableIdRecyclePath(tableId int32) string {
	return fmt.Sprintf("%s/%d", t.idRecyclePath, tableId)
}

func (t *TablesManager) GetTableZkPath(tableName string) string {
	return fmt.Sprintf("%s/%s", t.zkPath, tableName)
}

func (t *TablesManager) GetRecyclingIdContext(tableId int32) (bool, int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	IdContext, ok := t.recyclingIds[tableId]
	if !ok {
		logging.Info("Fail to get table recycling context, table:%d not exist", tableId)
		return false, 0
	}
	return IdContext.CleanTaskSideEffect, IdContext.CleanExecuteTime
}

func (t *TablesManager) RegisterTable(
	serviceName, tableName string,
	baseTable string,
) (int32, *pb.ErrorStatus) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if info, ok := t.tableNames[tableName]; ok {
		return -1, pb.AdminErrorMsg(pb.AdminError_kTableExists,
			"table %s already exists with id %d", tableName, info.TableId)
	} else {
		ans, err := t.allocateIdFromGlobalManager()
		if err != nil {
			return -1, err
		}
		t.tableIds[ans] = tableName
		t.tableNames[tableName] = TableRegistryInfo{TableId: ans, BaseTable: baseTable}
		t.groupByServices[serviceName] = append(t.groupByServices[serviceName], tableName)
		logging.Info("register table %s for service %s with id %d", tableName, serviceName, ans)

		// when we downgrade service, at least we can make sure we don't allocate
		// duplicate table id for the same partition-keeper server
		t.prop.NextTableID = ans + 1
		data := utils.MarshalJsonOrDie(&(t.prop))
		succ := t.metaStore.Set(context.Background(), t.zkPath, data)
		logging.Assert(succ, "")
		return ans, pb.ErrStatusOk()
	}
}

func (t *TablesManager) StartUnregisterTable(
	tableId int32,
	cleanTaskSideEffect bool,
	cleanExecuteTime int64,
) *pb.ErrorStatus {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.recyclingIds[tableId]; ok {
		return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, "table %d is in deleting", tableId)
	}

	deadline := time.Now().Add(*flagIdRecycleDuration).Unix()
	idContext := recyclingIdContext{
		Deadline:            deadline,
		CleanTaskSideEffect: cleanTaskSideEffect,
		CleanExecuteTime:    cleanExecuteTime,
	}
	data := utils.MarshalJsonOrDie(&idContext)
	succ := t.metaStore.Create(context.Background(), t.getTableIdRecyclePath(tableId), data)
	logging.Assert(succ, "")
	t.recyclingIds[tableId] = idContext
	return nil
}

func (t *TablesManager) FinishUnregisterTable(tableId int32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	name := t.tableIds[tableId]
	delete(t.tableNames, name)
	delete(t.tableIds, tableId)
	for serviceName, tables := range t.groupByServices {
		filtered := tables[:0]
		for _, table := range tables {
			if table != name {
				filtered = append(filtered, table)
			}
		}
		if len(filtered) == 0 {
			delete(t.groupByServices, serviceName)
		} else {
			t.groupByServices[serviceName] = filtered
		}
	}
}

func (t *TablesManager) IsUnregistered(tableId int32) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	_, ok := t.recyclingIds[tableId]
	return ok
}

func (t *TablesManager) CleanService(serviceName string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	tables := t.groupByServices[serviceName]
	for _, table := range tables {
		tableInfo, ok := t.tableNames[table]
		logging.Assert(ok, "")
		delete(t.tableNames, table)
		delete(t.tableIds, tableInfo.TableId)
	}
	delete(t.groupByServices, serviceName)
}

func (t *TablesManager) GetTablesByService(serviceName string) map[string]TableRegistryInfo {
	t.mu.Lock()
	defer t.mu.Unlock()

	names := t.groupByServices[serviceName]
	output := map[string]TableRegistryInfo{}
	for _, name := range names {
		output[name] = t.tableNames[name]
	}
	return output
}
