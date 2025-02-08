package node_mgr

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/est"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
)

type nodeStatTestEnv struct {
	t         *testing.T
	zkPrefix  string
	zkStore   metastore.MetaStore
	lock      *utils.LooseLock
	nodeStats *NodeStats
}

func createHubsMapByName(hubs []string) *utils.HubHandle {
	output := utils.NewHubHandle()
	for _, hub := range hubs {
		output.AddHub(&pb.ReplicaHub{Name: hub, Az: utils.KrpAzToStandard(hub[0 : len(hub)-1])})
	}
	return output
}

func setupNodeStatTestEnv(
	t *testing.T,
	hubs map[string]string,
	fdType pb.NodeFailureDomainType,
) *nodeStatTestEnv {
	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	ans := &nodeStatTestEnv{
		t:        t,
		zkPrefix: utils.SpliceZkRootPath("/test/pk/nodes"),
		zkStore: metastore.CreateZookeeperStore(
			[]string{"127.0.0.1:2181"}, time.Second*10, acl, scheme, auth,
		),
		lock: utils.NewLooseLock(),
	}

	assert.Assert(t, ans.zkStore.RecursiveDelete(context.Background(), ans.zkPrefix))
	assert.Assert(t, ans.zkStore.RecursiveCreate(context.Background(), ans.zkPrefix+"/nodes"))
	assert.Assert(t, ans.zkStore.RecursiveCreate(context.Background(), ans.zkPrefix+"/hints"))

	ans.nodeStats = NewNodeStats(
		"test",
		ans.lock,
		ans.zkPrefix+"/nodes",
		ans.zkPrefix+"/hints",
		ans.zkStore,
	).WithFailureDomainType(fdType)

	hubmap := utils.NewHubHandle()
	for name, az := range hubs {
		hubmap.AddHub(&pb.ReplicaHub{Name: name, Az: az})
	}
	ans.nodeStats.hubmap = hubmap
	return ans
}

func (te *nodeStatTestEnv) teardown() {
	assert.Assert(te.t, te.zkStore.RecursiveDelete(context.Background(), te.zkPrefix))
	te.zkStore.Close()
}

func (te *nodeStatTestEnv) checkZkSynced() {
	stats2 := NewNodeStats(
		"test",
		utils.NewLooseLock(),
		te.zkPrefix+"/nodes",
		te.zkPrefix+"/hints",
		te.zkStore,
	)
	stats2.loadNodes()
	assert.DeepEqual(te.t, stats2.idNodes, te.nodeStats.idNodes)
	for _, node := range te.nodeStats.idNodes {
		stats2.MustGetNodeInfo(node.Id).resource = node.resource
	}
	assert.Assert(te.t, utils.TreemapEqual(stats2.addrNodes.nodes, te.nodeStats.addrNodes.nodes))

	for _, info := range te.nodeStats.idNodes {
		info2 := te.nodeStats.addrNodes.get(info.Address, info.Id)
		// idNodes & addrNodes are same info item
		assert.Equal(te.t, info, info2)
	}

	stats2.loadHints()
	assert.Equal(te.t, len(stats2.hints), len(te.nodeStats.hints))
	for hp, hint := range stats2.hints {
		assert.Assert(te.t, proto.Equal(hint, te.nodeStats.hints[hp]))
	}
}

func (te *nodeStatTestEnv) checkNodeProperlyOrdered() {
	slots := make([]string, len(te.nodeStats.idNodes))
	for id, index := range te.nodeStats.nodeOrder {
		assert.Assert(te.t, index < len(slots))
		slots[index] = id
	}

	if len(slots) <= 0 {
		return
	}
	assert.Assert(te.t, len(slots[0]) > 0)
	for i := 1; i < len(slots); i++ {
		prev, next := slots[i-1], slots[i]
		assert.Assert(te.t, len(next) > 0)
		info1 := te.nodeStats.MustGetNodeInfo(prev)
		info2 := te.nodeStats.MustGetNodeInfo(next)
		if info1.Hub != info2.Hub {
			assert.Assert(te.t, info1.Hub < info2.Hub)
		} else {
			assert.Assert(te.t, prev < next)
		}
	}
}

func checkStringSameItems(t *testing.T, left []string, right []string) {
	sort.Strings(left)
	sort.Strings(right)
	assert.DeepEqual(t, left, right)
}

func TestNodeStats(t *testing.T) {
	te := setupNodeStatTestEnv(
		t,
		map[string]string{"yz1": "YZ", "yz2": "YZ", "yz3": "YZ"},
		pb.NodeFailureDomainType_PROCESS,
	)
	defer te.teardown()

	//add new node
	pingList1 := map[string]*NodePing{
		"node1": {true, "YZ", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
		"node2": {true, "YZ", utils.FromHostPort("127.0.0.1:1002"), "12342", 2002, ""},
		"node3": {true, "YZ", utils.FromHostPort("127.0.0.1:1003"), "12343", 2003, ""},
		"node4": {true, "YZ", utils.FromHostPort("127.0.0.1:1004"), "12344", 2004, ""},
		"node5": {true, "YZ", utils.FromHostPort("127.0.0.1:1005"), "12345", 2005, ""},
		"node6": {true, "YZ", utils.FromHostPort("127.0.0.1:1006"), "12346", 2006, ""},
	}
	te.lock.LockWrite()
	defer te.lock.UnlockWrite()
	te.nodeStats.UpdateStats(pingList1)
	te.checkZkSynced()
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"yz1": 2, "yz2": 2, "yz3": 2})
	te.checkNodeProperlyOrdered()

	// check node info getter
	nodesPing := te.nodeStats.GetNodeLivenessMap(false)
	assert.DeepEqual(t, pingList1, nodesPing)

	for id, ping := range pingList1 {
		node := te.nodeStats.GetNodeInfo(id)
		assert.Assert(t, node != nil)
		assert.Assert(t, node.Normal())
		assert.Equal(t, node.Id, id)
		assert.Equal(t, node.Op, pb.AdminNodeOp_kNoop)
		assert.DeepEqual(t, node.Address, ping.Address)
		assert.Equal(t, node.IsAlive, ping.IsAlive)
		assert.Equal(t, node.Az, ping.Az)
		assert.Equal(t, node.ProcessId, ping.ProcessId)
		assert.Equal(t, node.Weight, DEFAULT_WEIGHT)
		assert.Equal(t, node.Score, est.INVALID_SCORE)
		assert.Equal(t, node.WeightedScore(), INVALID_WEIGHTED_SCORE)
		assert.Assert(t, node.Hub == "yz1" || node.Hub == "yz2" || node.Hub == "yz3")
	}

	node := te.nodeStats.GetNodeInfo("node7")
	assert.Assert(t, is.Nil(node))

	nodes := te.nodeStats.GetNodeInfoByAddrs(
		[]*utils.RpcNode{
			utils.FromHostPort("127.0.0.1:1001"),
			utils.FromHostPort("127.0.0.1:1002"),
			utils.FromHostPort("127.0.0.1:1008"),
		},
		true,
	)
	assert.Equal(t, nodes[0].Id, te.nodeStats.MustGetNodeInfo("node1").Id)
	assert.Equal(t, nodes[1].Id, te.nodeStats.MustGetNodeInfo("node2").Id)
	assert.Assert(t, is.Nil(nodes[2]))

	// mark node dead
	pingList2 := map[string]*NodePing{
		"node1": {false, "YZ", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
		"node2": {false, "YZ", utils.FromHostPort("127.0.0.1:1002"), "22342", 2002, ""},
	}
	pingList1["node1"].IsAlive = false
	pingList1["node2"].IsAlive = false
	te.nodeStats.UpdateStats(pingList2)
	te.checkZkSynced()
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"yz1": 2, "yz2": 2, "yz3": 2})
	te.checkNodeProperlyOrdered()

	for id, ping := range pingList1 {
		node := te.nodeStats.GetNodeInfo(id)
		assert.Assert(t, node != nil)
		assert.Equal(t, node.Id, id)
		assert.Equal(t, node.Op, pb.AdminNodeOp_kNoop)
		assert.DeepEqual(t, node.Address, ping.Address)
		assert.Equal(t, node.IsAlive, ping.IsAlive)
		assert.Equal(t, node.Az, ping.Az)
		assert.Equal(t, node.ProcessId, ping.ProcessId)
		assert.Assert(t, node.Hub == "yz1" || node.Hub == "yz2" || node.Hub == "yz3")
	}

	// mark some node alive again
	pingList3 := map[string]*NodePing{
		"node1": {true, "YZ", utils.FromHostPort("127.0.0.1:1001"), "22341", 2001, ""},
		"node2": {true, "YZ", utils.FromHostPort("127.0.0.1:1002"), "22342", 2002, ""},
	}
	pingList1["node1"].IsAlive = true
	pingList1["node1"].ProcessId = "22341"
	pingList1["node2"].IsAlive = true
	pingList1["node2"].ProcessId = "22342"
	te.nodeStats.UpdateStats(pingList3)
	te.checkZkSynced()
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"yz1": 2, "yz2": 2, "yz3": 2})
	te.checkNodeProperlyOrdered()

	for id, ping := range pingList1 {
		node := te.nodeStats.GetNodeInfo(id)
		assert.Assert(t, node != nil)
		assert.Equal(t, node.Id, id)
		assert.Equal(t, node.Op, pb.AdminNodeOp_kNoop)
		assert.DeepEqual(t, node.Address, ping.Address)
		assert.Equal(t, node.IsAlive, ping.IsAlive)
		assert.Equal(t, node.Az, ping.Az)
		assert.Equal(t, node.ProcessId, ping.ProcessId)
		assert.Assert(t, node.Hub == "yz1" || node.Hub == "yz2" || node.Hub == "yz3")
	}

	// mark nodes as restarting
	currentNodes := map[string]*NodeInfo{}
	utils.GobClone(&currentNodes, te.nodeStats.AllNodes())

	results := te.nodeStats.AdminNodes(
		utils.FromHostPorts([]string{
			"127.0.0.1:1003",
			"127.0.0.1:1004",
			"127.0.0.1:1005",
			"127.0.0.1:1020",
		}),
		true, pb.AdminNodeOp_kRestart, nil)
	te.checkZkSynced()
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"yz1": 2, "yz2": 2, "yz3": 2})
	te.checkNodeProperlyOrdered()

	assert.Equal(t, len(results), 4)
	for _, err := range results[0:3] {
		assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	}
	assert.Equal(t, results[3].Code, int32(pb.AdminError_kNodeNotExisting))

	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node3").Op, pb.AdminNodeOp_kRestart)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node4").Op, pb.AdminNodeOp_kRestart)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node5").Op, pb.AdminNodeOp_kRestart)

	assert.Equal(t, currentNodes["node3"].Op, pb.AdminNodeOp_kNoop)
	assert.Equal(t, currentNodes["node4"].Op, pb.AdminNodeOp_kNoop)
	assert.Equal(t, currentNodes["node5"].Op, pb.AdminNodeOp_kNoop)

	currentNodes["node3"].Op = pb.AdminNodeOp_kRestart
	currentNodes["node4"].Op = pb.AdminNodeOp_kRestart
	currentNodes["node5"].Op = pb.AdminNodeOp_kRestart

	assert.DeepEqual(t, currentNodes, te.nodeStats.AllNodes())

	// 1001: noop -> noop, 1005: restart -> noop, 1020: not-exist
	results = te.nodeStats.AdminNodes(
		utils.FromHostPorts([]string{
			"127.0.0.1:1001",
			"127.0.0.1:1005",
			"127.0.0.1:1020"}),
		true, pb.AdminNodeOp_kNoop, nil)
	te.checkZkSynced()
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"yz1": 2, "yz2": 2, "yz3": 2})
	te.checkNodeProperlyOrdered()

	assert.Assert(t, is.Len(results, 3))
	codes := []pb.AdminError_Code{
		pb.AdminError_kDuplicateRequest,
		pb.AdminError_kOk,
		pb.AdminError_kNodeNotExisting,
	}
	for i, res := range results {
		assert.Equal(t, res.Code, int32(codes[i]))
	}

	currentNodes["node5"].Op = pb.AdminNodeOp_kNoop
	assert.DeepEqual(t, currentNodes, te.nodeStats.AllNodes())

	// 1003: restarting -> offline, 1005: noop -> offline, 1006: noop -> offline
	results = te.nodeStats.AdminNodes(
		utils.FromHostPorts([]string{
			"127.0.0.1:1003",
			"127.0.0.1:1005",
			"127.0.0.1:1006",
		}),
		true, pb.AdminNodeOp_kOffline, nil)
	te.checkZkSynced()
	newHubsize := map[string]int{"yz1": 2, "yz2": 2, "yz3": 2}
	newHubsize[te.nodeStats.MustGetNodeInfo("node3").Hub]--
	newHubsize[te.nodeStats.MustGetNodeInfo("node5").Hub]--
	newHubsize[te.nodeStats.MustGetNodeInfo("node6").Hub]--
	for hubid, count := range newHubsize {
		if count == 0 {
			delete(newHubsize, hubid)
		}
	}

	assert.DeepEqual(t, te.nodeStats.hubSize, newHubsize)
	assert.Assert(t, is.Len(results, 3))
	codes = []pb.AdminError_Code{
		pb.AdminError_kOk,
		pb.AdminError_kOk,
		pb.AdminError_kOk,
	}
	for i, res := range results {
		assert.Equal(t, res.Code, int32(codes[i]))
	}

	currentNodes["node3"].Op = pb.AdminNodeOp_kOffline
	currentNodes["node5"].Op = pb.AdminNodeOp_kOffline
	currentNodes["node6"].Op = pb.AdminNodeOp_kOffline

	assert.DeepEqual(t, currentNodes, te.nodeStats.AllNodes())

	// 1001: noop -> noop, 1002: noop -> noop, 1007: not-exist
	results = te.nodeStats.AdminNodes(
		utils.FromHostPorts([]string{
			"127.0.0.1:1001",
			"127.0.0.1:1002",
			"127.0.0.1:1007",
		}),
		true, pb.AdminNodeOp_kNoop, nil)
	te.checkZkSynced()
	assert.DeepEqual(t, te.nodeStats.hubSize, newHubsize)
	te.checkNodeProperlyOrdered()

	assert.Assert(t, is.Len(results, 3))
	codes = []pb.AdminError_Code{
		pb.AdminError_kDuplicateRequest,
		pb.AdminError_kDuplicateRequest,
		pb.AdminError_kNodeNotExisting,
	}
	for i, res := range results {
		assert.Equal(t, res.Code, int32(codes[i]))
	}

	// 1003: offline -> restarting(failed)
	results = te.nodeStats.AdminNodes(
		utils.FromHostPorts([]string{"127.0.0.1:1003"}),
		true,
		pb.AdminNodeOp_kRestart,
		nil,
	)
	te.checkZkSynced()
	assert.DeepEqual(t, te.nodeStats.hubSize, newHubsize)
	te.checkNodeProperlyOrdered()

	assert.Assert(t, is.Len(results, 1))
	assert.Equal(t, int32(pb.AdminError_kInvalidParameter), results[0].Code)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node3").Op, pb.AdminNodeOp_kOffline)

	// 1003: first cancel offline, then mark restarting
	newHubsize[te.nodeStats.MustGetNodeInfo("node3").Hub]++
	results = te.nodeStats.AdminNodes(
		utils.FromHostPorts([]string{"127.0.0.1:1003"}),
		true,
		pb.AdminNodeOp_kNoop,
		nil,
	)
	te.checkZkSynced()
	assert.DeepEqual(t, te.nodeStats.hubSize, newHubsize)
	assert.Equal(t, int32(pb.AdminError_kOk), results[0].Code)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node3").Op, pb.AdminNodeOp_kNoop)

	results = te.nodeStats.AdminNodes(
		utils.FromHostPorts([]string{"127.0.0.1:1003"}),
		true,
		pb.AdminNodeOp_kRestart,
		nil,
	)
	te.checkZkSynced()
	assert.DeepEqual(t, te.nodeStats.hubSize, newHubsize)
	assert.Equal(t, int32(pb.AdminError_kOk), results[0].Code)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node3").Op, pb.AdminNodeOp_kRestart)

	currentNodes["node3"].Op = pb.AdminNodeOp_kRestart

	// 1001: noop, alive
	// 1002: noop, dead
	// 1003: restarting, alive
	// 1004: restarting, dead
	// 1005: offline, alive
	// 1006: offline, dead
	pingList4 := map[string]*NodePing{
		"node2": {false, "YZ", utils.FromHostPort("127.0.0.1:1002"), "22342", 2002, ""},
		"node4": {false, "YZ", utils.FromHostPort("127.0.0.1:1004"), "22344", 2004, ""},
		"node6": {false, "YZ", utils.FromHostPort("127.0.0.1:1006"), "22346", 2006, ""},
	}
	te.nodeStats.UpdateStats(pingList4)
	te.checkZkSynced()
	assert.DeepEqual(t, te.nodeStats.hubSize, newHubsize)
	te.checkNodeProperlyOrdered()

	currentNodes["node2"].IsAlive = false
	currentNodes["node4"].IsAlive = false
	currentNodes["node6"].IsAlive = false
	assert.DeepEqual(t, currentNodes, te.nodeStats.AllNodes())

	// test all filters
	nodeList := te.nodeStats.FilterNodes(GetAliveNode)
	checkStringSameItems(t, nodeList, []string{"node1", "node3", "node5"})

	nodeList = te.nodeStats.FilterNodes(GetDeadNode)
	checkStringSameItems(t, nodeList, []string{"node2", "node4", "node6"})

	nodeList = te.nodeStats.FilterNodes(GetNodeForHub("yz2"))
	nodesByHubs := te.nodeStats.DivideByHubs(false)
	dividedList := []string{}
	for _, info := range nodesByHubs["yz2"] {
		dividedList = append(dividedList, info.Id)
	}
	checkStringSameItems(t, nodeList, dividedList)

	fetchByHubs := te.nodeStats.fetchHubs([]string{"yz2"}, false)
	fetchList := []string{}
	for _, info := range fetchByHubs["yz2"] {
		fetchList = append(fetchList, info.Id)
	}
	checkStringSameItems(t, nodeList, fetchList)

	nodeList = te.nodeStats.FilterNodes(GetNodeForHub("yz5"))
	assert.Assert(t, is.Len(nodeList, 0))

	nodeList = te.nodeStats.FilterNodes(GetNodeAdminByOp(pb.AdminNodeOp_kNoop))
	checkStringSameItems(t, nodeList, []string{"node1", "node2"})

	nodeList = te.nodeStats.FilterNodes(GetNodeAdminByOp(pb.AdminNodeOp_kRestart))
	checkStringSameItems(t, nodeList, []string{"node3", "node4"})

	nodeList = te.nodeStats.FilterNodes(GetNodeAdminByOp(pb.AdminNodeOp_kOffline))
	checkStringSameItems(t, nodeList, []string{"node5", "node6"})

	nodeList = te.nodeStats.FilterNodes(ExcludeNodeAdminByOp(pb.AdminNodeOp_kRestart))
	checkStringSameItems(t, nodeList, []string{"node1", "node2", "node5", "node6"})

	nodeList = te.nodeStats.FilterNodes(
		ExcludeFrom(map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kPrimary,
		}),
		GetAliveNode,
	)
	checkStringSameItems(t, nodeList, []string{"node3", "node5"})

	nodeList = te.nodeStats.FilterNodes(
		ExcludeNodeByRole(map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kPrimary,
			"node3": pb.ReplicaRole_kSecondary,
		}, pb.ReplicaRole_kPrimary),
		GetAliveNode,
	)
	checkStringSameItems(t, nodeList, []string{"node3"})

	nodeList = te.nodeStats.FilterNodes(
		GetNodeByRole(map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kPrimary,
			"node3": pb.ReplicaRole_kSecondary,
			"node5": pb.ReplicaRole_kSecondary,
		}, pb.ReplicaRole_kSecondary),
		GetAliveNode,
	)
	checkStringSameItems(t, nodeList, []string{"node3", "node5"})

	nodeList = te.nodeStats.FilterGivenNodes(
		[]string{"node1", "node3", "node5"},
		GetNodeAdminByOp(pb.AdminNodeOp_kRestart),
	)
	checkStringSameItems(t, nodeList, []string{"node3"})
}

func TestScoreWeightUpgradeFromOldServer(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"zw1": "ZW"}, pb.NodeFailureDomainType_PROCESS)
	defer te.teardown()

	oldInfo := `{
	"id":"node1",
	"op":0,
	"hub":"zw1",
	"alive":true,
	"az":"ZW",
	"address":{
		"node_name":"127.0.0.1",
		"port":1001
	},
	"process_id":"37047",
	"biz_port":10200,
	"node_index":""
}`
	ans := te.zkStore.Create(context.Background(), te.nodeStats.nodesPath+"/node1", []byte(oldInfo))
	assert.Assert(t, ans)

	handle := utils.MapHubs([]*pb.ReplicaHub{
		{Name: "zw1", Az: "ZW"},
	})

	te.nodeStats.LoadFromZookeeper(handle, false)
	info := te.nodeStats.MustGetNodeInfo("node1")
	assert.Equal(t, info.Score, est.INVALID_SCORE)
	assert.Equal(t, info.Weight, INVALID_WEIGHT)
	assert.Equal(t, info.GetWeight(), DEFAULT_WEIGHT)
	assert.Equal(t, info.WeightedScore(), INVALID_WEIGHTED_SCORE)
}

func TestRemoveUnusedNodeInfos(t *testing.T) {
	te := setupNodeStatTestEnv(
		t,
		map[string]string{"yz1": "YZ"},
		pb.NodeFailureDomainType_PROCESS,
	)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"normal", pb.AdminNodeOp_kNoop, "yz1", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
			nil,
		},
		{
			"offline_alive", pb.AdminNodeOp_kOffline, "yz1", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1002"), "12341", 2001, ""},
			nil,
		},
		{
			"offline_dead_no_empty", pb.AdminNodeOp_kOffline, "yz1", 10, 0,
			NodePing{false, "YZ", utils.FromHostPort("127.0.0.1:1003"), "12341", 2001, ""},
			nil,
		},
		{
			"offline_dead_empty", pb.AdminNodeOp_kOffline, "yz1", 10, 0,
			NodePing{false, "YZ", utils.FromHostPort("127.0.0.1:1004"), "12341", 2001, ""},
			nil,
		},
		{
			"alive_0_dup0", pb.AdminNodeOp_kNoop, "yz1", 10, 0,
			NodePing{false, "YZ", utils.FromHostPort("128.0.0.1:1001"), "12341", 2001, ""},
			nil,
		},
		{
			"alive_0_dup1", pb.AdminNodeOp_kNoop, "yz1", 10, 0,
			NodePing{false, "YZ", utils.FromHostPort("128.0.0.1:1001"), "12342", 2001, ""},
			nil,
		},
		{
			"alive_0_dup_offline_dead_no_empty", pb.AdminNodeOp_kOffline, "yz1", 10, 0,
			NodePing{false, "YZ", utils.FromHostPort("128.0.0.1:1001"), "12341", 2001, ""},
			nil,
		},
		{
			"alive_0_dup_offline_dead_empty", pb.AdminNodeOp_kOffline, "yz1", 10, 0,
			NodePing{false, "YZ", utils.FromHostPort("128.0.0.1:1001"), "12341", 2001, ""},
			nil,
		},
		{
			"alive_2_dup0", pb.AdminNodeOp_kNoop, "yz1", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("129.0.0.1:1001"), "12341", 2001, ""},
			nil,
		},
		{
			"alive_2_dup1", pb.AdminNodeOp_kNoop, "yz1", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("129.0.0.1:1001"), "12342", 2001, ""},
			nil,
		},
		{
			"alive_1_dup_alive", pb.AdminNodeOp_kNoop, "yz1", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("130.0.0.1:1001"), "12341", 2001, ""},
			nil,
		},
		{
			"alive_1_dup_dead", pb.AdminNodeOp_kNoop, "yz1", 10, 0,
			NodePing{false, "YZ", utils.FromHostPort("130.0.0.1:1001"), "12341", 2001, ""},
			nil,
		},
		{
			"alive_1_dup_offline_dead_no_empty", pb.AdminNodeOp_kOffline, "yz1", 10, 0,
			NodePing{false, "YZ", utils.FromHostPort("130.0.0.1:1001"), "12341", 2001, ""},
			nil,
		},
		{
			"alive_1_dup_offline_dead_empty", pb.AdminNodeOp_kOffline, "yz1", 10, 0,
			NodePing{false, "YZ", utils.FromHostPort("130.0.0.1:1001"), "12341", 2001, ""},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	te.nodeStats.LoadFromZookeeper(createHubsMapByName([]string{"yz1"}), false)
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"yz1": 7})
	te.checkNodeProperlyOrdered()

	assert.Equal(t, len(te.nodeStats.idNodes), 14)
	assert.Equal(t, te.nodeStats.addrNodes.nodes.Size(), 7)
	assert.Equal(t, te.nodeStats.addrNodes.count(infos[0].Address), 1)
	assert.Equal(t, te.nodeStats.addrNodes.count(infos[1].Address), 1)
	assert.Equal(t, te.nodeStats.addrNodes.count(infos[2].Address), 1)
	assert.Equal(t, te.nodeStats.addrNodes.count(infos[3].Address), 1)

	assert.Equal(t, te.nodeStats.addrNodes.count(infos[4].Address), 4)
	assert.Equal(t, te.nodeStats.addrNodes.count(infos[8].Address), 2)
	assert.Equal(t, te.nodeStats.addrNodes.count(infos[10].Address), 4)

	for _, info := range infos {
		info1 := te.nodeStats.MustGetNodeInfo(info.Id)
		assert.Equal(t, info1.Op, info.Op)
		info2 := te.nodeStats.addrNodes.get(info.Address, info.Id)
		assert.Equal(t, info2.Op, info.Op)
	}

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()

	rec := recorder.NewAllNodesRecorder(recorder.NewBriefNode)
	rec.Add("offline_dead_no_empty", 1, 1, pb.ReplicaRole_kLearner, 1)
	rec.Add("alive_1_dup_offline_dead_no_empty", 1, 2, pb.ReplicaRole_kLearner, 1)
	rec.Add("alive_0_dup_offline_dead_no_empty", 1, 3, pb.ReplicaRole_kLearner, 1)

	logging.Info("3 nodes removed, 1 nodes marked offline")
	te.nodeStats.RemoveUnusedNodeInfos(rec)
	assert.Equal(t, len(te.nodeStats.idNodes), 11)
	assert.Assert(t, te.nodeStats.GetNodeInfo("offline_dead_empty") == nil)
	assert.Assert(t, te.nodeStats.GetNodeInfo("alive_0_dup_offline_dead_empty") == nil)
	assert.Equal(t, te.nodeStats.addrNodes.count(infos[4].Address), 3)
	assert.Assert(t, te.nodeStats.GetNodeInfo("alive_1_dup_offline_dead_empty") == nil)
	assert.Equal(t, te.nodeStats.addrNodes.count(infos[10].Address), 3)

	infos[11].Op = pb.AdminNodeOp_kOffline
	for _, info := range infos {
		if ni := te.nodeStats.GetNodeInfo(info.Id); ni != nil {
			assert.Equal(t, ni.Op, info.Op)
		}
	}
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"yz1": 6})
	te.checkZkSynced()
	te.checkNodeProperlyOrdered()

	logging.Info("newly been empty nodes will be removed too")
	rec.Remove("offline_dead_no_empty", 1, 1, pb.ReplicaRole_kLearner, 1)
	rec.Remove("alive_1_dup_offline_dead_no_empty", 1, 2, pb.ReplicaRole_kLearner, 1)
	rec.Remove("alive_0_dup_offline_dead_no_empty", 1, 3, pb.ReplicaRole_kLearner, 1)

	te.nodeStats.RemoveUnusedNodeInfos(rec)
	assert.Equal(t, len(te.nodeStats.idNodes), 7)
	assert.Assert(t, te.nodeStats.GetNodeInfo("alive_1_dup_dead") == nil)
	assert.Assert(t, te.nodeStats.GetNodeInfo("offline_dead_no_empty") == nil)
	assert.Assert(t, te.nodeStats.GetNodeInfo("alive_0_dup_offline_dead_no_empty") == nil)
	assert.Assert(t, te.nodeStats.GetNodeInfo("alive_1_dup_offline_dead_no_empty") == nil)
	assert.Equal(t, te.nodeStats.addrNodes.count(infos[4].Address), 2)
	assert.Equal(t, te.nodeStats.addrNodes.count(infos[10].Address), 1)
	te.checkZkSynced()
	te.checkNodeProperlyOrdered()
}

func TestGetDuplicate(t *testing.T) {
	te := setupNodeStatTestEnv(
		t,
		map[string]string{"yz1": "YZ", "yz2": "YZ"},
		pb.NodeFailureDomainType_PROCESS,
	)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"node1",
			pb.AdminNodeOp_kNoop,
			"yz1", 10, 0,
			NodePing{false, "YZ", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
			nil,
		},
		{
			"node2",
			pb.AdminNodeOp_kNoop,
			"yz1", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1001"), "12342", 2001, ""},
			nil,
		},
		{
			"node3",
			pb.AdminNodeOp_kNoop,
			"yz2", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1002"), "12343", 2002, ""},
			nil,
		},
		{
			"node4",
			pb.AdminNodeOp_kNoop,
			"yz2", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1002"), "12344", 2002, ""},
			nil,
		},
		{
			"node5",
			pb.AdminNodeOp_kNoop,
			"yz2", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1004"), "12345", 2004, ""},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	te.nodeStats.LoadFromZookeeper(createHubsMapByName([]string{"yz1", "yz2"}), false)

	addrNodes := te.nodeStats.addrNodes
	assert.Equal(t, addrNodes.get(infos[0].Address, "node1").Id, "node1")
	assert.Equal(t, addrNodes.get(infos[0].Address, "node2").Id, "node2")
	assert.Assert(t, addrNodes.get(infos[0].Address, "node3") == nil)
	assert.Assert(t, addrNodes.get(utils.FromHostPort("127.0.0.1:1003"), "node3") == nil)

	assert.Assert(t, addrNodes.getUnique(infos[0].Address) == nil)
	assert.Equal(t, addrNodes.getUnique(infos[4].Address).Id, "node5")

	assert.Equal(t, addrNodes.count(infos[0].Address), 2)
	assert.Equal(t, addrNodes.countAlive(infos[0].Address), 1)
	assert.Equal(t, addrNodes.countAlive(infos[2].Address), 2)
}

func TestNodeAddressChanged(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"yz1": "YZ"}, pb.NodeFailureDomainType_PROCESS)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"node1",
			pb.AdminNodeOp_kNoop,
			"yz1", 10, 0,
			NodePing{false, "YZ", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	te.nodeStats.LoadFromZookeeper(createHubsMapByName([]string{"yz1"}), false)
	assert.Assert(t, te.nodeStats.addrNodes.getAll(utils.FromHostPort("127.0.0.1:1001")) != nil)
	assert.Assert(t, te.nodeStats.addrNodes.getAll(utils.FromHostPort("127.0.0.1:1011")) == nil)

	te.lock.LockWrite()
	te.nodeStats.UpdateStats(map[string]*NodePing{
		"node1": {true, "YZ", utils.FromHostPort("127.0.0.1:1011"), "13341", 2011, ""},
		"node2": {true, "YZ", utils.FromHostPort("127.0.0.1:1002"), "12342", 2002, ""},
	})
	te.lock.UnlockWrite()

	assert.Equal(t, len(te.nodeStats.idNodes), 2)
	assert.Equal(t, te.nodeStats.addrNodes.nodes.Size(), 2)

	data, _ := te.nodeStats.addrNodes.nodes.ToJSON()
	logging.Info(string(data))
	assert.Assert(t, te.nodeStats.addrNodes.getAll(utils.FromHostPort("127.0.0.1:1001")) == nil)
	assert.Assert(t, te.nodeStats.addrNodes.getAll(utils.FromHostPort("127.0.0.1:1002")) != nil)
	assert.Assert(t, te.nodeStats.addrNodes.getAll(utils.FromHostPort("127.0.0.1:1011")) != nil)
}

func TestAddHubsInLoadingNoAdjust(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"yz1": "YZ"}, pb.NodeFailureDomainType_PROCESS)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"node1",
			pb.AdminNodeOp_kNoop,
			"yz1", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
			nil,
		},
		{
			"node2",
			pb.AdminNodeOp_kNoop,
			"yz2", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1002"), "12342", 2002, ""},
			nil,
		},
		{
			"node3",
			pb.AdminNodeOp_kNoop,
			"yz3", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1003"), "12343", 2003, ""},
			nil,
		},
		{
			"node4",
			pb.AdminNodeOp_kNoop,
			"yz1", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1004"), "12344", 2004, ""},
			nil,
		},
		{
			"node5",
			pb.AdminNodeOp_kNoop,
			"yz2", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1005"), "12345", 2005, ""},
			nil,
		},
		{
			"node6",
			pb.AdminNodeOp_kRestart,
			"yz3", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1006"), "12346", 2006, ""},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	handle := createHubsMapByName([]string{"yz1", "yz2", "yz3", "yz4"})
	te.nodeStats.LoadFromZookeeper(handle, false)
	assert.Assert(t, utils.TreemapEqual(te.nodeStats.hubmap.GetMap(), handle.GetMap()))
	assert.Equal(t, len(te.nodeStats.idNodes), 6)
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"yz1": 2, "yz2": 2, "yz3": 2})

	for _, info := range infos {
		ninfo := te.nodeStats.MustGetNodeInfo(info.Id)
		assert.DeepEqual(t, info, ninfo)
	}

	te.checkZkSynced()
}

func TestAddHubsInLoadingAdjust(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"yz1": "YZ"}, pb.NodeFailureDomainType_HOST)
	defer te.teardown()

	uniqueHubNodes := []*NodeInfo{
		{
			"uniq_hub_node1", pb.AdminNodeOp_kNoop, "uniq_hub1", 10, 0,
			NodePing{true, "uniq", utils.FromHostPort("127.0.0.1:1001"), "10001", 2001, ""},
			nil,
		},
	}

	balancedHubs := []*NodeInfo{
		{
			"balance_node1", pb.AdminNodeOp_kNoop, "balance_hub1", 10, 0,
			NodePing{
				true,
				"balance",
				utils.FromHostPort("128.0.0.1:1001"),
				"10011",
				2001,
				"",
			},
			nil,
		},
		{
			"balance_node2", pb.AdminNodeOp_kNoop, "balance_hub1", 10, 0,
			NodePing{
				true,
				"balance",
				utils.FromHostPort("128.0.1.1:1001"),
				"10012",
				2001,
				"",
			},
			nil,
		},
		{
			"balance_node3", pb.AdminNodeOp_kNoop, "balance_hub2", 10, 0,
			NodePing{
				true,
				"balance",
				utils.FromHostPort("128.0.2.1:1001"),
				"10013",
				2001,
				"",
			},
			nil,
		},
		{
			"balance_node4", pb.AdminNodeOp_kOffline, "balance_hub2", 10, 0,
			NodePing{
				true,
				"balance",
				utils.FromHostPort("128.0.3.1:1001"),
				"10014",
				2001,
				"",
			},
			nil,
		},
	}

	unbalanceHubs := []*NodeInfo{
		{
			"unbalance_node1", pb.AdminNodeOp_kNoop, "unbalance_hub1", 10, 0,
			NodePing{
				true,
				"unbalance",
				utils.FromHostPort("129.0.0.1:1001"),
				"10011",
				2001,
				"",
			},
			nil,
		},
		{
			"unbalance_node2", pb.AdminNodeOp_kNoop, "unbalance_hub1", 10, 0,
			NodePing{
				true,
				"unbalance",
				utils.FromHostPort("129.0.1.1:1001"),
				"10012",
				2001,
				"",
			},
			nil,
		},
		{
			"unbalance_node3", pb.AdminNodeOp_kNoop, "unbalance_hub1", 10, 0,
			NodePing{
				true,
				"unbalance",
				utils.FromHostPort("129.0.2.1:1001"),
				"10013",
				2001,
				"",
			},
			nil,
		},
		{
			"unbalance_node4", pb.AdminNodeOp_kNoop, "unbalance_hub1", 10, 0,
			NodePing{
				true,
				"unbalance",
				utils.FromHostPort("129.0.3.1:1001"),
				"10014",
				2001,
				"",
			},
			nil,
		},
		{
			"unbalance_node5", pb.AdminNodeOp_kNoop, "unbalance_hub2", 10, 0,
			NodePing{
				true,
				"unbalance",
				utils.FromHostPort("129.0.4.1:1001"),
				"10015",
				2001,
				"",
			},
			nil,
		},
		{
			"unbalance_node6", pb.AdminNodeOp_kNoop, "unbalance_hub3", 10, 0,
			NodePing{
				true,
				"unbalance",
				utils.FromHostPort("129.0.5.1:1001"),
				"10016",
				2001,
				"",
			},
			nil,
		},
		{
			"unbalance_node7", pb.AdminNodeOp_kNoop, "unbalance_hub4", 10, 0,
			NodePing{
				true,
				"unbalance",
				utils.FromHostPort("129.0.6.1:1001"),
				"10011",
				2001,
				"",
			},
			nil,
		},
		{
			"unbalance_node8", pb.AdminNodeOp_kNoop, "unbalance_hub4", 10, 0,
			NodePing{
				true,
				"unbalance",
				utils.FromHostPort("129.0.7.1:1001"),
				"10012",
				2001,
				"",
			},
			nil,
		},
		{
			"unbalance_node9", pb.AdminNodeOp_kNoop, "unbalance_hub4", 10, 0,
			NodePing{
				true,
				"unbalance",
				utils.FromHostPort("129.0.8.1:1001"),
				"10013",
				2001,
				"",
			},
			nil,
		},
		{
			"unbalance_node10", pb.AdminNodeOp_kNoop, "unbalance_hub4", 10, 0,
			NodePing{
				true,
				"unbalance",
				utils.FromHostPort("129.0.9.1:1001"),
				"10014",
				2001,
				"",
			},
			nil,
		},
	}

	unbalanceShareHosts := []*NodeInfo{
		{
			"ubsh_node1", pb.AdminNodeOp_kNoop, "ubsh_hub1", 10, 0,
			NodePing{true, "ubsh", utils.FromHostPort("130.0.0.1:1001"), "10011", 2001, ""},
			nil,
		},
		{
			"ubsh_node2", pb.AdminNodeOp_kNoop, "ubsh_hub1", 10, 0,
			NodePing{true, "ubsh", utils.FromHostPort("130.0.0.1:1002"), "10012", 2001, ""},
			nil,
		},
		{
			"ubsh_node3", pb.AdminNodeOp_kNoop, "ubsh_hub1", 10, 0,
			NodePing{true, "ubsh", utils.FromHostPort("130.0.0.2:1001"), "10011", 2001, ""},
			nil,
		},
		{
			"ubsh_node4", pb.AdminNodeOp_kNoop, "ubsh_hub1", 10, 0,
			NodePing{true, "ubsh", utils.FromHostPort("130.0.0.2:1002"), "10012", 2001, ""},
			nil,
		},
		{
			"ubsh_node5", pb.AdminNodeOp_kNoop, "ubsh_hub2", 10, 0,
			NodePing{true, "ubsh", utils.FromHostPort("130.0.0.3:1001"), "10011", 2001, ""},
			nil,
		},
	}

	var allInfos []*NodeInfo
	allInfos = append(allInfos, uniqueHubNodes...)
	allInfos = append(allInfos, balancedHubs...)
	allInfos = append(allInfos, unbalanceHubs...)
	allInfos = append(allInfos, unbalanceShareHosts...)

	hubmap := utils.NewHubHandle()
	for _, info := range allInfos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
		hubmap.AddHub(&pb.ReplicaHub{Name: info.Hub, Az: info.Az})
	}

	te.nodeStats.LoadFromZookeeper(hubmap, true)
	expectHubsize := map[string]int{
		"uniq_hub1":      1,
		"balance_hub1":   2,
		"balance_hub2":   1,
		"unbalance_hub1": 3,
		"unbalance_hub2": 2,
		"unbalance_hub3": 2,
		"unbalance_hub4": 3,
		"ubsh_hub1":      2,
		"ubsh_hub2":      3,
	}
	assert.DeepEqual(t, te.nodeStats.hubSize, expectHubsize)

	nodes := te.nodeStats.DivideByHubs(true)
	assert.Equal(t, len(nodes), len(expectHubsize))
	for hub, count := range expectHubsize {
		infos := nodes[hub]
		assert.Equal(t, len(infos), count)
	}

	ubsh_hub1 := nodes["ubsh_hub1"]
	assert.Assert(t, ubsh_hub1[0].sameFailureDomain(ubsh_hub1[1], pb.NodeFailureDomainType_HOST))

	ubsh_hub2 := nodes["ubsh_hub2"]
	transferToHub2 := unbalanceShareHosts[3]
	if ubsh_hub1[0].Address.NodeName == transferToHub2.Address.NodeName {
		transferToHub2 = unbalanceShareHosts[1]
	}
	share_hosts, remain := PopNodesWithinFailureDomain(
		ubsh_hub2,
		transferToHub2,
		pb.NodeFailureDomainType_HOST,
	)
	assert.Equal(t, len(remain), 1)
	assert.Equal(t, remain[0].Id, "ubsh_node5")
	assert.Equal(t, len(share_hosts), 2)
	assert.Assert(
		t,
		share_hosts[0].sameFailureDomain(share_hosts[1], pb.NodeFailureDomainType_HOST),
	)

	assert.Assert(t, share_hosts[0].Address.NodeName != ubsh_hub1[0].Address.NodeName)
	te.checkZkSynced()
}

func TestRemoveHubsInLoadingNoAdjust(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"yz1": "YZ"}, pb.NodeFailureDomainType_PROCESS)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"node1",
			pb.AdminNodeOp_kNoop,
			"yz1", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
			nil,
		},
		{
			"node2",
			pb.AdminNodeOp_kNoop,
			"yz2", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1002"), "12342", 2002, ""},
			nil,
		},
		{
			"node3",
			pb.AdminNodeOp_kNoop,
			"yz3", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1003"), "12343", 2003, ""},
			nil,
		},
		{
			"node4",
			pb.AdminNodeOp_kNoop,
			"yz1", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1004"), "12344", 2004, ""},
			nil,
		},
		{
			"node5",
			pb.AdminNodeOp_kNoop,
			"yz2", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1005"), "12345", 2005, ""},
			nil,
		},
		{
			"node6",
			pb.AdminNodeOp_kRestart,
			"yz3", 10, 0,
			NodePing{true, "YZ", utils.FromHostPort("127.0.0.1:1006"), "12346", 2006, ""},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	handle := createHubsMapByName([]string{"yz1", "yz2"})
	te.nodeStats.LoadFromZookeeper(handle, false)
	assert.Assert(t, utils.TreemapEqual(te.nodeStats.hubmap.GetMap(), handle.GetMap()))
	assert.Equal(t, len(te.nodeStats.idNodes), 6)
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"yz1": 2, "yz2": 2})

	for _, info := range infos {
		ninfo := te.nodeStats.MustGetNodeInfo(info.Id)
		if info.Hub != "yz3" {
			assert.DeepEqual(t, info, ninfo)
		} else {
			info.Op = pb.AdminNodeOp_kOffline
			assert.DeepEqual(t, info, ninfo)
		}
	}

	te.checkZkSynced()
}

func TestRemoveHubsInLoadingAdjust(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"yz1": "YZ"}, pb.NodeFailureDomainType_HOST)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"hub1_node1", pb.AdminNodeOp_kNoop, "hub1", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.0.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"hub2_node1", pb.AdminNodeOp_kNoop, "hub2", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.1.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"hub3_node1", pb.AdminNodeOp_kNoop, "hub3", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.3.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"hub3_node2", pb.AdminNodeOp_kNoop, "hub3", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.3.1:1002"), "10001", 2001, ""},
			nil,
		},
		{
			"hub3_node3", pb.AdminNodeOp_kNoop, "hub3", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.3.1:1003"), "10001", 2001, ""},
			nil,
		},
		{
			"hub3_node4", pb.AdminNodeOp_kNoop, "hub3", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.3.1:1004"), "10001", 2001, ""},
			nil,
		},
		{
			"hub4_node1", pb.AdminNodeOp_kNoop, "hub4", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.4.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"hub4_node2", pb.AdminNodeOp_kNoop, "hub4", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.4.2:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"hub4_node3", pb.AdminNodeOp_kOffline, "hub4", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.4.3:1001"), "10001", 2001, ""},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	handle := utils.MapHubs([]*pb.ReplicaHub{
		{Name: "hub1", Az: "hub"},
		{Name: "hub2", Az: "hub"},
	})
	te.nodeStats.LoadFromZookeeper(handle, true)

	assert.Equal(t, len(te.nodeStats.idNodes), len(infos))
	assert.Equal(t, len(te.nodeStats.hubSize), 2)
	assert.Assert(t, is.Contains(te.nodeStats.hubSize, "hub1"))
	assert.Assert(t, te.nodeStats.hubSize["hub1"] > 1)
	assert.Assert(t, te.nodeStats.hubSize["hub2"] > 1)
	assert.Equal(t, te.nodeStats.hubSize["hub1"]+te.nodeStats.hubSize["hub2"], len(infos)-1)
	assert.Assert(t, is.Contains(te.nodeStats.hubSize, "hub2"))

	te.checkZkSynced()
}

func TestGetByHubs(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"yz1": "YZ"}, pb.NodeFailureDomainType_PROCESS)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"hub1_node1", pb.AdminNodeOp_kNoop, "hub1", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.0.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"hub1_node2", pb.AdminNodeOp_kNoop, "hub1", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.0.2:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"hub1_node3", pb.AdminNodeOp_kOffline, "hub1", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.0.3:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"hub2_node1", pb.AdminNodeOp_kNoop, "hub2", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.1.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"hub2_node2", pb.AdminNodeOp_kOffline, "hub2", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.1.2:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"hub3_node1", pb.AdminNodeOp_kOffline, "hub3", 10, 0,
			NodePing{true, "hub", utils.FromHostPort("127.0.2.1:1001"), "10001", 2001, ""},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	handle := utils.MapHubs([]*pb.ReplicaHub{
		{Name: "hub1", Az: "hub"},
		{Name: "hub2", Az: "hub"},
		{Name: "hub3", Az: "hub"},
		{Name: "hub4", Az: "hub"},
	})
	te.nodeStats.LoadFromZookeeper(handle, false)

	nodes := te.nodeStats.DivideByHubs(true)
	for hub, hubInfos := range nodes {
		if hub == "hub1" {
			sort.Slice(hubInfos, func(i, j int) bool {
				return hubInfos[i].Id < hubInfos[j].Id
			})
			assert.DeepEqual(t, hubInfos, infos[0:2])
		} else if hub == "hub2" {
			assert.Assert(t, is.Len(hubInfos, 1))
			assert.DeepEqual(t, hubInfos[0], infos[3])
		} else if hub == "hub3" {
			assert.Assert(t, is.Len(hubInfos, 0))
		} else {
			assert.Equal(t, hub, "hub4")
			assert.Assert(t, is.Len(hubInfos, 0))
		}
	}

	nodes = te.nodeStats.DivideByHubs(false)
	for hub, hubInfos := range nodes {
		if hub == "hub1" {
			sort.Slice(hubInfos, func(i, j int) bool {
				return hubInfos[i].Id < hubInfos[j].Id
			})
			assert.Equal(t, len(hubInfos), 3)
			assert.DeepEqual(t, hubInfos, infos[0:3])
		} else if hub == "hub2" {
			sort.Slice(hubInfos, func(i, j int) bool {
				return hubInfos[i].Id < hubInfos[j].Id
			})
			assert.DeepEqual(t, hubInfos, infos[3:5])
		} else if hub == "hub3" {
			assert.Assert(t, is.Len(hubInfos, 1))
			assert.DeepEqual(t, hubInfos[0], infos[5])
		} else {
			assert.Equal(t, hub, "hub4")
			assert.Assert(t, is.Len(hubInfos, 0))
		}
	}

	nodes2 := te.nodeStats.fetchHubs([]string{"hub1", "hub3", "hub5"}, true)
	for hub, hubInfos := range nodes2 {
		if hub == "hub1" {
			sort.Slice(hubInfos, func(i, j int) bool {
				return hubInfos[i].Id < hubInfos[j].Id
			})
			assert.DeepEqual(t, hubInfos, infos[0:2])
		} else if hub == "hub3" {
			assert.Assert(t, is.Len(hubInfos, 0))
		} else {
			assert.Equal(t, hub, "hub5")
			assert.Assert(t, is.Len(hubInfos, 0))
		}
	}

	nodes2 = te.nodeStats.fetchHubs([]string{"hub1", "hub3", "hub5"}, false)
	for hub, hubInfos := range nodes2 {
		if hub == "hub1" {
			sort.Slice(hubInfos, func(i, j int) bool {
				return hubInfos[i].Id < hubInfos[j].Id
			})
			assert.DeepEqual(t, hubInfos, infos[0:3])
		} else if hub == "hub3" {
			assert.Assert(t, is.Len(hubInfos, 1))
			assert.DeepEqual(t, hubInfos[0], infos[5])
		} else {
			assert.Equal(t, hub, "hub5")
			assert.Assert(t, is.Len(hubInfos, 0))
		}
	}
}

func TestAllocateHub(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"yz1": "YZ"}, pb.NodeFailureDomainType_HOST)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"yz1_node1", pb.AdminNodeOp_kNoop, "yz1", 10, 0,
			NodePing{true, "yz", utils.FromHostPort("127.0.0.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"yz1_node2", pb.AdminNodeOp_kNoop, "yz1", 10, 0,
			NodePing{true, "yz", utils.FromHostPort("127.0.0.2:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw1_node1", pb.AdminNodeOp_kNoop, "zw1", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.1.1:1001"), "10001", 2001, ""},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	handle := utils.MapHubs([]*pb.ReplicaHub{
		{Name: "yz1", Az: "yz"},
		{Name: "zw1", Az: "zw"},
		{Name: "zw2", Az: "zw"},
		{Name: "zw3", Az: "zw"},
	})
	te.nodeStats.LoadFromZookeeper(handle, false)
	assert.Equal(t, len(te.nodeStats.idNodes), len(infos))

	np := map[string]*NodePing{
		// yz only has one hub
		"yz_node3": {true, "yz", utils.FromHostPort("127.0.0.3:1001"), "10001", 2001, ""},
		"yz_node4": {true, "yz", utils.FromHostPort("127.0.0.4:1001"), "10001", 2001, ""},
		// zw1_node2 share failure domain with zw1_node1
		"zw1_node2": {true, "zw", utils.FromHostPort("127.0.1.1:1002"), "10002", 2002, ""},
		// these 3 nodes share same failure domain
		"zw_node1": {true, "zw", utils.FromHostPort("127.0.2.1:1001"), "10001", 2001, ""},
		"zw_node2": {true, "zw", utils.FromHostPort("127.0.2.1:1002"), "10002", 2002, ""},
		"zw_node3": {true, "zw", utils.FromHostPort("127.0.2.1:1003"), "10003", 2003, ""},
		// another node for zw,
		"zw_min": {true, "zw", utils.FromHostPort("127.0.3.1:1001"), "10001", 2001, ""},
	}

	te.lock.LockWrite()
	te.nodeStats.UpdateStats(np)
	te.lock.UnlockWrite()

	assert.Equal(t, len(te.nodeStats.idNodes), len(infos)+len(np))
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("yz_node3").Hub, "yz1")
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("yz_node4").Hub, "yz1")

	assert.Equal(t, te.nodeStats.MustGetNodeInfo("zw1_node2").Hub, "zw1")
	zw_node1 := te.nodeStats.MustGetNodeInfo("zw_node1")
	zw_node2 := te.nodeStats.MustGetNodeInfo("zw_node2")
	zw_node3 := te.nodeStats.MustGetNodeInfo("zw_node3")
	zw_min := te.nodeStats.MustGetNodeInfo("zw_min")

	assert.Equal(t, zw_node1.Hub, zw_node2.Hub)
	assert.Equal(t, zw_node1.Hub, zw_node3.Hub)
	assert.Assert(t, zw_node1.Hub != "zw1")

	assert.Assert(t, zw_min.Hub != zw_node1.Hub)
	assert.Assert(t, zw_min.Hub != "zw1")

	te.checkZkSynced()

	np2 := map[string]*NodePing{
		"zw_min2": {true, "zw", utils.FromHostPort("127.0.3.2:1001"), "10002", 2001, ""},
	}
	te.lock.LockWrite()
	te.nodeStats.UpdateStats(np2)
	te.lock.UnlockWrite()

	assert.Equal(t, len(te.nodeStats.idNodes), len(infos)+len(np)+len(np2))
	zw_min2 := te.nodeStats.MustGetNodeInfo("zw_min2")
	assert.Equal(t, zw_min2.Hub, zw_min.Hub)
	te.checkZkSynced()
}

func TestAllocateHubHasDuplicateNodes(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"yz1": "YZ"}, pb.NodeFailureDomainType_HOST)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"yz1_node1", pb.AdminNodeOp_kNoop, "yz1", 10, 0,
			NodePing{true, "yz", utils.FromHostPort("127.0.0.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"yz1_node1_dup1", pb.AdminNodeOp_kNoop, "yz1", 10, 0,
			NodePing{false, "yz", utils.FromHostPort("127.0.0.1:1001"), "10002", 2001, ""},
			nil,
		},
		{
			"yz1_node1_dup2", pb.AdminNodeOp_kNoop, "yz1", 10, 0,
			NodePing{false, "yz", utils.FromHostPort("127.0.0.1:1001"), "10003", 2001, ""},
			nil,
		},
		{
			"yz2_node1", pb.AdminNodeOp_kNoop, "yz2", 10, 0,
			NodePing{true, "yz", utils.FromHostPort("127.0.1.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"yz2_node2", pb.AdminNodeOp_kNoop, "yz2", 10, 0,
			NodePing{true, "yz", utils.FromHostPort("127.0.1.2:1001"), "10001", 2001, ""},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	handle := utils.MapHubs([]*pb.ReplicaHub{
		{Name: "yz1", Az: "yz"},
		{Name: "yz2", Az: "yz"},
	})
	te.nodeStats.LoadFromZookeeper(handle, false)
	assert.Equal(t, len(te.nodeStats.idNodes), len(infos))
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"yz1": 3, "yz2": 2})

	np := map[string]*NodePing{
		// although yz1 has 3 nodes, all of them are same ip:port.
		// so new node will be allocated to yz1
		"yz1_node3": {true, "yz", utils.FromHostPort("127.0.0.3:1001"), "10001", 2001, ""},
	}

	te.lock.LockWrite()
	te.nodeStats.UpdateStats(np)
	te.lock.UnlockWrite()

	assert.Equal(t, te.nodeStats.MustGetNodeInfo("yz1_node3").Hub, "yz1")
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"yz1": 4, "yz2": 2})
	te.checkZkSynced()
}

func TestShrinkAz(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"zw1": "zw"}, pb.NodeFailureDomainType_HOST)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"zw1_node1", pb.AdminNodeOp_kNoop, "zw1", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.0.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw1_node2", pb.AdminNodeOp_kNoop, "zw1", 10, 0,
			NodePing{false, "zw", utils.FromHostPort("127.0.0.2:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw1_node3", pb.AdminNodeOp_kRestart, "zw1", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.0.3:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw2_node1", pb.AdminNodeOp_kNoop, "zw2", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.1.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw2_node2", pb.AdminNodeOp_kNoop, "zw2", 10, 0,
			NodePing{false, "zw", utils.FromHostPort("127.0.1.2:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw3_node1", pb.AdminNodeOp_kNoop, "zw3", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.3.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw3_node2", pb.AdminNodeOp_kOffline, "zw3", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.3.2:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw3_node3", pb.AdminNodeOp_kOffline, "zw3", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.3.3:1001"), "10001", 2001, ""},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	handle := utils.MapHubs([]*pb.ReplicaHub{
		{Name: "zw1", Az: "zw"},
		{Name: "zw2", Az: "zw"},
		{Name: "zw3", Az: "zw"},
	})
	te.nodeStats.LoadFromZookeeper(handle, false)
	assert.Equal(t, len(te.nodeStats.idNodes), len(infos))

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()
	resp := &pb.ShrinkAzResponse{}
	te.nodeStats.ShrinkAz("yz", 3, nil, false, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	resp.Status.Code = 0
	te.nodeStats.ShrinkAz("zw", 0, nil, false, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kInvalidParameter))

	resp.Status.Code = 0
	te.nodeStats.ShrinkAz("zw", 10, nil, false, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, is.Len(resp.Shrinked, 0))

	te.nodeStats.ShrinkAz("zw", 3, nil, false, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, is.Len(resp.Shrinked, 3))

	infos[1].Op = pb.AdminNodeOp_kOffline
	infos[2].Op = pb.AdminNodeOp_kOffline
	infos[4].Op = pb.AdminNodeOp_kOffline

	for _, info := range infos {
		node := te.nodeStats.MustGetNodeInfo(info.Id)
		assert.DeepEqual(t, node, info)
	}
	te.checkZkSynced()
}

func TestCountResource(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"zw1": "zw"}, pb.NodeFailureDomainType_HOST)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"zw1_node1", pb.AdminNodeOp_kNoop, "zw1", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.0.1:1001"), "10001", 2001, ""},
			nil,
		},
		// 127.0.0.1:1001 only count once for resource as the ip:port is duplicate
		{
			"zw1_node2", pb.AdminNodeOp_kNoop, "zw1", 10, 0,
			NodePing{false, "zw", utils.FromHostPort("127.0.0.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw1_node3", pb.AdminNodeOp_kOffline, "zw1", 10, 0,
			NodePing{false, "zw", utils.FromHostPort("127.0.0.3:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw2_node1", pb.AdminNodeOp_kNoop, "zw2", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.1.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw2_node2", pb.AdminNodeOp_kNoop, "zw2", 10, 0,
			NodePing{false, "zw", utils.FromHostPort("127.0.1.2:1001"), "10001", 2001, ""},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	handle := utils.MapHubs([]*pb.ReplicaHub{
		{Name: "zw1", Az: "zw"},
		{Name: "zw2", Az: "zw"},
	})
	te.nodeStats.LoadFromZookeeper(handle, false)
	for _, node := range te.nodeStats.idNodes {
		node.resource = utils.HardwareUnit{"cpu": 64, "mem": 1024}
	}

	res := te.nodeStats.CountResource()
	assert.DeepEqual(t, res, utils.HardwareUnits{
		"zw1": utils.HardwareUnit{
			"cpu": 64,
			"mem": 1024,
		},
		"zw2": utils.HardwareUnit{
			"cpu": 128,
			"mem": 2048,
		},
	})

	res = te.nodeStats.CountResource(GetNodeForHub("zw1"))
	assert.DeepEqual(t, res, utils.HardwareUnits{
		"zw1": utils.HardwareUnit{
			"cpu": 64,
			"mem": 1024,
		},
	})
}

func TestShrinkAzWithAuditResource(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"zw1": "zw"}, pb.NodeFailureDomainType_HOST)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"zw1_node1", pb.AdminNodeOp_kNoop, "zw1", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.0.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw1_node2", pb.AdminNodeOp_kNoop, "zw1", 10, 0,
			NodePing{false, "zw", utils.FromHostPort("127.0.0.2:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw1_node3", pb.AdminNodeOp_kOffline, "zw1", 10, 0,
			NodePing{false, "zw", utils.FromHostPort("127.0.0.3:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw2_node1", pb.AdminNodeOp_kNoop, "zw2", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.1.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw2_node2", pb.AdminNodeOp_kNoop, "zw2", 10, 0,
			NodePing{false, "zw", utils.FromHostPort("127.0.1.2:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw2_node3", pb.AdminNodeOp_kNoop, "zw2", 10, 0,
			NodePing{false, "zw", utils.FromHostPort("127.0.1.3:1001"), "10001", 2001, ""},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	handle := utils.MapHubs([]*pb.ReplicaHub{
		{Name: "zw1", Az: "zw"},
		{Name: "zw2", Az: "zw"},
	})
	te.nodeStats.LoadFromZookeeper(handle, false)
	for _, node := range te.nodeStats.idNodes {
		node.resource = utils.HardwareUnit{"cpu": 64, "mem": 1024}
	}

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()
	resp := &pb.ShrinkAzResponse{}
	te.nodeStats.ShrinkAz("zw", 2, utils.HardwareUnit{"cpu": 80, "mem": 1024}, false, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kNotEnoughResource))
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"zw1": 2, "zw2": 3})

	te.nodeStats.ShrinkAz("zw", 2, utils.HardwareUnit{"cpu": 64, "mem": 1024}, false, resp)
	assert.Equal(t, resp.Status.Code, int32(pb.AdminError_kOk))
	assert.Assert(t, is.Len(resp.Shrinked, 3))
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"zw1": 1, "zw2": 1})
}

func TestOfflineNodesWithAuditResource(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"zw1": "zw"}, pb.NodeFailureDomainType_HOST)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"zw1_node1", pb.AdminNodeOp_kNoop, "zw1", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.0.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw1_node2", pb.AdminNodeOp_kNoop, "zw1", 10, 0,
			NodePing{false, "zw", utils.FromHostPort("127.0.0.2:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw2_node1", pb.AdminNodeOp_kNoop, "zw2", 10, 0,
			NodePing{true, "zw", utils.FromHostPort("127.0.1.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw2_node2", pb.AdminNodeOp_kNoop, "zw2", 10, 0,
			NodePing{false, "zw", utils.FromHostPort("127.0.1.2:1001"), "10001", 2001, ""},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	handle := utils.MapHubs([]*pb.ReplicaHub{
		{Name: "zw1", Az: "zw"},
		{Name: "zw2", Az: "zw"},
	})
	te.nodeStats.LoadFromZookeeper(handle, false)
	for _, node := range te.nodeStats.idNodes {
		node.resource = utils.HardwareUnit{"cpu": 64, "mem": 1024}
	}

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()

	res := te.nodeStats.AdminNodes(
		utils.FromHostPorts([]string{"127.0.0.1:1001", "127.0.1.1:1001"}),
		true,
		pb.AdminNodeOp_kOffline,
		utils.HardwareUnit{"cpu": 80, "mem": 1024},
	)
	assert.Equal(t, len(res), 2)
	assert.Equal(t, res[0].Code, int32(pb.AdminError_kNotEnoughResource))
	assert.Equal(t, res[1].Code, int32(pb.AdminError_kNotEnoughResource))
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"zw1": 2, "zw2": 2})
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("zw1_node1").Op, pb.AdminNodeOp_kNoop)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("zw2_node1").Op, pb.AdminNodeOp_kNoop)

	res = te.nodeStats.AdminNodes(
		utils.FromHostPorts([]string{"127.0.0.1:1001", "127.0.1.1:1001"}),
		true,
		pb.AdminNodeOp_kOffline,
		utils.HardwareUnit{"cpu": 64, "mem": 1024},
	)
	assert.Equal(t, len(res), 2)
	assert.Equal(t, res[0].Code, int32(pb.AdminError_kOk))
	assert.Equal(t, res[1].Code, int32(pb.AdminError_kOk))
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("zw1_node1").Op, pb.AdminNodeOp_kOffline)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("zw2_node1").Op, pb.AdminNodeOp_kOffline)
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"zw1": 1, "zw2": 1})
}

func TestRestartOpCleaned(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"zw1": "zw"}, pb.NodeFailureDomainType_HOST)
	defer te.teardown()

	infos := []*NodeInfo{
		{
			"zw1_node1", pb.AdminNodeOp_kOffline, "zw1", 10, 0,
			NodePing{false, "zw", utils.FromHostPort("127.0.0.1:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw1_node2", pb.AdminNodeOp_kRestart, "zw1", 10, 0,
			NodePing{false, "zw", utils.FromHostPort("127.0.0.2:1001"), "10001", 2001, ""},
			nil,
		},
		{
			"zw1_node3", pb.AdminNodeOp_kRestart, "zw1", 10, 0,
			NodePing{false, "zw", utils.FromHostPort("127.0.0.3:1001"), "10001", 2001, ""},
			nil,
		},
	}

	for _, info := range infos {
		data := utils.MarshalJsonOrDie(info)
		te.zkStore.Create(context.Background(), te.nodeStats.getNodeZkPath(info.Id), data)
	}

	handle := utils.MapHubs([]*pb.ReplicaHub{
		{Name: "zw1", Az: "zw"},
	})
	te.nodeStats.LoadFromZookeeper(handle, false)
	assert.Equal(t, len(te.nodeStats.idNodes), len(infos))

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()

	te.nodeStats.UpdateStats(map[string]*NodePing{
		"zw1_node1": {true, "zw", utils.FromHostPort("127.0.0.1:1001"), "10002", 2001, ""},
		"zw1_node2": {true, "zw", utils.FromHostPort("127.0.0.2:1001"), "10002", 2001, ""},
		"zw1_node3": {true, "zw", utils.FromHostPort("127.0.0.3:1001"), "10001", 2001, ""},
	})

	assert.Equal(t, te.nodeStats.MustGetNodeInfo(infos[0].Id).Op, pb.AdminNodeOp_kOffline)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo(infos[1].Id).Op, pb.AdminNodeOp_kNoop)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo(infos[2].Id).Op, pb.AdminNodeOp_kRestart)

	te.checkZkSynced()
}

func TestAssignHub(t *testing.T) {
	te := setupNodeStatTestEnv(
		t,
		map[string]string{"zw1": "ZW", "zw2": "ZW", "yz1": "YZ"},
		pb.NodeFailureDomainType_PROCESS,
	)
	defer te.teardown()

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()

	hint := map[string]*pb.NodeHints{
		"dev-huyifan03-02.dev.kwaidc.com:1003": {Hub: "zw1"},
		"dev-huyifan03-02.dev.kwaidc.com:1004": {Hub: "zw2"},
	}
	te.nodeStats.AddHints(hint, true)
	te.nodeStats.UpdateStats(
		map[string]*NodePing{
			"node1": {
				true,
				"ZW",
				utils.FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1003"),
				"10001",
				2003,
				"",
			},
			"node2": {
				true,
				"ZW",
				utils.FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1004"),
				"10002",
				2004,
				"",
			},
		},
	)

	info := te.nodeStats.GetNodeInfo("node1")
	assert.Equal(t, info.Hub, "zw1")

	logging.Info("assign to invalid hub")
	err := te.nodeStats.AssignHub(utils.FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1003"), "zw3")
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("assign hub but can't find node")
	err = te.nodeStats.AssignHub(utils.FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1001"), "zw1")
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("duplicate assign")
	err = te.nodeStats.AssignHub(utils.FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1003"), "zw1")
	assert.Equal(t, err.Code, int32(pb.AdminError_kDuplicateRequest))

	logging.Info("assign to different az")
	err = te.nodeStats.AssignHub(utils.FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1003"), "yz1")
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))

	logging.Info("assign succeed")
	err = te.nodeStats.AssignHub(utils.FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1003"), "zw2")
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))

	info = te.nodeStats.GetNodeInfo("node1")
	assert.Equal(t, info.Hub, "zw2")

	te.checkZkSynced()
}

func TestHintAllocateHub(t *testing.T) {
	te := setupNodeStatTestEnv(
		t,
		map[string]string{"zw1": "ZW", "zw3": "ZW", "yz1": "YZ"},
		pb.NodeFailureDomainType_PROCESS,
	)
	defer te.teardown()

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()

	hint := map[string]*pb.NodeHints{
		"127.0.0.2:1001":                       {Hub: "zw1"},
		"dev-huyifan03-02.dev.kwaidc.com:1003": {Hub: "zw1"},
		"dev-huyifan03-02.dev.kwaidc.com:1004": {Hub: "zw1"},
	}

	logging.Info("add failed due to can't find rms for some node")
	err := te.nodeStats.AddHints(hint, true)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Equal(t, len(te.nodeStats.hints), 0)
	te.checkZkSynced()

	delete(hint, "127.0.0.2:1001")
	hint["dev-huyifan03-02.dev.kwaidc.com"] = &pb.NodeHints{Hub: "yz1"}
	logging.Info("add failed due to not host:port")
	err = te.nodeStats.AddHints(hint, true)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Equal(t, len(te.nodeStats.hints), 0)
	te.checkZkSynced()

	delete(hint, "dev-huyifan03-02.dev.kwaidc.com")
	hint["dev-huyifan03-02.dev.kwaidc.com:1001"] = &pb.NodeHints{Hub: "yz1"}
	logging.Info("add failed due to az not match")
	err = te.nodeStats.AddHints(hint, true)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Equal(t, len(te.nodeStats.hints), 0)
	te.checkZkSynced()

	delete(hint, "dev-huyifan03-02.dev.kwaidc.com:1001")
	hint["dev-huyifan03-02.dev.kwaidc.com:1001"] = &pb.NodeHints{Hub: "zw2"}
	logging.Info("add failed due to hub not exists")
	err = te.nodeStats.AddHints(hint, true)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Equal(t, len(te.nodeStats.hints), 0)
	te.checkZkSynced()

	delete(hint, "dev-huyifan03-02.dev.kwaidc.com:1001")
	logging.Info("add hint succeed")
	err = te.nodeStats.AddHints(hint, true)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(te.nodeStats.hints), 2)
	assert.Equal(t, te.nodeStats.hints["dev-huyifan03-02.dev.kwaidc.com:1003"].Hub, "zw1")
	assert.Equal(t, te.nodeStats.hints["dev-huyifan03-02.dev.kwaidc.com:1004"].Hub, "zw1")
	te.checkZkSynced()

	logging.Info("update hint")
	err = te.nodeStats.AddHints(
		map[string]*pb.NodeHints{
			"dev-huyifan03-02.dev.kwaidc.com:1003": {Hub: "zw3"},
			"dev-huyifan03-02.dev.kwaidc.com:1004": {Hub: "zw1"},
		}, true)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(te.nodeStats.hints), 2)
	assert.Equal(t, te.nodeStats.hints["dev-huyifan03-02.dev.kwaidc.com:1003"].Hub, "zw3")
	assert.Equal(t, te.nodeStats.hints["dev-huyifan03-02.dev.kwaidc.com:1004"].Hub, "zw1")
	te.checkZkSynced()

	logging.Info("remove hint failed as can't find one")
	err = te.nodeStats.RemoveHints([]*pb.RpcNode{
		{NodeName: "dev-huyifan03-02.dev.kwaidc.com", Port: 1001},
		{NodeName: "dev-huyifan03-02.dev.kwaidc.com", Port: 1003},
	}, true)
	assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Equal(t, len(te.nodeStats.hints), 2)

	logging.Info("remove hint succeed")
	err = te.nodeStats.RemoveHints([]*pb.RpcNode{
		{NodeName: "dev-huyifan03-02.dev.kwaidc.com", Port: 1003},
	}, true)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(te.nodeStats.hints), 1)
	te.checkZkSynced()

	logging.Info("add hint back to check unbalanced hub allocation")
	err = te.nodeStats.AddHints(hint, true)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(te.nodeStats.hints), 2)
	assert.Equal(t, te.nodeStats.hints["dev-huyifan03-02.dev.kwaidc.com:1003"].Hub, "zw1")
	assert.Equal(t, te.nodeStats.hints["dev-huyifan03-02.dev.kwaidc.com:1004"].Hub, "zw1")
	te.checkZkSynced()

	logging.Info("all nodes are allocated to zw1")
	te.nodeStats.UpdateStats(map[string]*NodePing{
		"node1": {
			true,
			"ZW",
			utils.FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1003"),
			"10001",
			2003,
			"",
		},
		"node2": {
			true,
			"ZW",
			utils.FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1004"),
			"10002",
			2004,
			"",
		},
	})
	assert.Equal(t, len(te.nodeStats.idNodes), 2)
	assert.Equal(t, te.nodeStats.idNodes["node1"].Hub, "zw1")
	assert.Equal(t, te.nodeStats.idNodes["node2"].Hub, "zw1")
	assert.Equal(t, len(te.nodeStats.hints), 0)
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"zw1": 2})

	logging.Info("clean node1 and reallocation to zw3")
	te.nodeStats.GetNodeInfo("node1").Op = pb.AdminNodeOp_kOffline
	te.nodeStats.removeNodeInfo([]*NodeInfo{te.nodeStats.idNodes["node1"]})
	te.checkZkSynced()

	te.nodeStats.UpdateStats(map[string]*NodePing{
		"node1": {
			true,
			"ZW",
			utils.FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1003"),
			"10001",
			2003,
			"",
		},
	})
	assert.Equal(t, te.nodeStats.idNodes["node1"].Hub, "zw3")
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"zw1": 1, "zw3": 1})

	logging.Info("add hint and remove hub, then hint will be skipped")
	te.nodeStats.GetNodeInfo("node1").Op = pb.AdminNodeOp_kOffline
	te.nodeStats.GetNodeInfo("node2").Op = pb.AdminNodeOp_kOffline
	te.nodeStats.removeNodeInfo(te.nodeStats.GetNodeInfoByAddrs(
		utils.FromHostPorts(
			[]string{
				"dev-huyifan03-02.dev.kwaidc.com:1003",
				"dev-huyifan03-02.dev.kwaidc.com:1004",
			},
		),
		true))
	assert.Equal(t, len(te.nodeStats.idNodes), 0)

	err = te.nodeStats.AddHints(hint, true)
	assert.Equal(t, err.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(te.nodeStats.hints), 2)
	assert.Equal(t, te.nodeStats.hints["dev-huyifan03-02.dev.kwaidc.com:1003"].Hub, "zw1")
	assert.Equal(t, te.nodeStats.hints["dev-huyifan03-02.dev.kwaidc.com:1004"].Hub, "zw1")
	te.checkZkSynced()

	te.nodeStats.UpdateHubs(utils.MapHubs([]*pb.ReplicaHub{{Name: "zw3", Az: "ZW"}}), false)
	te.nodeStats.UpdateHubs(utils.MapHubs([]*pb.ReplicaHub{
		{Name: "zw2", Az: "ZW"},
		{Name: "zw3", Az: "ZW"},
	}), false)
	te.nodeStats.UpdateStats(map[string]*NodePing{
		"node1": {
			true,
			"ZW",
			utils.FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1003"),
			"10001",
			2003,
			"",
		},
		"node2": {
			true,
			"ZW",
			utils.FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1004"),
			"10002",
			2004,
			"",
		},
	})
	assert.Equal(t, len(te.nodeStats.idNodes), 2)
	assert.Assert(t, te.nodeStats.idNodes["node1"].Hub != "zw1")
	assert.Assert(t, te.nodeStats.idNodes["node2"].Hub != "zw1")
	assert.Equal(t, len(te.nodeStats.hints), 0)
	assert.DeepEqual(t, te.nodeStats.hubSize, map[string]int{"zw2": 1, "zw3": 1})
}

func TestGetAddrFuzzyPort(t *testing.T) {
	te := setupNodeStatTestEnv(
		t,
		map[string]string{"zw1": "ZW"},
		pb.NodeFailureDomainType_PROCESS,
	)
	defer te.teardown()

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()

	te.nodeStats.UpdateStats(
		map[string]*NodePing{
			"node1": {
				true,
				"ZW",
				utils.FromHostPort("127.0.0.1:1003"),
				"10001",
				2003,
				"",
			},
			"node2": {
				true,
				"ZW",
				utils.FromHostPort("127.0.0.2:1004"),
				"10002",
				2004,
				"",
			},
			"node3": {
				true,
				"ZW",
				utils.FromHostPort("127.0.0.2:1004"),
				"20002",
				2104,
				"",
			},
		},
	)

	ans := te.nodeStats.GetNodeInfoByAddrs(
		[]*utils.RpcNode{{NodeName: "127.0.0.1", Port: 1003}},
		true,
	)
	assert.Equal(t, ans[0].Id, "node1")
	ans = te.nodeStats.GetNodeInfoByAddrs(
		[]*utils.RpcNode{{NodeName: "127.0.0.1", Port: 1003}},
		false,
	)
	assert.Equal(t, ans[0].Id, "node1")

	ans = te.nodeStats.GetNodeInfoByAddrs(
		[]*utils.RpcNode{{NodeName: "127.0.0.1", Port: 1004}},
		true,
	)
	assert.Assert(t, ans[0] == nil)
	ans = te.nodeStats.GetNodeInfoByAddrs(
		[]*utils.RpcNode{{NodeName: "127.0.0.1", Port: 1004}},
		false,
	)
	assert.Equal(t, ans[0].Id, "node1")

	ans = te.nodeStats.GetNodeInfoByAddrs(
		[]*utils.RpcNode{{NodeName: "127.0.0.2", Port: 1004}},
		true,
	)
	assert.Assert(t, ans[0] == nil)
	ans = te.nodeStats.GetNodeInfoByAddrs(
		[]*utils.RpcNode{{NodeName: "127.0.0.2", Port: 1004}},
		false,
	)
	assert.Equal(t, len(ans), 1)
	assert.Assert(t, ans[0].Id == "node2" || ans[0].Id == "node3")
	ans = te.nodeStats.GetNodeInfoByAddrs(
		[]*utils.RpcNode{{NodeName: "127.0.0.2", Port: 1005}},
		false,
	)
	assert.Equal(t, len(ans), 1)
	assert.Assert(t, ans[0].Id == "node2" || ans[0].Id == "node3")
}

func TestAdminNodeFuzzyPort(t *testing.T) {
	te := setupNodeStatTestEnv(
		t,
		map[string]string{"zw1": "ZW"},
		pb.NodeFailureDomainType_PROCESS,
	)
	defer te.teardown()

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()

	te.nodeStats.UpdateStats(
		map[string]*NodePing{
			"node1": {
				true,
				"ZW",
				utils.FromHostPort("127.0.0.1:1003"),
				"10001",
				2003,
				"",
			},
			"node2": {
				true,
				"ZW",
				utils.FromHostPort("127.0.0.2:1004"),
				"10002",
				2004,
				"",
			},
		},
	)

	ans := te.nodeStats.AdminNodes([]*utils.RpcNode{
		{NodeName: "127.0.0.1", Port: 1001},
		{NodeName: "127.0.0.1", Port: 1002},
	}, false, pb.AdminNodeOp_kOffline, nil)

	assert.Equal(t, len(ans), 2)
	for _, status := range ans {
		assert.Equal(t, status.Code, int32(pb.AdminError_kOk))
	}

	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node1").Op, pb.AdminNodeOp_kOffline)
	te.checkZkSynced()
}

func TestHintsFuzzyPort(t *testing.T) {
	te := setupNodeStatTestEnv(
		t,
		map[string]string{"zw1": "ZW", "zw2": "ZW", "yz1": "YZ"},
		pb.NodeFailureDomainType_PROCESS,
	)
	te.nodeStats.WithSkipHintCheck(true)
	defer te.teardown()

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()

	hint := map[string]*pb.NodeHints{
		"127.0.0.1:1003": {Hub: "zw1"},
		"127.0.0.2:1003": {Hub: "zw1"},
		"127.0.0.5:1003": {Hub: "zw1"},
	}
	te.nodeStats.AddHints(hint, false)

	hint = map[string]*pb.NodeHints{
		"127.0.0.3:1003": {Hub: "zw1"},
		"127.0.0.4:1003": {Hub: "zw1"},
	}
	te.nodeStats.AddHints(hint, true)

	te.nodeStats.UpdateStats(map[string]*NodePing{
		"node1": {
			true,
			"ZW",
			utils.FromHostPort("127.0.0.1:1001"),
			"10001",
			2003,
			"",
		},
	})
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node1").Hub, "zw1")
	assert.Equal(t, len(te.nodeStats.hints), 4)
	te.checkZkSynced()

	te.nodeStats.UpdateStats(map[string]*NodePing{
		"node2": {
			true,
			"ZW",
			utils.FromHostPort("127.0.0.2:1001"),
			"10001",
			2003,
			"",
		},
	})
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2").Hub, "zw1")
	assert.Equal(t, len(te.nodeStats.hints), 3)
	te.checkZkSynced()

	te.nodeStats.UpdateStats(map[string]*NodePing{
		"node3": {
			true,
			"ZW",
			utils.FromHostPort("127.0.0.3:1001"),
			"10001",
			2003,
			"",
		},
	})
	assert.Assert(t, te.nodeStats.MustGetNodeInfo("node3").Hub != "zw1")
	assert.Equal(t, len(te.nodeStats.hints), 3)
	te.checkZkSynced()

	te.nodeStats.UpdateStats(map[string]*NodePing{
		"node4": {
			true,
			"ZW",
			utils.FromHostPort("127.0.0.4:1003"),
			"10001",
			2003,
			"",
		},
	})
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node4").Hub, "zw1")
	assert.Equal(t, len(te.nodeStats.hints), 2)
	te.checkZkSynced()

	ans := te.nodeStats.RemoveHints([]*pb.RpcNode{{NodeName: "127.0.0.5", Port: 1003}}, true)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Equal(t, len(te.nodeStats.hints), 2)
	te.checkZkSynced()

	ans = te.nodeStats.RemoveHints([]*pb.RpcNode{{NodeName: "127.0.0.5", Port: 1003}}, false)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(te.nodeStats.hints), 1)
	te.checkZkSynced()

	ans = te.nodeStats.RemoveHints([]*pb.RpcNode{{NodeName: "127.0.0.3", Port: 1003}}, false)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kInvalidParameter))
	assert.Equal(t, len(te.nodeStats.hints), 1)
	te.checkZkSynced()

	ans = te.nodeStats.RemoveHints([]*pb.RpcNode{{NodeName: "127.0.0.3", Port: 1003}}, true)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))
	assert.Equal(t, len(te.nodeStats.hints), 0)
	te.checkZkSynced()
}

func TestTwoNodesMatchSameHint(t *testing.T) {
	te := setupNodeStatTestEnv(
		t,
		map[string]string{"zw1": "ZW", "zw2": "ZW"},
		pb.NodeFailureDomainType_PROCESS,
	)
	te.nodeStats.WithSkipHintCheck(true)
	defer te.teardown()

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()

	hint := map[string]*pb.NodeHints{
		"127.0.0.1:1003": {Hub: "zw1"},
	}
	te.nodeStats.AddHints(hint, false)
	assert.Equal(t, len(te.nodeStats.hints), 1)

	te.nodeStats.UpdateStats(map[string]*NodePing{
		"node1": {
			true,
			"ZW",
			utils.FromHostPort("127.0.0.1:1001"),
			"10001",
			2003,
			"",
		},
		"node2": {
			true,
			"ZW",
			utils.FromHostPort("127.0.0.1:1002"),
			"10002",
			2004,
			"",
		},
	})
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node1").Hub, "zw1")
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2").Hub, "zw1")
	assert.Equal(t, len(te.nodeStats.hints), 0)
	te.checkZkSynced()
}

func TestHostHintWithHostFailureDomain(t *testing.T) {
	te := setupNodeStatTestEnv(
		t,
		map[string]string{"zw1": "ZW", "zw2": "ZW"},
		pb.NodeFailureDomainType_HOST,
	)
	te.nodeStats.WithSkipHintCheck(true)
	defer te.teardown()

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()

	hint := map[string]*pb.NodeHints{
		"127.0.0.1:1003": {Hub: "zw1"},
	}
	te.nodeStats.AddHints(hint, false)
	assert.Equal(t, len(te.nodeStats.hints), 1)

	te.nodeStats.UpdateStats(map[string]*NodePing{
		"node1": {
			true,
			"ZW",
			utils.FromHostPort("127.0.0.1:1001"),
			"10001",
			2003,
			"",
		},
	})
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node1").Hub, "zw1")
	assert.Equal(t, len(te.nodeStats.hints), 0)

	te.nodeStats.UpdateStats(map[string]*NodePing{
		"node2": {
			true,
			"ZW",
			utils.FromHostPort("127.0.0.1:1002"),
			"10002",
			2004,
			"",
		},
	})
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2").Hub, "zw1")
	te.checkZkSynced()
}
