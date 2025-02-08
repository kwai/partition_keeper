package node_mgr

import (
	"fmt"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/sd"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"gotest.tools/assert"
)

func TestNodePing(t *testing.T) {
	np := &NodePing{
		true,
		"YZ",
		&utils.RpcNode{NodeName: "127.0.0.1", Port: 1001},
		"12356",
		2001,
		"1.2",
	}
	hub, sub := np.ExtractNodeIndex()
	assert.Equal(t, hub, "YZ_1")
	assert.Equal(t, sub, 2)

	np.NodeIndex = ""
	hub, sub = np.ExtractNodeIndex()
	assert.Equal(t, hub, "")
	assert.Equal(t, sub, -1)
}

func TestReadKessFail(t *testing.T) {
	usePazs := []bool{false, true}
	for _, usePaz := range usePazs {
		sd := sd.NewServiceDiscovery(
			sd.SdTypeDirectUrl,
			"test",
			sd.DirectSdGivenUrl("http://127.0.0.1:4444"),
			sd.DirectSdUsePaz(usePaz),
		)
		detector := NewNodeDetector(
			"test",
			WithServiceDiscovery(sd),
			WithPollKessIntervalSecs(10000000),
		)
		defer detector.Stop()

		hubs := []*pb.ReplicaHub{
			{Az: "yz1", Name: "yz1"},
			{Az: "yz2", Name: "yz2"},
			{Az: "yz3", Name: "yz3"},
		}
		if usePaz {
			hubs = []*pb.ReplicaHub{
				{Az: "HB1AZ1", Name: "az1"},
				{Az: "HB1AZ2", Name: "az2"},
				{Az: "HB1AZ3", Name: "az3"},
			}
		}
		err := detector.UpdateHubs(hubs)
		assert.NilError(t, err)

		pings := map[string]*NodePing{
			"node1": {true, "yz1", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
			"node2": {true, "yz2", utils.FromHostPort("127.0.0.1:1002"), "12342", 2002, ""},
			"node3": {true, "yz3", utils.FromHostPort("127.0.0.1:1003"), "12343", 2003, ""},
		}
		if usePaz {
			pings = map[string]*NodePing{
				"node1": {true, "HB1AZ1", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
				"node2": {true, "HB1AZ2", utils.FromHostPort("127.0.0.1:1002"), "12342", 2002, ""},
				"node3": {true, "HB1AZ3", utils.FromHostPort("127.0.0.1:1003"), "12343", 2003, ""},
			}
		}
		eventChan := detector.Start(pings, false, true, usePaz)
		detector.updateNodeList()
		select {
		case <-eventChan:
			assert.Assert(t, false)
		default:
			assert.Assert(t, true)
		}
		assert.Equal(t, len(detector.activeList), 3)
	}
}

func TestPollerDisabled(t *testing.T) {
	usePazs := []bool{false, true}
	ports := []int{4444, 4466}
	for i, usePaz := range usePazs {
		mockedKessServer := sd.NewDiscoveryMockServer()
		mockedKessServer.Start(ports[i])
		defer mockedKessServer.Stop()

		az := "YZ"
		if usePaz {
			az = "HB1AZ1"
		}
		nodes := map[string]*NodePing{
			"node1": {true, az, utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
			"node2": {true, az, utils.FromHostPort("127.0.0.1:1002"), "12342", 2002, ""},
			"node3": {true, az, utils.FromHostPort("127.0.0.1:1003"), "12343", 2003, ""},
		}
		for id, node := range nodes {
			MockedKessUpdateNodePing(mockedKessServer, id, node)
		}

		logging.Info("start detector")
		sd := sd.NewServiceDiscovery(
			sd.SdTypeDirectUrl,
			"test",
			sd.DirectSdGivenUrl(fmt.Sprintf("http://127.0.0.1:%d", ports[i])),
			sd.DirectSdUsePaz(usePaz),
		)
		detector := NewNodeDetector(
			"test",
			WithServiceDiscovery(sd),
			WithPollKessIntervalSecs(10000000),
		)
		defer detector.Stop()

		err := detector.UpdateHubs([]*pb.ReplicaHub{{Name: az, Az: az}})
		assert.NilError(t, err)
		event := detector.Start(nil, false, false, usePaz)
		time.Sleep(time.Millisecond * 500)
		detector.updateNodeList()

		logging.Info("poller disabled, can't get anything")
		select {
		case <-event:
			assert.Assert(t, false)
		default:
			assert.Assert(t, true)
		}

		logging.Info("enable poller, can get node list")
		detector.EnablePoller(true)
		detector.updateNodeList()
		select {
		case got := <-event:
			assert.DeepEqual(t, got, nodes)
		default:
			assert.Assert(t, false)
		}

		logging.Info("disable poller, can't get node again")
		addNode := map[string]*NodePing{
			"node4": {true, az, utils.FromHostPort("127.0.0.1:1004"), "12341", 2001, ""},
		}
		MockedKessUpdateNodePing(mockedKessServer, "node4", addNode["node4"])
		detector.EnablePoller(false)
		detector.updateNodeList()
		select {
		case <-event:
			assert.Assert(t, false)
		default:
			assert.Assert(t, true)
		}

		logging.Info("enable poller, can get node list")
		detector.EnablePoller(true)
		detector.updateNodeList()
		select {
		case got := <-event:
			assert.DeepEqual(t, got, addNode)
		default:
			assert.Assert(t, false)
		}
	}
}

func TestReadInvalidDataFromKess(t *testing.T) {
	usePazs := []bool{false, true}
	ports := []int{4444, 4466}
	for i, usePaz := range usePazs {
		mockedKessServer := sd.NewDiscoveryMockServer()
		mockedKessServer.Start(ports[i])
		defer mockedKessServer.Stop()

		az := "YZ"
		if usePaz {
			az = "HB1AZ1"
		}
		nodes := map[string]*NodePing{
			"node1": {true, az, utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
			"node2": {true, az, utils.FromHostPort("127.0.0.1:1002"), "12342", 2002, ""},
			"node3": {true, az, utils.FromHostPort("127.0.0.1:1003"), "12343", 2003, ""},
		}

		for id, node := range nodes {
			MockedKessUpdateNodePing(mockedKessServer, id, node)
		}

		logging.Info("start detector")
		sd := sd.NewServiceDiscovery(
			sd.SdTypeDirectUrl,
			"test",
			sd.DirectSdGivenUrl(fmt.Sprintf("http://127.0.0.1:%d", ports[i])),
			sd.DirectSdUsePaz(usePaz),
		)
		detector := NewNodeDetector(
			"test",
			WithServiceDiscovery(sd),
			WithPollKessIntervalSecs(10000000),
		)
		defer detector.Stop()

		err := detector.UpdateHubs([]*pb.ReplicaHub{{Name: az, Az: az}})
		assert.NilError(t, err)
		event := detector.Start(nil, false, true, usePaz)
		time.Sleep(time.Millisecond * 500)
		detector.updateNodeList()

		logging.Info("wait event")
		select {
		case got := <-event:
			assert.DeepEqual(t, nodes, got)
		default:
			assert.Assert(t, false)
		}

		logging.Info("test can't read data from kess")
		for _, ping := range nodes {
			mockedKessServer.RemoveNode(ping.Address.String())
		}

		detector.updateNodeList()
		assert.Equal(t, len(detector.activeList), 3)
		select {
		case <-event:
			assert.Assert(t, false)
		default:
			assert.Assert(t, true)
		}

		logging.Info("test no rpc entry")
		for id, node := range nodes {
			MockedKessUpdateNodePing(mockedKessServer, id, node)
		}
		mockedKessServer.CleanRpcEntries("127.0.0.1:1001")
		detector.updateNodeList()
		assert.Equal(t, len(detector.activeList), 3)
		select {
		case <-event:
			assert.Assert(t, false)
		default:
			assert.Assert(t, true)
		}

		logging.Info("test invalid payload bizport")
		for id, node := range nodes {
			MockedKessUpdateNodePing(mockedKessServer, id, node)
		}
		MockedKessUpdateBizPort(mockedKessServer, "127.0.0.1:1001", 0)
		detector.updateNodeList()
		assert.Equal(t, len(detector.activeList), 3)
		select {
		case <-event:
			assert.Assert(t, false)
		default:
			assert.Assert(t, true)
		}

		logging.Info("test invalid payload node id")
		for id, node := range nodes {
			MockedKessUpdateNodePing(mockedKessServer, id, node)
		}
		MockedKessUpdateNodeId(mockedKessServer, "127.0.0.1:1001", "")
		detector.updateNodeList()
		assert.Equal(t, len(detector.activeList), 3)
		select {
		case <-event:
			assert.Assert(t, false)
		default:
			assert.Assert(t, true)
		}

		logging.Info("test invalid payload process id")
		for id, node := range nodes {
			MockedKessUpdateNodePing(mockedKessServer, id, node)
		}
		MockedKessUpdateProcessId(mockedKessServer, "127.0.0.1:1001", 0)
		detector.updateNodeList()
		assert.Equal(t, len(detector.activeList), 3)
		select {
		case <-event:
			assert.Assert(t, false)
		default:
			assert.Assert(t, true)
		}

		logging.Info("test node id conflict")
		for id, node := range nodes {
			MockedKessUpdateNodePing(mockedKessServer, id, node)
		}
		MockedKessUpdateNodeId(mockedKessServer, "127.0.0.1:1001", "node3")
		detector.updateNodeList()
		assert.Equal(t, len(detector.activeList), 3)
		select {
		case <-event:
			assert.Assert(t, false)
		default:
			assert.Assert(t, true)
		}
	}
}

func TestUpdateAz(t *testing.T) {
	usePazs := []bool{false, true}
	ports := []int{4444, 4466}
	for i, usePaz := range usePazs {
		mockedKessServer := sd.NewDiscoveryMockServer()
		mockedKessServer.Start(ports[i])
		defer mockedKessServer.Stop()

		nodes := map[string]*NodePing{
			"node1": {true, "YZ", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
			"node2": {true, "ZW", utils.FromHostPort("127.0.0.1:1002"), "12342", 2002, ""},
			"node3": {true, "GZ", utils.FromHostPort("127.0.0.1:1003"), "12343", 2003, ""},
		}
		if usePaz {
			nodes = map[string]*NodePing{
				"node1": {true, "HB1AZ1", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
				"node2": {true, "HB1AZ2", utils.FromHostPort("127.0.0.1:1002"), "12342", 2002, ""},
				"node3": {true, "HB1AZ3", utils.FromHostPort("127.0.0.1:1003"), "12343", 2003, ""},
			}
		}

		for id, node := range nodes {
			MockedKessUpdateNodePing(mockedKessServer, id, node)
		}

		logging.Info("start detector")
		sd := sd.NewServiceDiscovery(
			sd.SdTypeDirectUrl,
			"test",
			sd.DirectSdGivenUrl(fmt.Sprintf("http://127.0.0.1:%d", ports[i])),
			sd.DirectSdUsePaz(usePaz),
		)
		detector := NewNodeDetector(
			"test",
			WithServiceDiscovery(sd),
			WithPollKessIntervalSecs(10000000),
		)
		defer detector.Stop()

		updateAzs := []*pb.ReplicaHub{
			{Name: "YZ", Az: "YZ"},
			{Name: "ZW", Az: "ZW"},
			{Name: "GZ", Az: "GZ"},
		}
		if usePaz {
			updateAzs = []*pb.ReplicaHub{
				{Name: "HB1AZ1", Az: "HB1AZ1"},
				{Name: "HB1AZ2", Az: "HB1AZ2"},
				{Name: "HB1AZ3", Az: "HB1AZ3"},
			}
		}

		err := detector.UpdateHubs(updateAzs)
		assert.NilError(t, err)
		event := detector.Start(nil, false, true, usePaz)
		time.Sleep(time.Millisecond * 500)
		detector.updateNodeList()

		select {
		case got := <-event:
			assert.DeepEqual(t, nodes, got)
		default:
			assert.Assert(t, false)
		}

		updateAzs = []*pb.ReplicaHub{
			{Name: "YZ", Az: "YZ"},
			{Name: "ZW", Az: "ZW"},
			{Name: "GZ", Az: "GZ"},
			{Name: "SGP", Az: "SGP"},
		}
		if usePaz {
			updateAzs = []*pb.ReplicaHub{
				{Name: "HB1AZ1", Az: "HB1AZ1"},
				{Name: "HB1AZ2", Az: "HB1AZ2"},
				{Name: "HB1AZ3", Az: "HB1AZ3"},
				{Name: "HB1AZ4", Az: "HB1AZ4"},
			}
		}
		err = detector.UpdateHubs(updateAzs)
		assert.NilError(t, err)
		detector.updateNodeList()

		select {
		case <-event:
			assert.Assert(t, false)
		default:
			assert.Assert(t, true)
		}

		updateAzs = []*pb.ReplicaHub{
			{Name: "YZ", Az: "YZ"},
			{Name: "ZW", Az: "ZW"},
			{Name: "SGP", Az: "SGP"},
		}
		if usePaz {
			updateAzs = []*pb.ReplicaHub{
				{Name: "HB1AZ1", Az: "HB1AZ1"},
				{Name: "HB1AZ2", Az: "HB1AZ2"},
				{Name: "HB1AZ4", Az: "HB1AZ4"},
			}
		}

		err = detector.UpdateHubs(updateAzs)
		assert.NilError(t, err)
		detector.updateNodeList()

		select {
		case got := <-event:
			assert.Assert(t, true)
			assert.Equal(t, len(got), 1)
			node := got["node3"]
			assert.Assert(t, node != nil)
			assert.Equal(t, node.IsAlive, false)
		default:
			assert.Assert(t, false)
		}

		delete(nodes, "node3")
		assert.DeepEqual(t, detector.activeList, nodes)
	}
}

func TestDcBlackList(t *testing.T) {
	*flagDcBlackListKconf = "reco.partitionKeeper.testDcBlackList"

	mockedKessServer := sd.NewDiscoveryMockServer()
	mockedKessServer.Start(4444)
	defer mockedKessServer.Stop()

	nodes := map[string]*NodePing{
		"node1": {true, "YZ", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
		"node2": {true, "YZ", utils.FromHostPort("127.0.0.1:1002"), "12342", 2002, ""},
	}

	for id, node := range nodes {
		MockedKessUpdateNodePing(mockedKessServer, id, node)
	}
	mockedKessServer.UpdateKwsInfo(
		"127.0.0.1:1001",
		&pb.KwsInfo{Region: "HB1", Az: "YZ", Dc: "BJXY"},
	)
	mockedKessServer.UpdateKwsInfo(
		"127.0.0.1:1002",
		&pb.KwsInfo{Region: "HB1", Az: "YZ", Dc: "TEST_DC1"},
	)

	logging.Info("start detector")
	sd := sd.NewServiceDiscovery(
		sd.SdTypeDirectUrl,
		"test_service",
		sd.DirectSdGivenUrl("http://127.0.0.1:4444"),
	)
	detector := NewNodeDetector(
		"test_service",
		WithServiceDiscovery(sd),
		WithPollKessIntervalSecs(10000000),
	)
	defer detector.Stop()

	err := detector.UpdateHubs([]*pb.ReplicaHub{{Name: "YZ", Az: "YZ"}})
	assert.NilError(t, err)
	event := detector.Start(nil, false, true, false)
	time.Sleep(time.Millisecond * 500)
	detector.updateNodeList()

	select {
	case got := <-event:
		assert.Equal(t, len(got), 1)
		assert.DeepEqual(t, got["node1"], nodes["node1"])
	default:
		assert.Assert(t, false)
	}

	detector.serviceName = "replace_test_name"
	detector.updateNodeList()
	select {
	case got := <-event:
		assert.Equal(t, len(got), 1)
		assert.DeepEqual(t, got["node2"], nodes["node2"])
	default:
		assert.Assert(t, false)
	}

	detector.serviceName = "test_service"
	detector.updateNodeList()
	select {
	case got := <-event:
		assert.Equal(t, len(got), 1)
		assert.Equal(t, got["node2"].IsAlive, false)
	default:
		assert.Assert(t, false)
	}
}

func TestNodeInfoChanged(t *testing.T) {
	usePazs := []bool{false, true}
	ports := []int{4444, 4466}
	for i, usePaz := range usePazs {
		mockedKessServer := sd.NewDiscoveryMockServer()
		mockedKessServer.Start(ports[i])
		defer mockedKessServer.Stop()

		az := "YZ"
		if usePaz {
			az = "HB1AZ1"
		}
		nodes := map[string]*NodePing{
			"node1": {true, az, utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
			"node2": {true, az, utils.FromHostPort("127.0.0.1:1002"), "12342", 2002, ""},
			"node3": {true, az, utils.FromHostPort("127.0.0.1:1003"), "12343", 2003, ""},
		}

		for id, node := range nodes {
			MockedKessUpdateNodePing(mockedKessServer, id, node)
		}

		logging.Info("start detector")
		sd := sd.NewServiceDiscovery(
			sd.SdTypeDirectUrl,
			"test",
			sd.DirectSdGivenUrl(fmt.Sprintf("http://127.0.0.1:%d", ports[i])),
			sd.DirectSdUsePaz(usePaz),
		)
		detector := NewNodeDetector(
			"test",
			WithServiceDiscovery(sd),
			WithPollKessIntervalSecs(10000000),
		)
		defer detector.Stop()

		err := detector.UpdateHubs([]*pb.ReplicaHub{{Name: az, Az: az}})
		assert.NilError(t, err)
		event := detector.Start(nil, false, true, usePaz)
		time.Sleep(time.Millisecond * 500)
		detector.updateNodeList()
		select {
		case got := <-event:
			assert.DeepEqual(t, nodes, got)
		default:
			assert.Assert(t, false)
		}

		for _, node := range nodes {
			mockedKessServer.RemoveNode(node.Address.String())
		}

		nodes["node1"].ProcessId = "22341"
		nodes["node2"].BizPort = 2012
		nodes["node3"].Address.Port = 1006

		for id, node := range nodes {
			MockedKessUpdateNodePing(mockedKessServer, id, node)
		}

		detector.updateNodeList()
		nodes["node1"].IsAlive = false
		select {
		case got := <-event:
			assert.DeepEqual(t, got, nodes)
		default:
			assert.Assert(t, false)
		}

		detector.updateNodeList()
		select {
		case got := <-event:
			assert.Equal(t, len(got), 1)
			assert.Equal(t, got["node1"].IsAlive, true)
			assert.Equal(t, got["node1"].ProcessId, "22341")
		default:
			assert.Assert(t, false)
		}

		nodes["node1"].BizPort = 2002
		MockedKessUpdateNodePing(mockedKessServer, "node1", nodes["node1"])
		detector.updateNodeList()
		select {
		case got := <-event:
			assert.Equal(t, len(got), 1)
			assert.Equal(t, got["node1"].IsAlive, true)
			assert.Equal(t, got["node1"].BizPort, int32(2002))
		default:
			assert.Assert(t, false)
		}

		detector.updateNodeList()
		select {
		case <-event:
			assert.Assert(t, false)
		default:
			assert.Assert(t, true)
		}
	}
}
