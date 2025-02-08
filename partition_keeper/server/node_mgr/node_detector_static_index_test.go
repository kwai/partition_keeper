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

func TestNodeIndexErasedByNonIndexedService(t *testing.T) {
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
			"node1": {true, az, utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, "0.0"},
			"node2": {true, az, utils.FromHostPort("127.0.0.1:1002"), "12342", 2002, "0.1"},
			"node3": {true, az, utils.FromHostPort("127.0.0.1:1003"), "12343", 2003, "0.2"},
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

		for _, node := range nodes {
			node.NodeIndex = ""
		}
		select {
		case got := <-event:
			assert.DeepEqual(t, nodes, got)
		default:
			assert.Assert(t, false)
		}
	}
}

func TestHandleNodeIndex(t *testing.T) {
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
		logging.Info("test invalid node index")
		nodes := map[string]*NodePing{
			"node1": {true, az, utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, "0.a"},
			"node2": {true, az, utils.FromHostPort("127.0.0.1:1002"), "12342", 2002, "0.1"},
			"node3": {
				true,
				az,
				utils.FromHostPort("127.0.0.1:1003"),
				"12343",
				2003,
				"hello_world",
			},
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
		event := detector.Start(nil, true, true, usePaz)
		time.Sleep(time.Millisecond * 500)
		detector.updateNodeList()

		select {
		case <-event:
			assert.Assert(t, false)
		default:
			assert.Assert(t, true)
		}

		logging.Info("test conflict node index")
		nodes["node1"].NodeIndex = "0.0"
		nodes["node2"].NodeIndex = "0.1"
		nodes["node3"].NodeIndex = "0.1"
		for id, node := range nodes {
			MockedKessUpdateNodePing(mockedKessServer, id, node)
		}

		detector.updateNodeList()
		select {
		case <-event:
			assert.Assert(t, false)
		default:
			assert.Assert(t, true)
		}

		logging.Info("test detect succeed with node index")
		nodes["node1"].NodeIndex = "0.0"
		nodes["node2"].NodeIndex = "0.1"
		nodes["node3"].NodeIndex = "0.2"
		for id, node := range nodes {
			MockedKessUpdateNodePing(mockedKessServer, id, node)
		}
		detector.updateNodeList()

		select {
		case got := <-event:
			assert.DeepEqual(t, got, nodes)
		default:
			assert.Assert(t, false)
		}
	}
}
