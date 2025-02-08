package node_mgr

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/sd"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

func MockedKessUpdateBizPort(server *sd.DiscoveryMockServer, hostPort string, bizPort int) {
	server.UpdatePayload(hostPort, func(old string) string {
		payload := &utils.PartitionServicePayload{}
		utils.UnmarshalJsonOrDie([]byte(old), &payload)
		payload.BizPort = int32(bizPort)
		return string(utils.MarshalJsonOrDie(payload))
	})
}

func MockedKessUpdateNodePing(server *sd.DiscoveryMockServer, nodeId string, ping *NodePing) {
	server.UpdateServiceNode(ping.ToServiceNode(nodeId))
}

func MockedKessUpdateProcessId(server *sd.DiscoveryMockServer, hostPort string, processId int64) {
	server.UpdatePayload(hostPort, func(old string) string {
		payload := &utils.PartitionServicePayload{}
		utils.UnmarshalJsonOrDie([]byte(old), &payload)
		payload.ProcessId = processId
		return string(utils.MarshalJsonOrDie(payload))
	})
}

func MockedKessUpdateNodeId(server *sd.DiscoveryMockServer, hostPort string, nodeId string) {
	server.UpdatePayload(hostPort, func(old string) string {
		payload := &utils.PartitionServicePayload{}
		utils.UnmarshalJsonOrDie([]byte(old), &payload)
		payload.NodeId = nodeId
		return string(utils.MarshalJsonOrDie(payload))
	})
}
