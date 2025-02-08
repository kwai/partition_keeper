package sd

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

func createTestServiceNode(ipPort, location, paz string) *pb.ServiceNode {
	node := utils.FromHostPort(ipPort)
	return &pb.ServiceNode{
		Id:              "grpc:" + ipPort,
		Protocol:        "grpc",
		Host:            node.NodeName,
		Port:            node.Port,
		Payload:         "",
		Weight:          0,
		Shard:           "0",
		Location:        location,
		InitialLiveTime: 0,
		Paz:             paz,
	}
}
