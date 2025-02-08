package utils

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
)

type RpcNode struct {
	NodeName string `json:"node_name"`
	Port     int32  `json:"port"`
}

// TODO(huyifan03): handle ipv6
func FromHostPort(hostPort string) *RpcNode {
	lastColon := strings.LastIndex(hostPort, ":")
	if lastColon == -1 {
		return nil
	}
	p, err := strconv.Atoi(hostPort[lastColon+1:])
	if err != nil {
		return nil
	}
	return &RpcNode{
		NodeName: hostPort[0:lastColon],
		Port:     int32(p),
	}
}

func FromHostPorts(hostPorts []string) []*RpcNode {
	var output []*RpcNode
	for _, hp := range hostPorts {
		output = append(output, FromHostPort(hp))
	}
	return output
}

func FromPb(r *pb.RpcNode) *RpcNode {
	return &RpcNode{
		NodeName: r.NodeName,
		Port:     r.Port,
	}
}

func FromPbs(prs []*pb.RpcNode) (output []*RpcNode) {
	for _, p := range prs {
		output = append(output, FromPb(p))
	}
	return
}

func (r *RpcNode) String() string {
	return fmt.Sprintf("%s:%d", r.NodeName, r.Port)
}

func (r *RpcNode) Equals(another *RpcNode) bool {
	return r.Port == another.Port && r.NodeName == another.NodeName
}

func (r *RpcNode) Compare(another *RpcNode) int {
	if ans := strings.Compare(r.NodeName, another.NodeName); ans != 0 {
		return ans
	}
	return int(r.Port) - int(another.Port)
}

func (r *RpcNode) ToPb() *pb.RpcNode {
	return &pb.RpcNode{
		NodeName: r.NodeName,
		Port:     r.Port,
	}
}

func (r *RpcNode) Clone() *RpcNode {
	return &RpcNode{
		NodeName: r.NodeName,
		Port:     r.Port,
	}
}
