package utils

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
)

var (
	ErrNoNodeId         = errors.New("payload_error: no node_id")
	ErrNoProcessId      = errors.New("payload_error: no process_id")
	ErrNoBizPort        = errors.New("payload_error: no biz_port")
	ErrInvalidNodeIndex = errors.New("invalid node index")
)

type PartitionServicePayload struct {
	NodeId    string `json:"node_id"`
	ProcessId int64  `json:"process_id"`
	BizPort   int32  `json:"biz_port"`
	NodeIndex string `json:"node_index"`
}

func ExtractPayload(n *pb.ServiceNode) (*PartitionServicePayload, error) {
	output := &PartitionServicePayload{}
	err := json.Unmarshal([]byte(n.Payload), output)
	if err != nil {
		return nil, err
	}
	if output.NodeId == "" {
		return nil, ErrNoNodeId
	}
	if output.ProcessId == 0 {
		return nil, ErrNoProcessId
	}
	if output.BizPort == 0 {
		return nil, ErrNoBizPort
	}
	return output, nil
}

func ParseNodeIndex(index string) (int, int, error) {
	var a, b int
	n, err := fmt.Sscanf(index, "%d.%d", &a, &b)
	if err != nil {
		return -1, -1, err
	}
	if n != 2 {
		return -1, -1, ErrInvalidNodeIndex
	}
	return a, b, nil
}
