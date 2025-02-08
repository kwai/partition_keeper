package pb

import (
	"fmt"
	"strings"
)

func (n *RpcNode) ToHostPort() string {
	return fmt.Sprintf("%s:%d", n.NodeName, n.Port)
}

func (n *RpcNode) Equal(n2 *RpcNode) bool {
	return n.Port == n2.Port && n.NodeName == n2.NodeName
}

func (n *RpcNode) Compare(n2 *RpcNode) int {
	if ans := strings.Compare(n.NodeName, n2.NodeName); ans != 0 {
		return ans
	}
	return int(n.Port) - int(n2.Port)
}

func RoleInServing(role ReplicaRole) bool {
	return role == ReplicaRole_kPrimary || role == ReplicaRole_kSecondary
}

func ErrStatusOk() *ErrorStatus {
	return &ErrorStatus{Code: 0}
}

func AdminErrorCode(code AdminError_Code) *ErrorStatus {
	return &ErrorStatus{
		Code:    int32(code),
		Message: AdminError_Code_name[int32(code)],
	}
}

func AdminErrorMsg(code AdminError_Code, format string, args ...interface{}) *ErrorStatus {
	return &ErrorStatus{
		Code:    int32(code),
		Message: fmt.Sprintf(format, args...),
	}
}

func PartitionErrorCode(code PartitionError_Code) *ErrorStatus {
	return &ErrorStatus{
		Code:    int32(code),
		Message: PartitionError_Code_name[int32(code)],
	}
}

func PartitionErrorMsg(code PartitionError_Code, format string, args ...interface{}) *ErrorStatus {
	return &ErrorStatus{
		Code:    int32(code),
		Message: fmt.Sprintf(format, args...),
	}
}

func TableManagerErrorMsg(
	code TablesManagerError_Code,
	format string,
	args ...interface{},
) *ErrorStatus {
	return &ErrorStatus{
		Code:    int32(code),
		Message: fmt.Sprintf(format, args...),
	}
}

func RemoveOtherPeers(info *PartitionPeerInfo, node *RpcNode) bool {
	for i := 0; i < len(info.Peers); i++ {
		if info.Peers[i].Node.Equal(node) {
			info.Peers[0] = info.Peers[i]
			info.Peers = info.Peers[0:1]
			return true
		}
	}
	return false
}

func (info *PartitionPeerInfo) FindPeer(node *RpcNode) *PartitionReplica {
	for i := 0; i < len(info.Peers); i++ {
		if info.Peers[i].Node.Equal(node) {
			return info.Peers[i]
		}
	}
	return nil
}

func (b *NodeBrief) ReplicaCount() int {
	return int(b.PrimaryCount + b.SecondaryCount + b.LearnerCount)
}

func (st *ServiceType) Set(val string) error {
	if t, ok := ServiceType_value[val]; !ok {
		var allTypes []string
		for _, item := range ServiceType_name {
			allTypes = append(allTypes, item)
		}
		return fmt.Errorf("unrecognized service type %s, should be one of %v", val, allTypes)
	} else {
		*st = ServiceType(t)
	}
	return nil
}

func (s *ServerLocation) ToHostPort() string {
	return fmt.Sprintf("%s:%d", s.Host, s.Port)
}

func (r ReplicaRole) IsServing() bool {
	return r == ReplicaRole_kPrimary || r == ReplicaRole_kSecondary
}

func (r *ExpandAzsRequest) GetExpandMap() map[string]int32 {
	output := map[string]int32{}
	for _, opt := range r.AzOptions {
		output[opt.Az] = opt.NewSize
	}
	return output
}

func (r *ExpandAzsRequest) SetExpandMap(input map[string]int32) {
	r.AzOptions = []*ExpandAzsRequest_AzOption{}
	for az, size := range input {
		r.AzOptions = append(
			r.AzOptions,
			&ExpandAzsRequest_AzOption{Az: az, NewSize: size},
		)
	}
}
