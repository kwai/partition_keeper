package node_mgr

import (
	"fmt"
	"reflect"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/est"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type WeightType = float32

const (
	INVALID_WEIGHT         = WeightType(0.0)
	MIN_WEIGHT             = WeightType(0.1)
	DEFAULT_WEIGHT         = WeightType(1.0)
	MAX_WEIGHT             = WeightType(10.0)
	INVALID_WEIGHTED_SCORE = est.ScoreType(0)
)

type NodeInfo struct {
	Id     string         `json:"id"`
	Op     pb.AdminNodeOp `json:"op"`
	Hub    string         `json:"hub"`
	Weight WeightType     `json:"weight"`
	Score  est.ScoreType  `json:"score"`
	NodePing

	resource utils.HardwareUnit
}

func (n *NodeInfo) SetResource(logName string, res utils.HardwareUnit) {
	if !reflect.DeepEqual(n.resource, res) {
		logging.Info("%s: %s resource update %v -> %v", logName, n.LogStr(), n.resource, res)
	}
	n.resource = res
}

func (n *NodeInfo) GetResource() utils.HardwareUnit {
	return n.resource
}

// only used for unit test to do deep equal
func (n *NodeInfo) Equal(another *NodeInfo) bool {
	return n.Id == another.Id && n.Op == another.Op && n.Hub == another.Hub &&
		reflect.DeepEqual(n.NodePing, another.NodePing)
}

func (n *NodeInfo) Normal() bool {
	return n.IsAlive && n.Op == pb.AdminNodeOp_kNoop
}

func (n *NodeInfo) Servable() bool {
	if n.Op == pb.AdminNodeOp_kOffline {
		return false
	}
	if n.IsAlive {
		return true
	}
	return n.Op == pb.AdminNodeOp_kRestart
}

func (n *NodeInfo) OfflineDead() bool {
	return n.Op == pb.AdminNodeOp_kOffline && !n.IsAlive
}

func (n *NodeInfo) GetWeight() WeightType {
	if n.Weight < MIN_WEIGHT {
		return DEFAULT_WEIGHT
	} else if n.Weight > MAX_WEIGHT {
		return MAX_WEIGHT
	} else {
		return n.Weight
	}
}

// max_value of WeightedScore is 10.0 * 10000
func (n *NodeInfo) WeightedScore() est.ScoreType {
	if n.Score == est.INVALID_SCORE {
		return INVALID_WEIGHTED_SCORE
	}
	val := int32(WeightType(n.Score)*n.GetWeight() + 0.5)
	if val < 1 {
		return 1
	} else {
		return val
	}
}

func (n *NodeInfo) AllowRole(hubs []*pb.ReplicaHub, given pb.ReplicaRole) bool {
	for _, hub := range hubs {
		if hub.Name == n.Hub {
			for _, role := range hub.DisallowedRoles {
				if role == given {
					return false
				}
			}
			return true
		}
	}
	return false
}

func (n *NodeInfo) LogStr() string {
	return fmt.Sprintf("%s@%s", n.Address.String(), n.Id)
}

func (n *NodeInfo) Clone() *NodeInfo {
	result := *n
	result.Address = n.Address.Clone()
	return &result
}

func (n *NodeInfo) sameFailureDomain(n2 *NodeInfo, fd pb.NodeFailureDomainType) bool {
	if fd == pb.NodeFailureDomainType_PROCESS {
		return n.Address.Equals(n2.Address)
	}
	// TODO: support rack level failure domain
	return n.Address.NodeName == n2.Address.NodeName
}

type NodeFilter func(info *NodeInfo) bool

func ReverseFilter(f NodeFilter) NodeFilter {
	return func(info *NodeInfo) bool {
		return !f(info)
	}
}

func GetAliveNode(info *NodeInfo) bool {
	return info.IsAlive
}

func GetDeadNode(info *NodeInfo) bool {
	return !info.IsAlive
}

func GetNodeForHub(hub string) NodeFilter {
	return func(info *NodeInfo) bool {
		return info.Hub == hub
	}
}

func GetNodeAdminByOp(op pb.AdminNodeOp) NodeFilter {
	return func(info *NodeInfo) bool {
		return info.Op == op
	}
}

func ExcludeNodeAdminByOp(op pb.AdminNodeOp) NodeFilter {
	return func(info *NodeInfo) bool {
		return info.Op != op
	}
}

func ExcludeFrom(members map[string]pb.ReplicaRole) NodeFilter {
	return func(info *NodeInfo) bool {
		_, ok := members[info.Id]
		return !ok
	}
}

func ExcludeNodeByRole(membership map[string]pb.ReplicaRole, want pb.ReplicaRole) NodeFilter {
	return func(info *NodeInfo) bool {
		if role, ok := membership[info.Id]; !ok {
			return false
		} else {
			return role != want
		}
	}
}

func GetNodeByRole(membership map[string]pb.ReplicaRole, want pb.ReplicaRole) NodeFilter {
	return func(info *NodeInfo) bool {
		if role, ok := membership[info.Id]; !ok {
			return false
		} else {
			return role == want
		}
	}
}

func CountNodes(infos map[string]*NodeInfo, filters ...NodeFilter) int {
	ans := 0
	for _, info := range infos {
		passFilters := true
		for _, f := range filters {
			if !f(info) {
				passFilters = false
				break
			}
		}
		if passFilters {
			ans++
		}
	}
	return ans
}

func FindMinMax(infos map[string][]*NodeInfo) (min, max string, minVal, maxVal int) {
	minVal = 0x7fffffff
	maxVal = -1

	for name, info := range infos {
		cnt := len(info)
		if cnt < minVal {
			minVal = cnt
			min = name
		}
		if cnt > maxVal {
			maxVal = cnt
			max = name
		}
	}
	return
}

func PopNodesWithinFailureDomain(
	nodes []*NodeInfo,
	info *NodeInfo,
	fd pb.NodeFailureDomainType,
) (popped, remained []*NodeInfo) {
	for _, n := range nodes {
		if n.sameFailureDomain(info, fd) {
			popped = append(popped, n.Clone())
		} else {
			remained = append(remained, n)
		}
	}
	return
}

func MapNodeInfos(nodes []*NodeInfo) map[string]*NodeInfo {
	output := map[string]*NodeInfo{}
	for _, n := range nodes {
		output[n.Id] = n
	}
	return output
}
