package actions

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
)

type ActionAcceptor interface {
	AddLearner(node string) ActionResult
	RemoveLearner(node string)
	RemoveNode(node string)
	Transform(node string, role pb.ReplicaRole)
	PromoteLearner(node string) ActionResult
	AtomicSwitchPrimary(from, to string) ActionResult
	CreateMembers(members *table_model.PartitionMembership)
}
