package actions

import (
	"fmt"
	"sort"
	"strings"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
)

type ActionResult int

const (
	RunActionOk ActionResult = iota
	ActionShouldPending
	ActionShouldAbort
)

type PartitionAction interface {
	Action(p ActionAcceptor) ActionResult
	String() string
}

type AddLearnerAction struct {
	Node string
}

func (a *AddLearnerAction) Action(p ActionAcceptor) ActionResult {
	return p.AddLearner(a.Node)
}

func (a *AddLearnerAction) String() string {
	return fmt.Sprintf("add_learner %s", a.Node)
}

type RemoveLearnerAction struct {
	Node string
}

func (r *RemoveLearnerAction) Action(p ActionAcceptor) ActionResult {
	p.RemoveLearner(r.Node)
	return RunActionOk
}

func (r *RemoveLearnerAction) String() string {
	return fmt.Sprintf("remove_learner %s", r.Node)
}

type RemoveNodeAction struct {
	Node string
}

func (r *RemoveNodeAction) Action(p ActionAcceptor) ActionResult {
	p.RemoveNode(r.Node)
	return RunActionOk
}

func (r *RemoveNodeAction) String() string {
	return fmt.Sprintf("remove_node %s", r.Node)
}

type TransformAction struct {
	Node   string
	ToRole pb.ReplicaRole
}

func (t *TransformAction) Action(p ActionAcceptor) ActionResult {
	p.Transform(t.Node, t.ToRole)
	return RunActionOk
}

func (t *TransformAction) String() string {
	return fmt.Sprintf("transform %s %s", t.Node, t.ToRole.String())
}

type PromoteToSecondaryAction struct {
	Node string
}

func (t *PromoteToSecondaryAction) Action(p ActionAcceptor) ActionResult {
	return p.PromoteLearner(t.Node)
}

func (t *PromoteToSecondaryAction) String() string {
	return fmt.Sprintf("promote_secondary %s", t.Node)
}

type AtomicSwitchPrimaryAction struct {
	FromNode string
	ToNode   string
}

func (a *AtomicSwitchPrimaryAction) Action(p ActionAcceptor) ActionResult {
	return p.AtomicSwitchPrimary(a.FromNode, a.ToNode)
}

func (a *AtomicSwitchPrimaryAction) String() string {
	return fmt.Sprintf("atomic_switch_primary from(%s) to(%s)", a.FromNode, a.ToNode)
}

type CreateMembersAction struct {
	Members table_model.PartitionMembership
}

func (a *CreateMembersAction) Action(p ActionAcceptor) ActionResult {
	p.CreateMembers(&a.Members)
	return RunActionOk
}

func (a *CreateMembersAction) String() string {
	out := []string{}
	for node, role := range a.Members.Peers {
		out = append(out, fmt.Sprintf("%s:%d", node, role))
	}
	sort.Strings(out)
	return fmt.Sprintf("create members %s", strings.Join(out, ","))
}
