package actions

import "github.com/kuaishou/open_partition_keeper/partition_keeper/pb"

type PartitionActions struct {
	steps []PartitionAction
}

func (p *PartitionActions) ActionCount() int {
	return len(p.steps)
}

func (p *PartitionActions) GetAction(index int) PartitionAction {
	return p.steps[index]
}

func (p *PartitionActions) GetAllActions() []PartitionAction {
	return p.steps
}

func (p *PartitionActions) AddAction(act PartitionAction) {
	p.steps = append(p.steps, act)
}

func (p *PartitionActions) HasAction() bool {
	return len(p.steps) > 0
}

func (p *PartitionActions) ConsumeAction(part ActionAcceptor) bool {
	switch p.steps[0].Action(part) {
	case RunActionOk:
		p.steps = p.steps[1:]
		return true
	case ActionShouldAbort:
		p.steps = nil
		return false
	default:
		return false
	}
}

func MakeActions(actions ...PartitionAction) *PartitionActions {
	output := &PartitionActions{nil}
	output.steps = append(output.steps, actions...)
	return output
}

func SwitchPrimary(from, to string) *PartitionActions {
	output := &PartitionActions{}
	output.AddAction(&AtomicSwitchPrimaryAction{from, to})
	return output
}

func TransferPrimary(from, to string) *PartitionActions {
	output := &PartitionActions{}

	output.AddAction(&AddLearnerAction{to})
	output.AddAction(&PromoteToSecondaryAction{to})
	output.AddAction(&AtomicSwitchPrimaryAction{from, to})
	output.AddAction(&TransformAction{from, pb.ReplicaRole_kLearner})
	output.AddAction(&RemoveLearnerAction{from})

	return output
}

func TransferSecondary(from, to string, removeFirst bool) *PartitionActions {
	output := &PartitionActions{}

	if removeFirst {
		output.AddAction(&TransformAction{from, pb.ReplicaRole_kLearner})
		output.AddAction(&RemoveLearnerAction{from})
		output.AddAction(&AddLearnerAction{to})
		output.AddAction(&PromoteToSecondaryAction{to})
	} else {
		output.AddAction(&AddLearnerAction{to})
		output.AddAction(&PromoteToSecondaryAction{to})
		output.AddAction(&TransformAction{from, pb.ReplicaRole_kLearner})
		output.AddAction(&RemoveLearnerAction{from})
	}

	return output
}

func TransferLearner(from, to string, removeFirst bool) *PartitionActions {
	output := &PartitionActions{}

	if removeFirst {
		output.AddAction(&RemoveLearnerAction{from})
		output.AddAction(&AddLearnerAction{to})
	} else {
		output.AddAction(&AddLearnerAction{to})
		output.AddAction(&RemoveLearnerAction{from})
	}

	return output
}

func RemoveLearner(from string) *PartitionActions {
	output := &PartitionActions{}
	output.AddAction(&RemoveLearnerAction{from})
	return output
}

func FluentRemoveNode(from string) *PartitionActions {
	output := &PartitionActions{}
	output.AddAction(&TransformAction{from, pb.ReplicaRole_kLearner})
	output.AddAction(&RemoveLearnerAction{from})
	return output
}

func AddSecondary(node string) *PartitionActions {
	output := &PartitionActions{}
	output.AddAction(&AddLearnerAction{node})
	output.AddAction(&PromoteToSecondaryAction{node})
	return output
}
