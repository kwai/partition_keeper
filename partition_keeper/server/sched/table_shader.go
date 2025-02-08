package sched

import (
	"fmt"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type tableShader struct {
	subSchedulerBase
	baseTable       table_model.TableModel
	schedForPrimary bool
}

func newTableShader(baseTable table_model.TableModel, schedForPrimary bool) *tableShader {
	return &tableShader{
		baseTable:       baseTable,
		schedForPrimary: schedForPrimary,
	}
}

func (t *tableShader) compareHubs(
	pid int32,
	srcInHubs, dstInHubs map[string][]string,
	_, target *table_model.PartitionMembership,
) bool {
	logPrefix := fmt.Sprintf("%s: %s t%d.p%d", t.logName, t.GetTableName(), t.GetTableId(), pid)
	for _, hub := range t.Hubs {
		srcNodes, dstNodes := srcInHubs[hub.Name], dstInHubs[hub.Name]
		left, _, right := utils.Diff(srcNodes, dstNodes)
		// TODO(huyifan03): handle left > 1, which meaning the base table is in
		// trasfering node, the shaded table should only need to add node in new place
		if len(left) > 0 {
			logging.Info("%s: try to add learner %s", logPrefix, left[0])
			t.addLearner(pid, left[0])
			return true
		}
		if len(right) > 0 {
			remainServing := 0
			for _, node := range dstNodes {
				if node != right[0] && target.GetMember(node).IsServing() {
					remainServing++
				}
			}
			if remainServing > 0 {
				logging.Info(
					"%s: try to remove %s, remainServing: %d, total extra: %d",
					logPrefix,
					right[0],
					remainServing,
					len(right),
				)
				t.removeNode(pid, right[0])
				return true
			}
			// if a hub is not well scheduled, we don't continue with next hub
			logging.Verbose(
				1,
				"%s: %s t%d.p%d don't continue to schedule as hub %s not well scheduled",
				t.logName,
				t.GetTableName(),
				t.GetTableId(),
				pid,
				hub.Name,
			)
			return false
		}
	}
	return false
}

func (t *tableShader) comparePrimary(
	pid int32,
	source, target *table_model.PartitionMembership,
) bool {
	logPrefix := fmt.Sprintf("%s: %s t%d.p%d", t.logName, t.GetTableName(), t.GetTableId(), pid)
	sPri, _, _ := source.DivideRoles()
	tPri, _, _ := target.DivideRoles()
	if len(sPri) == 0 {
		return false
	}
	tRole := target.GetMember(sPri[0])
	switch tRole {
	// source & target primary match
	case pb.ReplicaRole_kPrimary:
		return false
	case pb.ReplicaRole_kSecondary:
		if len(tPri) == 0 { // no primary in target
			logging.Info("%s: try to assign primary to %s", logPrefix, sPri[0])
			t.transform(pid, sPri[0], pb.ReplicaRole_kSecondary, pb.ReplicaRole_kPrimary)
		} else {
			logging.Info("%s: try to transfer primary from %s -> %s", logPrefix, tPri[0], sPri[0])
			t.switchPrimary(pid, tPri[0], sPri[0])
		}
		return true
	default:
		return false
	}
}

func (t *tableShader) schedPartition(pid int32, source, target *table_model.PartitionMembership) {
	if t.HasPlan(pid) {
		return
	}

	if t.schedForPrimary {
		t.comparePrimary(pid, source, target)
	} else {
		srcInHubs, _ := source.DivideByHubs(t.Nodes, t.Hubs)
		dstInHubs, dstOutHubs := target.DivideByHubs(t.Nodes, t.Hubs)
		if t.compareHubs(pid, srcInHubs, dstInHubs, source, target) {
			return
		}
		t.removeExtraHubs(pid, dstOutHubs)
	}
}

func (t *tableShader) Schedule(
	input *ScheduleInput,
	rec *recorder.AllNodesRecorder,
	plan SchedulePlan,
) {
	t.Initialize(input, rec, plan)
	for i := int32(0); i < t.GetPartsCount(); i++ {
		t.schedPartition(i, t.baseTable.GetMembership(i), t.GetMembership(i))
	}
}
