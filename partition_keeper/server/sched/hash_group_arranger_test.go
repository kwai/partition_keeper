package sched

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/est"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

func TestScheduleSummaryToposort(t *testing.T) {
	{
		logging.Info("single DAG")
		summary := newScheduleSummary(recorder.NewAllNodesRecorder(recorder.NewBriefNode))
		nodeCount := 100
		for i := 0; i < nodeCount-1; i++ {
			sourceNode := fmt.Sprintf("node_%04d", i)
			dstNode := fmt.Sprintf("node_%04d", i+1)
			summary.addTransferDependency(sourceNode, dstNode)
		}
		for i := 0; i < nodeCount*10; i++ {
			sourceIdx := rand.Intn(nodeCount - 1)
			dstIdx := sourceIdx + 1 + rand.Intn(nodeCount-sourceIdx-1)
			sourceNode := fmt.Sprintf("node_%04d", sourceIdx)
			dstNode := fmt.Sprintf("node_%04d", dstIdx)
			summary.addTransferDependency(sourceNode, dstNode)
		}
		assert.Equal(t, summary.topoSortFindTailNode(), "node_0099")
	}

	{
		logging.Info("multiple DAG")
		summary := newScheduleSummary(recorder.NewAllNodesRecorder(recorder.NewBriefNode))
		for _, group := range []string{"g1", "g2"} {
			nodeCount := 100
			for i := 0; i < nodeCount-1; i++ {
				sourceNode := fmt.Sprintf("%s_node_%04d", group, i)
				dstNode := fmt.Sprintf("%s_node_%04d", group, i+1)
				summary.addTransferDependency(sourceNode, dstNode)
			}
			for i := 0; i < nodeCount*10; i++ {
				sourceIdx := rand.Intn(nodeCount - 1)
				dstIdx := sourceIdx + 1 + rand.Intn(nodeCount-sourceIdx-1)
				sourceNode := fmt.Sprintf("%s_node_%04d", group, sourceIdx)
				dstNode := fmt.Sprintf("%s_node_%04d", group, dstIdx)
				summary.addTransferDependency(sourceNode, dstNode)
			}
		}

		node := summary.topoSortFindTailNode()
		assert.Assert(t, node == "g1_node_0099" || node == "g2_node_0099")
	}

	{
		logging.Info("circle exists")
		summary := newScheduleSummary(recorder.NewAllNodesRecorder(recorder.NewBriefNode))
		summary.addTransferDependency(TRANSER_DEP_FAKE_SOURCE, "1")
		summary.addTransferDependency("1", "2")
		summary.addTransferDependency("2", "3")
		summary.addTransferDependency("3", "4")
		summary.addTransferDependency("4", "5")
		summary.addTransferDependency("5", "3")

		node := summary.topoSortFindTailNode()
		assert.Assert(t, node == "3" || node == "4" || node == "5")
	}
}

func TestGroupPartsDecider(t *testing.T) {
	testCases := []struct {
		Name   string
		Parts  int
		Groups int
		Nodes  int
	}{
		{"dp_group1", 36, 12, 8},
		{"dp_group2", 16, 4, 5},
		{"dp_group3", 16, 4, 11},
		{"dp_group4", 18, 6, 4},
		{"dp_group5", 16, 1, 5},
		{"dp_group6", 32, 4, 10},
		{"greedy_group1", 16, 4, 4},
		{"greedy_group2", 16, 8, 18},
	}

	for i := range testCases {
		testCase := &testCases[i]
		decider := newGroupPartsDecider(
			testCase.Name,
			testCase.Parts,
			testCase.Groups,
			testCase.Nodes,
		)
		decider.prepare()
		assert.Equal(t, decider.lowerNodesCnt+decider.upperNodesCnt, testCase.Nodes)

		nodes := []string{}
		for j := 0; j < testCase.Nodes; j++ {
			nodes = append(nodes, fmt.Sprintf("nodes_%d", j))
		}
		output := []briefMembership{}
		for j := 0; j < testCase.Parts; j++ {
			output = append(output, map[string]string{})
		}
		decider.assignPartitions(
			nodes[0:decider.lowerNodesCnt],
			nodes[decider.lowerNodesCnt:],
			"test",
			output,
		)

		outputDist := map[string]int{}
		for j := 0; j < testCase.Parts; j++ {
			assert.Equal(t, len(output[j]), 1)
			outputDist[output[j]["test"]]++
		}
		for j := 0; j < decider.lowerNodesCnt; j++ {
			assert.Equal(t, outputDist[nodes[j]], decider.nodeCapLower)
		}
		for j := 0; j < decider.upperNodesCnt; j++ {
			assert.Equal(t, outputDist[nodes[decider.lowerNodesCnt+j]], decider.nodeCapUpper)
		}
	}
}

func TestHashGroupArrangerTakeActionByHub(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 2, []int{2, 2}, 4)
	input := te.getScheduleInput()
	input.Opts.PartGroupCnt = 2

	logging.Info("take action by hub")
	arranger := &hashGroupArranger{}
	arranger.SetLogName("test-service")

	plan := SchedulePlan{}
	te.updateRecorder()
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 4)
	for _, action := range plan {
		assert.Equal(t, action.ActionCount(), 1)
		assert.Equal(
			t,
			te.nodes.MustGetNodeInfo(action.GetAction(0).(*actions.AddLearnerAction).Node).Hub,
			"yz0",
		)
	}
	te.applyPlan(plan)

	plan = SchedulePlan{}
	te.updateRecorder()
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 4)
	for _, action := range plan {
		assert.Equal(t, action.ActionCount(), 1)
		assert.Equal(
			t,
			te.nodes.MustGetNodeInfo(action.GetAction(0).(*actions.AddLearnerAction).Node).Hub,
			"yz1",
		)
	}
	te.applyPlan(plan)

	plan = SchedulePlan{}
	te.updateRecorder()
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 0)

	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kLearner,
		"node_yz1_0": pb.ReplicaRole_kLearner,
	})
	assert.DeepEqual(t, te.table.GetMembership(1).Peers, map[string]pb.ReplicaRole{
		"node_yz0_1": pb.ReplicaRole_kLearner,
		"node_yz1_1": pb.ReplicaRole_kLearner,
	})
	assert.DeepEqual(t, te.table.GetMembership(2).Peers, map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kLearner,
		"node_yz1_0": pb.ReplicaRole_kLearner,
	})
	assert.DeepEqual(t, te.table.GetMembership(3).Peers, map[string]pb.ReplicaRole{
		"node_yz0_1": pb.ReplicaRole_kLearner,
		"node_yz1_1": pb.ReplicaRole_kLearner,
	})
}

func TestHashGroupArrangerExpandByHub(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 2, []int{2, 2}, 8)
	input := te.getScheduleInput()
	input.Opts.PartGroupCnt = 4

	arranger := &hashGroupArranger{}
	arranger.SetLogName("test-service")

	logging.Info("first schedule partitions on hub1")
	plan := SchedulePlan{}
	te.updateRecorder()
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 8)
	te.applyPlan(plan)

	logging.Info("first schedule partitions on hub2")
	plan = SchedulePlan{}
	te.updateRecorder()
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 8)
	te.applyPlan(plan)

	logging.Info("promote replicas to serving")
	for i := 0; i < int(te.table.PartsCount); i++ {
		assert.Equal(t, len(te.table.Partitions[i].Peers), 2)
		for node, role := range te.table.Partitions[i].Peers {
			assert.Equal(t, role, pb.ReplicaRole_kLearner)
			if te.nodes.MustGetNodeInfo(node).Hub == "yz0" {
				te.table.Partitions[i].Peers[node] = pb.ReplicaRole_kPrimary
			} else {
				te.table.Partitions[i].Peers[node] = pb.ReplicaRole_kSecondary
			}
		}
	}

	logging.Info("add 1 node for every hub, then the arranger will try to rearrange hub1")
	te.addNodes([]int{1, 1})
	plan = SchedulePlan{}
	te.updateRecorder()
	arranger.Schedule(input, te.nr, plan)
	planSize := len(plan)
	assert.Assert(t, planSize > 1)
	te.applyPlan(plan)

	fixedPartitions := 0
	remainedPart := -1
	for i := 0; i < int(te.table.PartsCount); i++ {
		pri, _, learners := te.table.GetMembership(int32(i)).DivideRoles()
		for _, l := range learners {
			assert.Equal(t, te.nodes.MustGetNodeInfo(l).Hub, "yz0")
		}
		// primaries are all at yz0, so rearrange will downgrade primary
		if len(learners) > 0 {
			assert.Equal(t, len(pri), 0)
		}
		if len(learners) > 0 {
			if fixedPartitions+1 < planSize {
				te.table.GetMembership(int32(i)).Peers[learners[0]] = pb.ReplicaRole_kPrimary
				fixedPartitions++
			} else {
				remainedPart = i
			}
		}
	}
	te.updateRecorder()

	logging.Info("hub2 will not be scheduled as long as hub1 is not totally ordered")
	plan = SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 0)

	logging.Info("hub2 will be scheduled until hub1 is ordered")
	members := te.table.GetMembership(int32(remainedPart))
	_, _, learners := members.DivideRoles()
	members.Peers[learners[0]] = pb.ReplicaRole_kPrimary
	te.updateRecorder()

	arranger.Schedule(input, te.nr, plan)
	assert.Assert(t, len(plan) > 0)
	te.applyPlan(plan)
}

func TestHashGroupArrangerLotsNodesInHub(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 1, []int{3}, 1)
	input := te.getScheduleInput()
	input.Opts.PartGroupCnt = 1
	arranger := &hashGroupArranger{}
	arranger.SetLogName("test-service")

	lowerNodes := te.getNodesWithCap(0, "yz0")
	upperNodes := te.getNodesWithCap(1, "yz0")
	assert.Equal(t, len(lowerNodes), 2)
	assert.Equal(t, len(upperNodes), 1)

	logging.Info("1 right-learner, 1 wrong-learner, 1 wrong-sec, only learner removed")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			upperNodes[0]: pb.ReplicaRole_kLearner,
			lowerNodes[0]: pb.ReplicaRole_kSecondary,
			lowerNodes[1]: pb.ReplicaRole_kLearner,
		},
	}
	te.updateRecorder()
	plan := SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		upperNodes[0]: pb.ReplicaRole_kLearner,
		lowerNodes[0]: pb.ReplicaRole_kSecondary,
	})

	te.updateRecorder()
	plan = SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 0)

	logging.Info("1 right leaner, 2 wrong learners removed")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			upperNodes[0]: pb.ReplicaRole_kLearner,
			lowerNodes[0]: pb.ReplicaRole_kLearner,
			lowerNodes[1]: pb.ReplicaRole_kLearner,
		},
	}
	te.updateRecorder()
	plan = SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.Equal(t, len(te.table.GetMembership(0).Peers), 2)
	assert.Equal(t, te.table.GetMembership(0).GetMember(upperNodes[0]), pb.ReplicaRole_kLearner)

	te.updateRecorder()
	plan = SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		upperNodes[0]: pb.ReplicaRole_kLearner,
	})

	te.updateRecorder()
	plan = SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 0)

	logging.Info("1 right secondary, 1 wrong secondary removed")
	*te.table.GetMembership(0) = table_model.PartitionMembership{
		MembershipVersion: 10,
		Peers: map[string]pb.ReplicaRole{
			upperNodes[0]: pb.ReplicaRole_kSecondary,
			lowerNodes[0]: pb.ReplicaRole_kSecondary,
		},
	}
	te.updateRecorder()
	plan = SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		upperNodes[0]: pb.ReplicaRole_kSecondary,
	})
	te.updateRecorder()
	plan = SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 0)
}

func TestHashGroupArranger1NodeInHub(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 3, []int{2, 2, 2}, 1)
	input := te.getScheduleInput()
	input.Opts.PartGroupCnt = 1
	arranger := &hashGroupArranger{}
	arranger.SetLogName("test-service")

	lower := te.getNodesWithCap(0, "yz0")[0]
	upper := te.getNodesWithCap(1, "yz0")[0]

	logging.Info("1 right node, no action")
	input.Hubs = []*pb.ReplicaHub{
		{Name: "yz0", Az: "YZ"},
	}
	te.table.GetMembership(0).Peers = map[string]pb.ReplicaRole{
		upper: pb.ReplicaRole_kLearner,
	}
	te.updateRecorder()
	plan := SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 0)

	logging.Info("1 wrong learner, don't do action if already has one")
	te.table.GetMembership(0).Peers = map[string]pb.ReplicaRole{
		lower: pb.ReplicaRole_kLearner,
	}
	te.updateRecorder()
	plan = SchedulePlan{
		0: actions.MakeActions(
			&actions.TransformAction{Node: lower, ToRole: pb.ReplicaRole_kSecondary},
		),
	}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		lower: pb.ReplicaRole_kSecondary,
	})

	logging.Info("1 wrong learner, transfer to right one")
	te.table.GetMembership(0).Peers = map[string]pb.ReplicaRole{
		lower: pb.ReplicaRole_kLearner,
	}
	te.updateRecorder()
	plan = SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	assert.Equal(t, plan[0].ActionCount(), 2)
	switch plan[0].GetAction(0).(type) {
	case *actions.RemoveLearnerAction:
		assert.Assert(t, true)
	default:
		assert.Assert(t, false)
	}
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		upper: pb.ReplicaRole_kLearner,
	})

	logging.Info("this hub 1 wrong secondary, total enough secondaries, transfer to right one")
	input.Hubs = []*pb.ReplicaHub{
		{Name: "yz0", Az: "YZ"},
		{Name: "yz1", Az: "YZ"},
	}
	te.table.GetMembership(0).Peers = map[string]pb.ReplicaRole{
		lower:        pb.ReplicaRole_kSecondary,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	te.updateRecorder()
	plan = SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	assert.Equal(t, plan[0].GetAction(0).(*actions.TransformAction).Node, lower)
	assert.Equal(t, plan[0].GetAction(0).(*actions.TransformAction).ToRole, pb.ReplicaRole_kLearner)
	switch plan[0].GetAction(1).(type) {
	case *actions.RemoveNodeAction:
		assert.Assert(t, true)
	default:
		assert.Assert(t, false)
	}
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		upper:        pb.ReplicaRole_kLearner,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	})

	logging.Info("this hub 1 wrong secondary, total not enough serving, no action")
	te.table.GetMembership(0).Peers = map[string]pb.ReplicaRole{
		lower:                           pb.ReplicaRole_kSecondary,
		te.getNodesWithCap(1, "yz1")[0]: pb.ReplicaRole_kLearner,
	}
	te.updateRecorder()
	plan = SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 0)
}

func TestHashGroupArrangerRemoveExtraHubs(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 2, []int{1, 1}, 1)
	input := te.getScheduleInput()
	input.Opts.PartGroupCnt = 1
	arranger := &hashGroupArranger{}
	arranger.SetLogName("test-service")

	logging.Info("don't remove if has add action")
	te.table.GetMembership(0).Peers = map[string]pb.ReplicaRole{
		"node_yz1_0": pb.ReplicaRole_kLearner,
	}
	te.updateRecorder()
	input.Hubs = []*pb.ReplicaHub{
		{Name: "yz0", Az: "YZ"},
	}
	plan := SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kLearner,
		"node_yz1_0": pb.ReplicaRole_kLearner,
	})

	logging.Info("remove extra learner")
	te.updateRecorder()
	plan = SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kLearner,
	})

	logging.Info("remove extra secondary")
	te.table.GetMembership(0).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kLearner,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	te.updateRecorder()
	plan = SchedulePlan{}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kLearner,
	})

	logging.Info("don't remove if already has partition action")
	te.table.GetMembership(0).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kLearner,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	}
	te.updateRecorder()
	plan = SchedulePlan{
		0: actions.MakeActions(
			&actions.TransformAction{Node: "node_yz0_0", ToRole: pb.ReplicaRole_kSecondary},
		),
	}
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 1)
	assert.Equal(t, plan[0].GetAction(0).(*actions.TransformAction).Node, "node_yz0_0")
	te.applyPlan(plan)
	assert.DeepEqual(t, te.table.GetMembership(0).Peers, map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kSecondary,
		"node_yz1_0": pb.ReplicaRole_kSecondary,
	})
}

func TestHashGroupArrangerEmptyHub(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 2, []int{0, 2}, 4)
	input := te.getScheduleInput()
	input.Opts.PartGroupCnt = 2
	arranger := &hashGroupArranger{}
	arranger.SetLogName("test-service")

	plan := SchedulePlan{}
	te.updateRecorder()
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 4)
	te.applyPlan(plan)

	assert.DeepEqual(
		t,
		te.table.GetMembership(0).Peers,
		map[string]pb.ReplicaRole{"node_yz1_0": pb.ReplicaRole_kLearner},
	)
	assert.DeepEqual(
		t,
		te.table.GetMembership(1).Peers,
		map[string]pb.ReplicaRole{"node_yz1_1": pb.ReplicaRole_kLearner},
	)
	assert.DeepEqual(
		t,
		te.table.GetMembership(2).Peers,
		map[string]pb.ReplicaRole{"node_yz1_0": pb.ReplicaRole_kLearner},
	)
	assert.DeepEqual(
		t,
		te.table.GetMembership(3).Peers,
		map[string]pb.ReplicaRole{"node_yz1_1": pb.ReplicaRole_kLearner},
	)

	plan = SchedulePlan{}
	te.updateRecorder()
	arranger.Schedule(input, te.nr, plan)
	assert.Equal(t, len(plan), 0)
}

func TestHashGroupArrangerSymmetric(t *testing.T) {
	for _, groups := range []int{4, 8, 16, 32} {
		te := setupTestSchedulerStableEnv(t, 2, []int{15, 15}, 1024)
		input := te.getScheduleInput()
		input.Opts.PartGroupCnt = groups

		arranger := &hashGroupArranger{}
		arranger.SetLogName("test-service")

		for i := 0; i < 2; i++ {
			plan := SchedulePlan{}
			te.updateRecorder()
			arranger.Schedule(input, te.nr, plan)
			assert.Equal(t, len(plan), 1024)
			te.applyPlan(plan)
		}

		te.updateRecorder()
		for i := 0; i < 15; i++ {
			rec1 := te.nr.GetNode(fmt.Sprintf("node_yz0_%d", i)).(*recorder.NodePartitions)
			rec2 := te.nr.GetNode(fmt.Sprintf("node_yz1_%d", i)).(*recorder.NodePartitions)

			assert.Equal(t, rec1.CountAll(), te.table.GetEstimatedReplicasOnNode(rec1.NodeId))
			assert.Equal(t, rec2.CountAll(), te.table.GetEstimatedReplicasOnNode(rec2.NodeId))

			assert.DeepEqual(t, rec1.Primaries, rec2.Primaries)
			assert.DeepEqual(t, rec1.Secondaries, rec2.Secondaries)
			assert.DeepEqual(t, rec1.Learners, rec2.Learners)
		}
	}
}

func TestArrangerCacheReclaim(t *testing.T) {
	arranger := &hashGroupArranger{}
	arranger.SetLogName("test-service")

	te1 := setupTestSchedulerStableEnv(t, 2, []int{4, 4}, 256)
	te1.opts.PartGroupCnt = 16

	plan := SchedulePlan{}
	te1.updateRecorder()
	arranger.Schedule(te1.getScheduleInput(), te1.nr, plan)
	assert.Equal(t, len(plan), 256)
	assert.Equal(t, len(arranger.cache.deciders), 1)
	keys := []string{"test-256-16-4"}
	for _, key := range keys {
		assert.Assert(t, arranger.cache.deciders[key] != nil)
	}

	plan = SchedulePlan{}
	te2 := setupTestSchedulerStableEnv(t, 2, []int{4, 8}, 256)
	te2.opts.PartGroupCnt = 16
	te2.updateRecorder()
	arranger.Schedule(te2.getScheduleInput(), te2.nr, plan)
	assert.Equal(t, len(plan), 256)
	assert.Equal(t, len(arranger.cache.deciders), 2)
	keys = []string{"test-256-16-4", "test-256-16-8"}
	for _, key := range keys {
		assert.Assert(t, arranger.cache.deciders[key] != nil)
	}

	plan = SchedulePlan{}
	te3 := setupTestSchedulerStableEnv(t, 2, []int{16, 16}, 256)
	te3.opts.PartGroupCnt = 16
	te3.updateRecorder()
	arranger.Schedule(te3.getScheduleInput(), te3.nr, plan)
	assert.Equal(t, len(plan), 256)
	assert.Equal(t, len(arranger.cache.deciders), 1)
	keys = []string{"test-256-16-16"}
	for _, key := range keys {
		assert.Assert(t, arranger.cache.deciders[key] != nil)
	}
}
func TestHashGroupArrangerFluentAddRemoveNode(t *testing.T) {
	arranger := &hashGroupArranger{}
	arranger.SetLogName("test-service")

	te := setupTestSchedulerStableEnv(t, 1, []int{20}, 512)
	te.opts.PartGroupCnt = 32
	te.opts.Configs.HashArrangerAddReplicaFirst = true

	logging.Info("first assign node")
	for i := 0; i < 10000; i++ {
		logging.Info("start round %d", i)
		te.updateRecorder()
		plan := SchedulePlan{}
		arranger.Schedule(te.getScheduleInput(), te.nr, plan)
		if arranger.nextDisorderHub == "" && len(plan) == 0 {
			break
		}
		te.applyPlan(plan)
		te.promoteAllLearners()
	}
	assert.Assert(t, true)

	logging.Info("add 1 node")
	te.addNodes([]int{1})

	logging.Info("then schedule again")
	for i := 0; i < 10000; i++ {
		logging.Info("start round %d", i)
		te.updateRecorder()
		plan := SchedulePlan{}
		arranger.Schedule(te.getScheduleInput(), te.nr, plan)
		te.applyPlan(plan)
		te.promoteAllLearners()
		if arranger.nextDisorderHub == "" && len(plan) == 0 {
			break
		}
	}
	assert.Assert(t, true)

	logging.Info("remove 1 node")
	te.removeNodes([]int{1})

	logging.Info("then schedule again")
	for i := 0; i < 10000; i++ {
		logging.Info("start round %d", i)
		te.updateRecorder()
		plan := SchedulePlan{}
		arranger.Schedule(te.getScheduleInput(), te.nr, plan)
		te.applyPlan(plan)
		te.promoteAllLearners()
		if arranger.nextDisorderHub == "" && len(plan) == 0 {
			break
		}
	}
	assert.Assert(t, true)
}

func TestHashGroupArrangerAddFirstWithEnoughCapacity(t *testing.T) {
	arranger := &hashGroupArranger{}
	arranger.SetLogName("test-service")

	te := setupTestSchedulerStableEnv(t, 1, []int{2}, 4)
	te.opts.PartGroupCnt = 4
	te.opts.Configs.HashArrangerAddReplicaFirst = true
	te.opts.Configs.HashArrangerMaxOverflowReplicas = 1

	te.table.GetMembership(0).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kPrimary,
	}
	te.table.GetMembership(1).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kPrimary,
	}
	te.table.GetMembership(2).Peers = map[string]pb.ReplicaRole{
		"node_yz0_1": pb.ReplicaRole_kLearner,
	}
	te.table.GetMembership(3).Peers = map[string]pb.ReplicaRole{
		"node_yz0_1": pb.ReplicaRole_kPrimary,
	}

	plan := SchedulePlan{}
	te.updateRecorder()
	arranger.Initialize(te.getScheduleInput(), te.nr, plan)
	arranger.nextDisorderHub = "yz0"

	newPlacement := []briefMembership{
		map[string]string{"yz0": "node_yz0_0"},
		map[string]string{"yz0": "node_yz0_1"},
		map[string]string{"yz0": "node_yz0_0"},
		map[string]string{"yz0": "node_yz0_1"},
	}
	arranger.adjust(newPlacement)
	assert.Equal(t, len(plan), 2)
	CmpActions(t, plan[1], actions.TransferPrimary("node_yz0_0", "node_yz0_1"))
	CmpActions(t, plan[2], actions.TransferLearner("node_yz0_1", "node_yz0_0", false))
}

func TestHashGroupArrangerAddFirstCapacityLimitedButHasRedudant(t *testing.T) {
	arranger := &hashGroupArranger{}
	arranger.SetLogName("test-service")

	te := setupTestSchedulerStableEnv(t, 1, []int{2}, 4)
	te.opts.PartGroupCnt = 4
	te.opts.Configs.HashArrangerAddReplicaFirst = true
	te.opts.Configs.HashArrangerMaxOverflowReplicas = 0

	{
		logging.Info("has redudant and remove it")
		te.table.GetMembership(0).Peers = map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz0_1": pb.ReplicaRole_kLearner,
		}
		te.table.GetMembership(1).Peers = map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
		}
		te.table.GetMembership(3).Peers = map[string]pb.ReplicaRole{
			"node_yz0_1": pb.ReplicaRole_kPrimary,
		}

		plan := SchedulePlan{}
		te.updateRecorder()
		arranger.Initialize(te.getScheduleInput(), te.nr, plan)
		arranger.nextDisorderHub = "yz0"

		newPlacement := []briefMembership{
			map[string]string{"yz0": "node_yz0_0"},
			map[string]string{"yz0": "node_yz0_1"},
			map[string]string{"yz0": "node_yz0_0"},
			map[string]string{"yz0": "node_yz0_1"},
		}
		arranger.adjust(newPlacement)
		assert.Equal(t, len(plan), 1)
		CmpActions(t, plan[0], actions.RemoveLearner("node_yz0_1"))
	}

	{
		logging.Info("has redudant but can't remove it")
		te.table.GetMembership(0).Peers = map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kLearner,
			"node_yz0_1": pb.ReplicaRole_kPrimary,
		}
		te.table.GetMembership(1).Peers = map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
		}
		te.table.GetMembership(3).Peers = map[string]pb.ReplicaRole{
			"node_yz0_1": pb.ReplicaRole_kPrimary,
		}

		plan := SchedulePlan{}
		te.updateRecorder()
		arranger.Initialize(te.getScheduleInput(), te.nr, plan)
		arranger.nextDisorderHub = "yz0"

		newPlacement := []briefMembership{
			map[string]string{"yz0": "node_yz0_0"},
			map[string]string{"yz0": "node_yz0_1"},
			map[string]string{"yz0": "node_yz0_0"},
			map[string]string{"yz0": "node_yz0_1"},
		}
		arranger.adjust(newPlacement)
		assert.Equal(t, len(plan), 0)
	}
}

func TestHashGroupArrangerAddFirstCapacityLimitedFixStuck(t *testing.T) {
	arranger := &hashGroupArranger{}
	arranger.SetLogName("test-service")

	te := setupTestSchedulerStableEnv(t, 1, []int{4}, 8)
	te.opts.PartGroupCnt = 4
	te.opts.Configs.HashArrangerAddReplicaFirst = true
	te.opts.Configs.HashArrangerMaxOverflowReplicas = 0

	te.table.GetMembership(0).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kPrimary,
	}
	te.table.GetMembership(7).Peers = map[string]pb.ReplicaRole{
		"node_yz0_0": pb.ReplicaRole_kLearner,
	}

	te.table.GetMembership(1).Peers = map[string]pb.ReplicaRole{
		"node_yz0_1": pb.ReplicaRole_kLearner,
	}
	te.table.GetMembership(2).Peers = map[string]pb.ReplicaRole{
		"node_yz0_1": pb.ReplicaRole_kPrimary,
	}

	te.table.GetMembership(3).Peers = map[string]pb.ReplicaRole{
		"node_yz0_2": pb.ReplicaRole_kLearner,
	}
	te.table.GetMembership(4).Peers = map[string]pb.ReplicaRole{
		"node_yz0_2": pb.ReplicaRole_kPrimary,
	}

	te.table.GetMembership(5).Peers = map[string]pb.ReplicaRole{
		"node_yz0_3": pb.ReplicaRole_kLearner,
	}
	te.table.GetMembership(6).Peers = map[string]pb.ReplicaRole{
		"node_yz0_3": pb.ReplicaRole_kPrimary,
	}

	plan := SchedulePlan{}
	te.updateRecorder()
	arranger.Initialize(te.getScheduleInput(), te.nr, plan)
	arranger.nextDisorderHub = "yz0"

	newPlacement := []briefMembership{
		map[string]string{"yz0": "node_yz0_0"},
		map[string]string{"yz0": "node_yz0_0"},
		map[string]string{"yz0": "node_yz0_1"},
		map[string]string{"yz0": "node_yz0_1"},
		map[string]string{"yz0": "node_yz0_2"},
		map[string]string{"yz0": "node_yz0_2"},
		map[string]string{"yz0": "node_yz0_3"},
		map[string]string{"yz0": "node_yz0_3"},
	}
	arranger.adjust(newPlacement)
	assert.Equal(t, len(plan), 1)
	var thePlanAction *actions.PartitionActions = nil
	for _, act := range plan {
		thePlanAction = act
		break
	}
	assert.Equal(t, thePlanAction.ActionCount(), 1)
	selectNode := thePlanAction.GetAction(0).(*actions.RemoveLearnerAction).Node
	CmpActions(t, thePlanAction, actions.RemoveLearner(selectNode))
}

func TestHashGroupNonEvenSchedule(t *testing.T) {
	arranger := &hashGroupArranger{}
	arranger.SetLogName("test-service")

	te := setupTestSchedulerStableEnv(t, 2, []int{3, 4}, 32)
	te.opts.PartGroupCnt = 8

	nodeResource := map[string]utils.HardwareUnit{
		"node_yz0_0": {
			utils.DISK_CAP: est.GB_10 * 100,
		},
		"node_yz0_1": {
			utils.DISK_CAP: est.GB_10 * 200,
		},
		"node_yz0_2": {
			utils.DISK_CAP: est.GB_10 * 300,
		},
		"node_yz1_0": {
			utils.DISK_CAP: est.GB_10 * 300,
		},
		"node_yz1_1": {
			utils.DISK_CAP: est.GB_10 * 300,
		},
		"node_yz1_2": {
			utils.DISK_CAP: est.GB_10 * 300,
		},
		"node_yz1_3": {
			utils.DISK_CAP: est.GB_10 * 300,
		},
	}
	te.reEstimateReplicas(nodeResource, est.NewEstimator(est.DISK_CAP_ESTIMATOR))

	logging.Info("schedule first hub")
	plan := SchedulePlan{}
	te.updateRecorder()
	arranger.Schedule(te.getScheduleInput(), te.nr, plan)
	assert.Assert(t, te.applyPlan(plan))

	logging.Info("schedule next hub")
	plan = SchedulePlan{}
	te.updateRecorder()
	arranger.Schedule(te.getScheduleInput(), te.nr, plan)
	assert.Assert(t, te.applyPlan(plan))

	logging.Info("check replicas placed properly")
	te.updateRecorder()

	minReplicas := map[string]int{}
	maxReplicas := map[string]int{}
	for node, info := range te.nodes.AllNodes() {
		assert.Equal(t, te.table.GetEstimatedReplicasOnNode(node), te.nr.CountAll(node))
		if _, ok := minReplicas[info.Hub]; !ok {
			minReplicas[info.Hub] = te.nr.CountAll(node)
			maxReplicas[info.Hub] = te.nr.CountAll(node)
		} else {
			minReplicas[info.Hub] = utils.Min(minReplicas[info.Hub], te.nr.CountAll(node))
			maxReplicas[info.Hub] = utils.Max(maxReplicas[info.Hub], te.nr.CountAll(node))
		}
	}
	assert.Assert(t, maxReplicas["yz0"]-minReplicas["yz0"] > 1)
	assert.Assert(t, maxReplicas["yz1"]-minReplicas["yz1"] == 0)

	separatedGroups := 0
	for group := 0; group < te.opts.PartGroupCnt; group++ {
		groupNodes := map[string]int{}
		for part := group; part < int(te.table.GetPartsCount()); part += te.opts.PartGroupCnt {
			member := te.table.GetMembership(int32(part))
			for node := range member.Peers {
				if te.nodes.MustGetNodeInfo(node).Hub == "yz0" {
					groupNodes[node]++
				}
			}
		}
		assert.Assert(t, len(groupNodes) <= 2, "group %d on %v", group, groupNodes)
		if len(groupNodes) == 2 {
			separatedGroups++
		}
	}
	assert.Assert(t, separatedGroups <= 2)
}
