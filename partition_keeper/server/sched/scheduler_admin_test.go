package sched

import (
	"fmt"
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/est"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"gotest.tools/assert"
)

type testSchedulerStableEnv struct {
	t           *testing.T
	lock        *utils.LooseLock
	hubs        []*pb.ReplicaHub
	table       *actions.TableModelMock
	nodes       *node_mgr.NodeStats
	opts        ScheduleCtrlOptions
	nr          *recorder.AllNodesRecorder
	perHubNodes []int
}

func setupTestSchedulerStableEnv(
	t *testing.T,
	hubCount int,
	perHubNodes []int,
	partsCount int32,
) *testSchedulerStableEnv {
	var err error
	hubs := []*pb.ReplicaHub{}
	for i := 0; i < hubCount; i++ {
		hubs = append(hubs, &pb.ReplicaHub{Name: fmt.Sprintf("yz%d", i), Az: "YZ"})
	}
	nodes, lock := createNodeStats(t, hubs, perHubNodes)
	table := actions.NewTableModelMock(&pb.Table{
		TableId:    1,
		TableName:  "test",
		PartsCount: partsCount,
	})
	table.EstimatedReplicas, err = EstimateTableReplicas("test", nodes, table)
	assert.NilError(t, err)
	return &testSchedulerStableEnv{
		t:     t,
		lock:  lock,
		hubs:  hubs,
		table: table,
		nodes: nodes,
		opts: ScheduleCtrlOptions{
			Configs: &pb.ScheduleOptions{
				EnablePrimaryScheduler:          true,
				MaxSchedRatio:                   SCHEDULE_RATIO_MAX_VALUE,
				EnableSplitBalancer:             false,
				HashArrangerAddReplicaFirst:     false,
				HashArrangerMaxOverflowReplicas: 0,
			},
			PrepareRestoring: false,
		},
		perHubNodes: perHubNodes,
	}
}

func (te *testSchedulerStableEnv) removeNodes(perHubDec []int) {
	pings := map[string]*node_mgr.NodePing{}
	offlineNodes := []*utils.RpcNode{}
	for k, hub := range te.hubs {
		for i := 0; i < perHubDec[k]; i++ {
			index := te.perHubNodes[k] - 1 - i
			rpcNode := &utils.RpcNode{
				NodeName: fmt.Sprintf("127.0.0.%d", k),
				Port:     int32(index),
			}
			offlineNodes = append(offlineNodes, rpcNode.Clone())
			pings[fmt.Sprintf("node_%s_%d", hub.Name, index)] = &node_mgr.NodePing{
				IsAlive:   false,
				Az:        hub.Az,
				Address:   rpcNode,
				ProcessId: "1",
				BizPort:   int32(index + 1000),
			}
		}

		te.perHubNodes[k] -= perHubDec[k]
	}

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()
	te.nodes.AdminNodes(offlineNodes, true, pb.AdminNodeOp_kOffline, nil)

	var err error
	te.table.EstimatedReplicas, err = EstimateTableReplicas("test", te.nodes, te.table)
	assert.NilError(te.t, err)
}

func (te *testSchedulerStableEnv) addNodes(perHubInc []int) {
	hints := map[string]*pb.NodeHints{}
	pings := map[string]*node_mgr.NodePing{}
	for k, hub := range te.hubs {
		for i := 0; i < perHubInc[k]; i++ {
			index := te.perHubNodes[k] + i
			rpcNode := &utils.RpcNode{
				NodeName: fmt.Sprintf("127.0.0.%d", k),
				Port:     int32(index),
			}
			hints[rpcNode.String()] = &pb.NodeHints{Hub: hub.Name}
			pings[fmt.Sprintf("node_%s_%d", hub.Name, index)] = &node_mgr.NodePing{
				IsAlive:   true,
				Az:        hub.Az,
				Address:   rpcNode,
				ProcessId: "1",
				BizPort:   int32(index + 1000),
			}
		}
		te.perHubNodes[k] += perHubInc[k]
	}

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()
	assert.Equal(te.t, te.nodes.AddHints(hints, true).Code, int32(pb.AdminError_kOk))
	te.nodes.UpdateStats(pings)
	te.nodes.RefreshScore(est.NewEstimator(est.DEFAULT_ESTIMATOR), true)

	var err error
	te.table.EstimatedReplicas, err = EstimateTableReplicas("test", te.nodes, te.table)
	assert.NilError(te.t, err)
}

func (te *testSchedulerStableEnv) reEstimateReplicas(
	resource map[string]utils.HardwareUnit,
	estimator est.Estimator,
) {
	te.lock.LockWrite()
	defer te.lock.UnlockWrite()

	for node, res := range resource {
		te.nodes.MustGetNodeInfo(node).SetResource("test-service", res)
	}
	assert.Assert(te.t, te.nodes.RefreshScore(estimator, true))
	var err error
	te.table.EstimatedReplicas, err = EstimateTableReplicas("test", te.nodes, te.table)
	assert.NilError(te.t, err)
}

func (te *testSchedulerStableEnv) getNodesWithCap(cap int, hub string) []string {
	output := []string{}
	for node, n := range te.table.GetEstimatedReplicas() {
		if n == cap && te.nodes.MustGetNodeInfo(node).Hub == hub {
			output = append(output, node)
		}
	}
	return output
}

func (te *testSchedulerStableEnv) getScheduleInput() *ScheduleInput {
	return &ScheduleInput{
		Table: te.table,
		Nodes: te.nodes,
		Hubs:  te.hubs,
		Opts:  te.opts,
	}
}

func (te *testSchedulerStableEnv) promoteAllLearners() {
	for {
		plan := SchedulePlan{}
		for i := int32(0); i < te.table.PartsCount; i++ {
			_, _, learners := te.table.Partitions[i].DivideRoles()
			if len(learners) > 0 {
				plan[i] = actions.MakeActions(
					&actions.TransformAction{Node: learners[0], ToRole: pb.ReplicaRole_kSecondary},
				)
			}
		}
		if len(plan) == 0 {
			return
		}
		te.applyPlan(plan)
	}
}

func (te *testSchedulerStableEnv) applyPlan(plan SchedulePlan) bool {
	if len(plan) == 0 {
		return false
	}
	for pid, action := range plan {
		part := te.table.GetActionAcceptor(pid)
		for action.HasAction() {
			action.ConsumeAction(part)
		}
	}
	return true
}

func (te *testSchedulerStableEnv) updateRecorder() {
	te.nr = recorder.NewAllNodesRecorder(recorder.NewNodePartitions)
	te.table.Record(te.nr)
}

func TestScheduleOptionsEffect(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 4, []int{1, 1, 1, 1}, 10000)
	for i := range te.table.Partitions {
		members := te.table.GetMembership(int32(i))
		members.MembershipVersion = 7
		members.Peers = map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
			"node_yz2_0": pb.ReplicaRole_kSecondary,
			"node_yz3_0": pb.ReplicaRole_kSecondary,
		}
	}

	scheduler := NewScheduler(ADAPTIVE_SCHEDULER, "test-service")
	scheduleInput := &ScheduleInput{
		Table: te.table,
		Nodes: te.nodes,
		Hubs:  te.hubs,
		Opts: ScheduleCtrlOptions{
			Configs: &pb.ScheduleOptions{
				EnablePrimaryScheduler: false,
				MaxSchedRatio:          100,
			},
			PrepareRestoring: false,
		},
	}
	plan := scheduler.Schedule(scheduleInput)
	assert.Equal(t, len(plan), 0)

	scheduleInput.Opts.Configs.EnablePrimaryScheduler = true
	plan = scheduler.Schedule(scheduleInput)
	assert.Equal(t, len(plan), 1000)

	scheduleInput.Opts.Configs = &pb.ScheduleOptions{EnablePrimaryScheduler: true, MaxSchedRatio: 1}
	plan = scheduler.Schedule(scheduleInput)
	assert.Equal(t, len(plan), 10)

	scheduleInput.Opts.Configs = &pb.ScheduleOptions{
		EnablePrimaryScheduler: true,
		MaxSchedRatio:          1000,
	}
	plan = scheduler.Schedule(scheduleInput)
	assert.Equal(t, len(plan), 7500)
}

func TestRestartingWithDisallowPrimary(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 4, []int{1, 1, 1, 1}, 16)

	scheduler := NewScheduler(ADAPTIVE_SCHEDULER, "test-service")

	te.nodes.MustGetNodeInfo("node_yz0_0").Op = pb.AdminNodeOp_kRestart
	te.hubs[1].DisallowedRoles = []pb.ReplicaRole{pb.ReplicaRole_kPrimary}

	logging.Info("primary will transfer to node_yz3_0")
	for i := range te.table.Partitions {
		members := &(te.table.Partitions[i])
		members.MembershipVersion = 7
		members.Peers = map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
			"node_yz1_0": pb.ReplicaRole_kSecondary,
			"node_yz2_0": pb.ReplicaRole_kSecondary,
			"node_yz3_0": pb.ReplicaRole_kSecondary,
		}
	}

	scheduleInput := &ScheduleInput{
		Table: te.table,
		Nodes: te.nodes,
		Hubs:  te.hubs,
		Opts: ScheduleCtrlOptions{
			Configs:          &pb.ScheduleOptions{EnablePrimaryScheduler: true, MaxSchedRatio: 100},
			PrepareRestoring: false,
		},
	}
	plan := scheduler.Schedule(scheduleInput)
	te.applyPlan(plan)
	for i := range te.table.Partitions {
		members := &(te.table.Partitions[i])
		assert.Equal(t, members.GetMember("node_yz0_0"), pb.ReplicaRole_kLearner)
		assert.Equal(t, members.GetMember("node_yz1_0"), pb.ReplicaRole_kSecondary)
	}
}

func TestAllHubsRemoved(t *testing.T) {
	te := setupTestSchedulerStableEnv(t, 1, []int{1}, 16)
	scheduler := NewScheduler(ADAPTIVE_SCHEDULER, "test-service")
	for i := range te.table.Partitions {
		members := &(te.table.Partitions[i])
		members.Peers = map[string]pb.ReplicaRole{
			"node_yz0_0": pb.ReplicaRole_kPrimary,
		}
	}

	scheduleInput := &ScheduleInput{
		Table: te.table,
		Nodes: te.nodes,
		Hubs:  []*pb.ReplicaHub{},
		Opts: ScheduleCtrlOptions{
			Configs: &pb.ScheduleOptions{
				EnablePrimaryScheduler: true,
				MaxSchedRatio:          1000,
			},
			PrepareRestoring: false,
		},
	}
	plan := scheduler.Schedule(scheduleInput)
	assert.Assert(t, len(plan) > 0)
	te.applyPlan(plan)
	for i := range te.table.Partitions {
		assert.Equal(t, len(te.table.Partitions[i].Peers), 0)
	}
}
