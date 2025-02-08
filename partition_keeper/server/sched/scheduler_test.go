package sched

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/est"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"gotest.tools/assert"
)

var (
	flagSeed = flag.Int64("seed", 0, "seed")
)

type testSchedulerRandomEnv struct {
	lock  *utils.LooseLock
	hubs  []*pb.ReplicaHub
	table *actions.TableModelMock
	nodes *node_mgr.NodeStats

	perHubNodes []int
}

func createNodeStats(
	t *testing.T,
	hubs []*pb.ReplicaHub,
	perHubNodes []int,
) (*node_mgr.NodeStats, *utils.LooseLock) {
	acl, scheme, auth := acl.GetKeeperACLandAuthForZK()
	zkPrefix := utils.SpliceZkRootPath("/test/schedule")

	zkStore := metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"}, time.Second*10, acl, scheme, auth,
	)

	assert.Assert(t, zkStore.RecursiveDelete(context.Background(), zkPrefix))
	assert.Assert(t, zkStore.RecursiveCreate(context.Background(), zkPrefix+"/nodes"))
	assert.Assert(t, zkStore.RecursiveCreate(context.Background(), zkPrefix+"/hints"))

	lock := utils.NewLooseLock()
	nodes := node_mgr.NewNodeStats(
		"test",
		lock,
		zkPrefix+"/nodes",
		zkPrefix+"/hints",
		zkStore,
	).WithSkipHintCheck(true)
	nodes.LoadFromZookeeper(utils.MapHubs(hubs), false)

	hints := map[string]*pb.NodeHints{}
	pings := map[string]*node_mgr.NodePing{}

	for k, hub := range hubs {
		for i := 0; i < perHubNodes[k]; i++ {
			rpcNode := &utils.RpcNode{
				NodeName: fmt.Sprintf("127.0.0.%d", k),
				Port:     int32(i),
			}
			hints[rpcNode.String()] = &pb.NodeHints{Hub: hub.Name}
			pings[fmt.Sprintf("node_%s_%d", hub.Name, i)] = &node_mgr.NodePing{
				IsAlive:   true,
				Az:        hub.Az,
				Address:   rpcNode,
				ProcessId: "1",
				BizPort:   int32(i + 1000),
			}
		}
	}

	lock.LockWrite()
	defer lock.UnlockWrite()
	assert.Equal(t, nodes.AddHints(hints, true).Code, int32(pb.AdminError_kOk))
	nodes.UpdateStats(pings)
	nodes.RefreshScore(est.NewEstimator(est.DEFAULT_ESTIMATOR), true)
	return nodes, lock
}

func setupTestSchedulerRandomEnv(
	t *testing.T,
	withLearners bool,
	withDisallowPrimaries bool,
) *testSchedulerRandomEnv {
	if *flagSeed == 0 {
		seed := time.Now().UnixNano()
		fmt.Fprintf(os.Stderr, "use random seed: %d\n", seed)
		rand.Seed(seed)
	} else {
		rand.Seed(*flagSeed)
	}
	hubs := []*pb.ReplicaHub{
		{Name: "yz0", Az: "YZ"},
		{Name: "yz1", Az: "YZ"},
		{Name: "yz2", Az: "YZ"},
		{Name: "yz3", Az: "YZ"},
		{Name: "yz4", Az: "YZ"},
	}
	if withDisallowPrimaries {
		hubs[0].DisallowedRoles = []pb.ReplicaRole{pb.ReplicaRole_kPrimary}
	}

	perHubCount := []int{}
	for range hubs {
		count := 10 + rand.Intn(90)
		perHubCount = append(perHubCount, count)
	}
	nodes, lock := createNodeStats(t, hubs, perHubCount)

	tableInfo := &pb.Table{
		TableId:    1,
		TableName:  "test",
		PartsCount: int32(256 + rand.Intn(1024)),
	}
	table := actions.NewTableModelMock(tableInfo)

	for i := int32(0); i < table.PartsCount; i++ {
		members := table.GetMembership(i)
		members.MembershipVersion = 7
		for j, hub := range hubs {
			assignId := rand.Intn(perHubCount[j])
			node := fmt.Sprintf("node_%s_%d", hub.Name, assignId)
			if j == 0 {
				members.Peers[node] = pb.ReplicaRole_kPrimary
			} else if j == len(hubs)-1 && withLearners {
				members.Peers[node] = pb.ReplicaRole_kLearner
			} else {
				members.Peers[node] = pb.ReplicaRole_kSecondary
			}
		}
	}

	var err error
	table.EstimatedReplicas, err = EstimateTableReplicas("test", nodes, table)
	assert.NilError(t, err)
	return &testSchedulerRandomEnv{
		lock:        lock,
		hubs:        hubs,
		table:       table,
		nodes:       nodes,
		perHubNodes: perHubCount,
	}
}

func (te *testSchedulerRandomEnv) applyPlan(plan SchedulePlan) bool {
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

func (te *testSchedulerRandomEnv) printDistribution(
	t *testing.T,
	checkBalance bool,
	checkPrimaryBalance bool,
) {
	nr := recorder.NewAllNodesRecorder(recorder.NewBriefNode)
	te.table.Record(nr)

	buffer := bytes.NewBuffer(nil)
	writer := tabwriter.NewWriter(buffer, 0, 2, 1, ' ', tabwriter.AlignRight)
	fmt.Fprintln(writer, "node\tprimaries\tsecondaries\tlearners\ttotal\testimated\t")
	for hubid, hubcount := range te.perHubNodes {
		minPri, minTotal, maxPri, maxTotal := 0x7fffffff, 0x7fffffff, -1, -1
		for j := 0; j < hubcount; j++ {
			node := fmt.Sprintf("node_yz%d_%d", hubid, j)
			rec := nr.GetNode(node)
			fmt.Fprintf(
				writer,
				"%s\t%d\t%d\t%d\t%d\t%d\t\n",
				node,
				rec.Count(pb.ReplicaRole_kPrimary),
				rec.Count(pb.ReplicaRole_kSecondary),
				rec.Count(pb.ReplicaRole_kLearner),
				rec.CountAll(),
				te.table.GetEstimatedReplicasOnNode(node),
			)
			minPri = utils.Min(minPri, rec.Count(pb.ReplicaRole_kPrimary))
			minTotal = utils.Min(minTotal, rec.CountAll())
			maxPri = utils.Max(maxPri, rec.Count(pb.ReplicaRole_kPrimary))
			maxTotal = utils.Max(maxTotal, rec.CountAll())
			if checkBalance {
				assert.Equal(t, rec.CountAll(), te.table.GetEstimatedReplicasOnNode(node))
				assert.Assert(t, maxTotal-minTotal <= 1)
				if checkPrimaryBalance {
					assert.Assert(t, maxPri-minPri <= 1)
				}
			}
		}
	}
	writer.Flush()
	logging.Info("\n" + buffer.String())
}

func TestRandomScheduler(t *testing.T) {
	te := setupTestSchedulerRandomEnv(t, false, false)
	te.printDistribution(t, false, false)

	scheduler := NewScheduler(ADAPTIVE_SCHEDULER, "test_service")
	for {
		plan := scheduler.Schedule(
			&ScheduleInput{
				Table: te.table,
				Nodes: te.nodes,
				Hubs:  te.hubs,
				Opts: ScheduleCtrlOptions{
					Configs: &pb.ScheduleOptions{
						EnablePrimaryScheduler: true,
						MaxSchedRatio:          100,
					},
					PrepareRestoring: false,
				},
			},
		)
		if !te.applyPlan(plan) {
			break
		}
	}
	te.printDistribution(t, true, true)
}

func TestPrimaryBalanceWithLearners(t *testing.T) {
	te := setupTestSchedulerRandomEnv(t, true, false)
	te.printDistribution(t, false, false)

	scheduler := NewScheduler(ADAPTIVE_SCHEDULER, "test_service")
	for {
		plan := scheduler.Schedule(
			&ScheduleInput{
				Table: te.table,
				Nodes: te.nodes,
				Hubs:  te.hubs,
				Opts: ScheduleCtrlOptions{
					Configs: &pb.ScheduleOptions{
						EnablePrimaryScheduler: true,
						MaxSchedRatio:          100,
					},
					PrepareRestoring: false,
				},
			},
		)
		if !te.applyPlan(plan) {
			break
		}
	}
	te.printDistribution(t, true, false)
}

func TestPrimaryBalanceWithDisallow(t *testing.T) {
	te := setupTestSchedulerRandomEnv(t, false, true)
	te.printDistribution(t, false, false)

	scheduler := NewScheduler(ADAPTIVE_SCHEDULER, "test_service")
	for {
		plan := scheduler.Schedule(
			&ScheduleInput{
				Table: te.table,
				Nodes: te.nodes,
				Hubs:  te.hubs,
				Opts: ScheduleCtrlOptions{
					Configs: &pb.ScheduleOptions{
						EnablePrimaryScheduler: true,
						MaxSchedRatio:          100,
					},
					PrepareRestoring: false,
				},
			},
		)
		if !te.applyPlan(plan) {
			break
		}
	}
	te.printDistribution(t, true, false)

	nr := recorder.NewAllNodesRecorder(recorder.NewBriefNode)
	te.table.Record(nr)
	for j := 0; j < te.perHubNodes[0]; j++ {
		node := fmt.Sprintf("node_yz%d_%d", 0, j)
		rec := nr.GetNode(node)
		assert.Equal(t, rec.Count(pb.ReplicaRole_kPrimary), 0)
	}
}
