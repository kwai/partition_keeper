package server

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/checkpoint"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

func TestSplitTableFullSchedule(t *testing.T) {
	nodes := []*node_mgr.NodeInfo{
		{
			Id:  "node1",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ1",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ1",
				Address:   utils.FromHostPort("127.0.0.1:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
		{
			Id:  "node2",
			Op:  pb.AdminNodeOp_kNoop,
			Hub: "YZ2",
			NodePing: node_mgr.NodePing{
				IsAlive:   true,
				Az:        "YZ2",
				Address:   utils.FromHostPort("127.0.0.2:1001"),
				ProcessId: "12340",
				BizPort:   2001,
			},
		},
	}

	opts := &pb.CreateServiceRequest{ServiceType: pb.ServiceType_colossusdb_dummy}
	tableParts := 4
	te := setupTestServiceEnv(
		t,
		nodes,
		opts,
		&testServiceEnvSetupOptions{true, true, "zk", int32(tableParts)},
	)
	defer te.stop()

	ans := te.service.UpdateScheduleOptions(&pb.ScheduleOptions{
		MaxSchedRatio: 1000,
	}, []string{"max_sched_ratio"})
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	queryTaskReq := &pb.QueryTaskRequest{
		TableName: "test",
		TaskName:  checkpoint.TASK_NAME,
	}
	queryTaskResp := &pb.QueryTaskResponse{}
	te.service.QueryTask(queryTaskReq, queryTaskResp)
	assert.Equal(t, queryTaskResp.Task.Paused, false)
	queryTaskResp.Task.Paused = true
	ans = te.service.UpdateTask("test", queryTaskResp.Task)
	assert.Equal(t, ans.Code, int32(pb.AdminError_kOk))

	te.initGivenMembership(&table_model.PartitionMembership{
		MembershipVersion: 7,
		Peers: map[string]pb.ReplicaRole{
			"node1": pb.ReplicaRole_kPrimary,
			"node2": pb.ReplicaRole_kSecondary,
		},
	})
	te.TriggerCollectAndWaitFinish()

	logging.Info("first wait node distribution to be balanced")
	for {
		te.TriggerScheduleAndWaitFinish()
		if te.service.tableNames["test"].StatsConsistency(false) {
			break
		}
		te.TriggerCollectAndWaitFinish()
	}

	logging.Info("split not-exist table")
	req := &pb.SplitTableRequest{
		ServiceName:     "test",
		TableName:       "invalid_table_name",
		NewSplitVersion: 1,
		Options: &pb.SplitTableOptions{
			MaxConcurrentParts: 1,
			DelaySeconds:       1,
		},
	}
	resp := te.service.SplitTable(req)
	assert.Equal(t, resp.Code, int32(pb.AdminError_kTableNotExists))

	table := te.service.tableNames["test"]
	client0 := te.lpb.GetClient("127.0.0.1:1001")
	client1 := te.lpb.GetClient("127.0.0.2:1001")

	logging.Info("send split request succeed")
	req.TableName = "test"
	resp = te.service.SplitTable(req)
	assert.Equal(t, resp.Code, int32(pb.AdminError_kOk), resp.Message)

	logging.Info("do split schedule")
	for {
		te.TriggerScheduleAndWaitFinish()
		te.TriggerCollectAndWaitFinish()

		for i := 0; i < tableParts; i++ {
			assert.Equal(
				t,
				table.currParts[i].members.SplitVersion,
				table.currParts[i+tableParts].members.SplitVersion,
			)
		}
		lastFinished := 0
		for _, fact := range table.currParts[tableParts*2-1].facts {
			if fact.SplitCleanupVersion == 1 {
				lastFinished++
			}
		}
		if lastFinished == 2 {
			break
		}
	}

	assert.Equal(t, table.SplitVersion, int32(1))
	assert.Equal(t, table.PartsCount, int32(tableParts*2))
	for i := tableParts; i < tableParts*2; i++ {
		part := &(table.currParts[i])
		pri, sec, learner := part.members.DivideRoles()
		assert.Equal(t, len(pri), 1)
		assert.Equal(t, len(sec), 1)
		assert.Equal(t, len(learner), 0)
	}
	assert.Equal(t, client0.GetMetric("replica_split"), tableParts)
	assert.Equal(t, client0.GetMetric("replica_split_cleanup"), tableParts*2)
	assert.Equal(t, client1.GetMetric("replica_split"), tableParts)
	assert.Equal(t, client1.GetMetric("replica_split_cleanup"), tableParts*2)
}
