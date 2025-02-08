package rodis

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
)

func TestChildLearnerTransform(t *testing.T) {
	st := Create()
	runningOpts := &base.TableRunningOptions{IsRestoring: false}

	self := &pb.PartitionReplica{
		Role:           pb.ReplicaRole_kLearner,
		Node:           &pb.RpcNode{NodeName: "127.0.0.1", Port: 1001},
		StatisticsInfo: nil,
		ReadyToPromote: false,
	}
	parentPrimary := &pb.PartitionReplica{
		Role:           pb.ReplicaRole_kPrimary,
		Node:           &pb.RpcNode{NodeName: "127.0.0.2", Port: 1001},
		StatisticsInfo: nil,
		ReadyToPromote: false,
	}

	logging.Info("child its self not ready")
	role := st.Transform(
		utils.MakeTblPartID(1, 0),
		self,
		nil,
		[]*pb.PartitionReplica{parentPrimary},
		runningOpts,
	)
	assert.Equal(t, role, pb.ReplicaRole_kLearner)

	logging.Info("not child sequence number")
	self.ReadyToPromote = true
	role = st.Transform(
		utils.MakeTblPartID(1, 0),
		self,
		nil,
		[]*pb.PartitionReplica{parentPrimary},
		runningOpts,
	)
	assert.Equal(t, role, pb.ReplicaRole_kLearner)

	logging.Info("no primary")
	self.StatisticsInfo = map[string]string{pb.RodisReplicaStat_seq.String(): "12345"}
	role = st.Transform(utils.MakeTblPartID(1, 0), self, nil, []*pb.PartitionReplica{}, runningOpts)
	assert.Equal(t, role, pb.ReplicaRole_kLearner)

	logging.Info("primary no sequence number")
	role = st.Transform(
		utils.MakeTblPartID(1, 0),
		self,
		nil,
		[]*pb.PartitionReplica{parentPrimary},
		runningOpts,
	)
	assert.Equal(t, role, pb.ReplicaRole_kLearner)

	logging.Info("child and parent has lag")
	parentPrimary.StatisticsInfo = map[string]string{pb.RodisReplicaStat_seq.String(): "66666"}
	role = st.Transform(
		utils.MakeTblPartID(1, 0),
		self,
		nil,
		[]*pb.PartitionReplica{parentPrimary},
		runningOpts,
	)
	assert.Equal(t, role, pb.ReplicaRole_kLearner)

	logging.Info("child promotable")
	parentPrimary.StatisticsInfo = map[string]string{pb.RodisReplicaStat_seq.String(): "12347"}
	role = st.Transform(
		utils.MakeTblPartID(1, 0),
		self,
		nil,
		[]*pb.PartitionReplica{parentPrimary},
		runningOpts,
	)
	assert.Equal(t, role, pb.ReplicaRole_kSecondary)
}

func TestRodisPromotableLearner(t *testing.T) {
	st := Create()

	runningOpts := &base.TableRunningOptions{
		IsRestoring: false,
	}
	logging.Info("can't promote if node itself not ready")
	self := &pb.PartitionReplica{
		Role: pb.ReplicaRole_kLearner,
		Node: &pb.RpcNode{
			NodeName: "127.0.0.1",
			Port:     1001,
		},
		StatisticsInfo: nil,
		ReadyToPromote: false,
		HubName:        "yz1",
		NodeUniqueId:   "node1",
	}
	primary := &pb.PartitionReplica{
		Role: pb.ReplicaRole_kPrimary,
		Node: &pb.RpcNode{
			NodeName: "127.0.0.1",
			Port:     1002,
		},
		StatisticsInfo: map[string]string{
			pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "2000",
		},
		ReadyToPromote: true,
		HubName:        "yz1",
		NodeUniqueId:   "node2",
	}
	assert.Equal(
		t,
		st.Transform(utils.MakeTblPartID(1, 1), self, nil, nil, runningOpts),
		pb.ReplicaRole_kLearner,
	)

	logging.Info("can't promote if can't get node's sequence number")
	self.ReadyToPromote = true
	assert.Equal(
		t,
		st.Transform(
			utils.MakeTblPartID(1, 1),
			self,
			[]*pb.PartitionReplica{primary},
			nil,
			runningOpts,
		),
		pb.ReplicaRole_kLearner,
	)

	logging.Info("if no primary and no others, node will get promotable")
	self = &pb.PartitionReplica{
		Role: pb.ReplicaRole_kLearner,
		Node: &pb.RpcNode{
			NodeName: "127.0.0.1",
			Port:     1001,
		},
		StatisticsInfo: map[string]string{
			pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "2000",
		},
		ReadyToPromote: true,
		HubName:        "yz1",
		NodeUniqueId:   "node1",
	}
	assert.Equal(
		t,
		st.Transform(utils.MakeTblPartID(1, 1), self, nil, nil, runningOpts),
		pb.ReplicaRole_kSecondary,
	)

	logging.Info("if no primary, won't promote until all seq collected")
	others := []*pb.PartitionReplica{}
	others = append(others, proto.Clone(self).(*pb.PartitionReplica))
	others[0].StatisticsInfo = nil
	others[0].NodeUniqueId = "node2"
	assert.Equal(
		t,
		st.Transform(utils.MakeTblPartID(1, 1), self, others, nil, runningOpts),
		pb.ReplicaRole_kLearner,
	)

	logging.Info("learner will promote to secondary if it's ready and table in restoring")
	runningOpts.IsRestoring = true
	assert.Equal(
		t,
		st.Transform(utils.MakeTblPartID(1, 1), self, others, nil, runningOpts),
		pb.ReplicaRole_kSecondary,
	)
	runningOpts.IsRestoring = false

	logging.Info("if no primary, will be promotable as one of majority")
	others = nil
	other1 := proto.Clone(self).(*pb.PartitionReplica)
	other1.NodeUniqueId = "node2"
	other1.StatisticsInfo = map[string]string{
		pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "3000",
	}
	others = append(others, other1)
	other1 = proto.Clone(self).(*pb.PartitionReplica)
	other1.NodeUniqueId = "node3"
	others = append(others, other1)
	assert.Equal(
		t,
		st.Transform(utils.MakeTblPartID(1, 1), self, others, nil, runningOpts),
		pb.ReplicaRole_kSecondary,
	)

	logging.Info("if no primary, won't be promotable as one of minority")
	others = nil
	other1 = proto.Clone(self).(*pb.PartitionReplica)
	other1.NodeUniqueId = "node2"
	other1.StatisticsInfo = map[string]string{
		pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "3000",
	}
	others = append(others, other1)
	assert.Equal(
		t,
		st.Transform(utils.MakeTblPartID(1, 1), self, others, nil, runningOpts),
		pb.ReplicaRole_kLearner,
	)

	logging.Info("if no sequence info in primary's statistics info, then not promotable")
	self.ReadyToPromote = true
	self.StatisticsInfo = map[string]string{
		pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "2000",
	}
	primary.StatisticsInfo = nil

	assert.Equal(
		t,
		st.Transform(
			utils.MakeTblPartID(1, 1),
			self,
			[]*pb.PartitionReplica{primary},
			nil,
			runningOpts,
		),
		pb.ReplicaRole_kLearner,
	)

	logging.Info("if sequence info is not integer, then not promotable")
	self.ReadyToPromote = true
	self.StatisticsInfo = map[string]string{
		pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "2000",
	}
	primary.StatisticsInfo = map[string]string{
		pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "invalid",
	}
	assert.Equal(
		t,
		st.Transform(
			utils.MakeTblPartID(1, 1),
			self,
			[]*pb.PartitionReplica{primary},
			nil,
			runningOpts,
		),
		pb.ReplicaRole_kLearner,
	)

	logging.Info("if self and primary has a huge lag, then not promotable")
	self.ReadyToPromote = true
	self.StatisticsInfo = map[string]string{
		pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "2000",
	}
	primary.StatisticsInfo = map[string]string{
		pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "200000",
	}
	assert.Equal(
		t,
		st.Transform(
			utils.MakeTblPartID(1, 1),
			self,
			[]*pb.PartitionReplica{primary},
			nil,
			runningOpts,
		),
		pb.ReplicaRole_kLearner,
	)

	logging.Info("promotable by primary even if node has least data")
	self.ReadyToPromote = true
	self.StatisticsInfo = map[string]string{
		pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "2000",
	}
	other1 = proto.Clone(self).(*pb.PartitionReplica)
	other1.NodeUniqueId = "node1"
	other1.StatisticsInfo = map[string]string{
		pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "2010",
	}
	primary.StatisticsInfo = map[string]string{
		pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "2010",
	}
	assert.Equal(
		t,
		st.Transform(
			utils.MakeTblPartID(1, 1),
			self,
			[]*pb.PartitionReplica{primary, other1},
			nil, runningOpts,
		),
		pb.ReplicaRole_kSecondary,
	)
}

func TestRodisDowngradeSecondary(t *testing.T) {
	st := Create()
	tpid := utils.MakeTblPartID(1, 1)

	runningOpts := &base.TableRunningOptions{
		IsRestoring: true,
	}
	// if no primary, don't downgrade secondary
	self := &pb.PartitionReplica{
		Role: pb.ReplicaRole_kSecondary,
		Node: &pb.RpcNode{
			NodeName: "127.0.0.1",
			Port:     1001,
		},
		StatisticsInfo: nil,
		ReadyToPromote: true,
		HubName:        "yz1",
		NodeUniqueId:   "node1",
	}
	assert.Equal(
		t,
		st.Transform(tpid, self, nil, nil, runningOpts),
		pb.ReplicaRole_kSecondary,
	)
	assert.Equal(
		t,
		st.Transform(tpid, self, nil, []*pb.PartitionReplica{}, runningOpts),
		pb.ReplicaRole_kSecondary,
	)

	// if don't report seq by self, then secondary stayed
	primary := &pb.PartitionReplica{
		Role: pb.ReplicaRole_kPrimary,
		Node: &pb.RpcNode{
			NodeName: "127.0.0.1",
			Port:     1002,
		},
		StatisticsInfo: map[string]string{
			pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "2000",
		},
		ReadyToPromote: true,
		HubName:        "yz1",
		NodeUniqueId:   "node2",
	}

	assert.Equal(
		t,
		st.Transform(tpid, self, []*pb.PartitionReplica{primary}, nil, runningOpts),
		pb.ReplicaRole_kSecondary,
	)
	assert.Equal(
		t,
		st.Transform(tpid, self, nil, []*pb.PartitionReplica{primary}, runningOpts),
		pb.ReplicaRole_kSecondary,
	)

	// if no sequence info in primary's statistics info, then not promotable
	self.StatisticsInfo = map[string]string{
		pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "2000",
	}
	primary.StatisticsInfo = nil
	assert.Equal(
		t,
		st.Transform(tpid, self, []*pb.PartitionReplica{primary}, nil, runningOpts),
		pb.ReplicaRole_kSecondary,
	)

	// if self and primary's lag isn't big, then stay
	primary.StatisticsInfo = map[string]string{
		pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "2010",
	}
	assert.Equal(
		t,
		st.Transform(tpid, self, []*pb.PartitionReplica{primary}, nil, runningOpts),
		pb.ReplicaRole_kSecondary,
	)
	assert.Equal(
		t,
		st.Transform(tpid, self, nil, []*pb.PartitionReplica{primary}, runningOpts),
		pb.ReplicaRole_kSecondary,
	)

	// if self and primary's lag is big, then downgrade
	primary.StatisticsInfo = map[string]string{
		pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]: "100000",
	}
	assert.Equal(
		t,
		st.Transform(tpid, self, []*pb.PartitionReplica{primary}, nil, runningOpts),
		pb.ReplicaRole_kLearner,
	)
	assert.Equal(
		t,
		st.Transform(tpid, self, nil, []*pb.PartitionReplica{primary}, runningOpts),
		pb.ReplicaRole_kLearner,
	)
}

func TestRodisMakeStoreOpts(t *testing.T) {
	st := Create()

	logging.Info("test invalid json")
	args := "not_a_json"

	tablePb := &pb.Table{
		TableName: "test",
		JsonArgs:  args,
	}
	_, err := st.MakeStoreOpts(nil, "", "", tablePb.JsonArgs)
	assert.Assert(t, err != nil)

	logging.Info("test missing parameters")
	tablePb.JsonArgs = `{
		"zk_hosts": [
			"127.0.0.1:1001",
			"127.0.0.1:1002",
			"127.0.0.1:1003"
		]
	}`
	_, err = st.MakeStoreOpts(nil, "", "", tablePb.JsonArgs)
	assert.Assert(t, err != nil)

	logging.Info("test invalid region")
	tablePb.JsonArgs = `{
		"zk_hosts": [
			"127.0.0.1:1001",
			"127.0.0.1:1002",
			"127.0.0.1:1003"
		],
		"cluster_prefix": "/test/rodis/test",
		"unrelated_info": "unrelated_value"
	}`
	errorRegions := map[string]bool{
		"ERROR_REGION": true,
	}
	_, err = st.MakeStoreOpts(errorRegions, "", "", tablePb.JsonArgs)
	assert.Assert(t, err != nil)

	logging.Info("test succeed")
	tablePb.JsonArgs = `{
		"zk_hosts": [
			"127.0.0.1:1001",
			"127.0.0.1:1002",
			"127.0.0.1:1003"
		],
		"cluster_prefix": "/test/rodis/test",
		"unrelated_info": "unrelated_value"
	}`
	regions := map[string]bool{
		"HB1": true,
	}
	options, err := st.MakeStoreOpts(regions, "", "", tablePb.JsonArgs)
	assert.Assert(t, err == nil)
	assert.Assert(t, len(options) == 3)
}

func TestRodisValidateTableArgs(t *testing.T) {
	st := Create()

	tablePb := &pb.Table{
		TableName: "test",
		JsonArgs:  "not_a_json",
	}
	err := st.ValidateCreateTableArgs(tablePb)
	assert.Assert(t, err != nil)

	tablePb.JsonArgs = `{
		"zk_hosts": [
			"127.0.0.1:1001",
			"127.0.0.1:1002",
			"127.0.0.1:1003"
		],
		"cluster_prefix": "/test/rodis/test",
		"unrelated_info": "unrelated_value"
	}`
	err = st.ValidateCreateTableArgs(tablePb)
	assert.Assert(t, err != nil)

	tablePb.JsonArgs = `{
		"zk_hosts": [
			"127.0.0.1:1001",
			"127.0.0.1:1002",
			"127.0.0.1:1003"
		],
		"cluster_prefix": "/test/rodis/test",
		"btq_prefix": "unrelated_value"
	}`
	err = st.ValidateCreateTableArgs(tablePb)
	assert.Assert(t, err != nil)

	tablePb.JsonArgs = `{
		"zk_hosts": [
			"127.0.0.1:1001",
			"127.0.0.1:1002",
			"127.0.0.1:1003"
		],
		"cluster_prefix": "/test/rodis/test",
		"btq_prefix": "dummy"
	}`
	err = st.ValidateCreateTableArgs(tablePb)
	assert.NilError(t, err)
}

func TestGetServerResource(t *testing.T) {
	st := Create()

	resource, err := st.GetServerResource(nil, "node1")
	assert.Assert(t, resource == nil)
	assert.Assert(t, err != nil)
	logging.Info("error: %s", err)

	resource, err = st.GetServerResource(map[string]string{
		"unrelated_key": "1234",
	}, "node1")
	assert.Assert(t, resource == nil)
	assert.Assert(t, err != nil)
	logging.Info("error: %s", err)

	resource, err = st.GetServerResource(map[string]string{
		pb.RodisServerStat_dbcap_bytes.String(): "abcdefg",
	}, "node1")
	assert.Assert(t, resource == nil)
	assert.Assert(t, err != nil)
	logging.Info("error: %s", err)

	resource, err = st.GetServerResource(map[string]string{
		pb.RodisServerStat_dbcap_bytes.String(): "1234567890",
	}, "node1")
	assert.Assert(t, resource != nil)
	assert.Assert(t, err == nil)
	assert.Equal(t, resource[utils.DISK_CAP], int64(1234567890))

	resource, err = st.GetServerResource(map[string]string{
		pb.RodisServerStat_dbcpu_cores.String(): "abcdefg",
	}, "node1")
	assert.Assert(t, resource == nil)
	assert.Assert(t, err != nil)
	logging.Info("error: %s", err)

	resource, err = st.GetServerResource(map[string]string{
		pb.RodisServerStat_dbcpu_cores.String(): "1280",
		pb.StdStat_cpu_percent.String():         "aaaaaaa",
	}, "node1")
	assert.Assert(t, resource != nil)
	assert.Assert(t, err == nil)
	assert.Equal(t, len(resource), 1)
	assert.Equal(t, resource[utils.CPU], int64(1280))

	resource, err = st.GetServerResource(map[string]string{
		pb.RodisServerStat_dbcpu_cores.String(): "1280",
		pb.StdStat_cpu_percent.String():         "6600",
	}, "node1")
	assert.Assert(t, resource != nil)
	assert.Assert(t, err == nil)
	assert.Equal(t, len(resource), 2)
	assert.Equal(t, resource[utils.CPU], int64(1280))
	assert.Equal(t, resource[utils.KSAT_CPU], int64(6600))
}
