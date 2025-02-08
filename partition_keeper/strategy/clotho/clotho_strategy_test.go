package clotho

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

func TestCheckSwitchPrimary(t *testing.T) {
	*flagClothoDisallowFallback = true
	st := Create()
	tpid := utils.MakeTblPartID(1, 1)
	from := &pb.PartitionReplica{
		Role:           pb.ReplicaRole_kLearner,
		ReadyToPromote: false,
		StatisticsInfo: make(map[string]string),
	}
	to := &pb.PartitionReplica{
		Role:           pb.ReplicaRole_kLearner,
		ReadyToPromote: false,
		StatisticsInfo: make(map[string]string),
	}
	// no paused
	result := st.CheckSwitchPrimary(tpid, from, to)
	assert.Equal(t, result, false)

	from.StatisticsInfo[pb.StdReplicaStat_name[int32(pb.StdReplicaStat_stream_load_paused)]] = "true"
	from.StatisticsInfo[pb.StdReplicaStat_name[int32(pb.StdReplicaStat_stream_load_offset)]] = "1"

	result = st.CheckSwitchPrimary(tpid, from, to)
	assert.Equal(t, result, false)

	to.StatisticsInfo[pb.StdReplicaStat_name[int32(pb.StdReplicaStat_stream_load_paused)]] = "true"
	to.StatisticsInfo[pb.StdReplicaStat_name[int32(pb.StdReplicaStat_stream_load_offset)]] = "0"
	result = st.CheckSwitchPrimary(tpid, from, to)
	assert.Equal(t, result, false)

	to.StatisticsInfo[pb.StdReplicaStat_name[int32(pb.StdReplicaStat_stream_load_offset)]] = "1"
	result = st.CheckSwitchPrimary(tpid, from, to)
	assert.Equal(t, result, true)
	*flagClothoDisallowFallback = false
}

func TestCheckSwitchPrimary2(t *testing.T) {
	st := Create()
	tpid := utils.MakeTblPartID(1, 1)
	from := &pb.PartitionReplica{
		Role:           pb.ReplicaRole_kLearner,
		ReadyToPromote: false,
		StatisticsInfo: make(map[string]string),
	}
	to := &pb.PartitionReplica{
		Role:           pb.ReplicaRole_kLearner,
		ReadyToPromote: false,
		StatisticsInfo: make(map[string]string),
	}
	result := st.CheckSwitchPrimary(tpid, from, to)
	assert.Equal(t, result, true)
}
