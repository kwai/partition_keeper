package node_mgr

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

func TestUpdateWeight(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"yz1": "YZ"}, pb.NodeFailureDomainType_PROCESS)
	defer te.teardown()
	ping := map[string]*NodePing{
		"node1": {true, "YZ", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
		"node2": {true, "YZ", utils.FromHostPort("127.0.0.2:1001"), "12341", 2001, ""},
		"node3": {true, "YZ", utils.FromHostPort("127.0.0.3:1001"), "12341", 2001, ""},
		"node4": {true, "YZ", utils.FromHostPort("127.0.0.4:1001"), "12341", 2001, ""},
	}
	te.lock.LockWrite()
	defer te.lock.UnlockWrite()

	te.nodeStats.UpdateStats(ping)
	result := te.nodeStats.UpdateWeight(nil, []WeightType{1, 10})
	assert.Equal(t, len(result), 0)

	result = te.nodeStats.UpdateWeight(utils.FromHostPorts([]string{
		"127.0.0.1:1002",
		"127.0.0.1:1001",
		"127.0.0.2:1001",
		"127.0.0.3:1001",
		"127.0.0.4:1001",
	}), []WeightType{1, 1, -1, 101, 8, 22})
	assert.Equal(t, len(result), 5)
	assert.Equal(t, result[0].Code, int32(pb.AdminError_kNodeNotExisting))
	assert.Equal(t, result[1].Code, int32(pb.AdminError_kOk))
	assert.Equal(t, result[2].Code, int32(pb.AdminError_kInvalidParameter))
	assert.Equal(t, result[3].Code, int32(pb.AdminError_kInvalidParameter))
	assert.Equal(t, result[4].Code, int32(pb.AdminError_kOk))

	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node1").Weight, DEFAULT_WEIGHT)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2").Weight, DEFAULT_WEIGHT)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node3").Weight, DEFAULT_WEIGHT)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node4").Weight, WeightType(8))
	te.checkZkSynced()
}
