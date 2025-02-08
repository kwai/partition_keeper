package node_mgr

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/est"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

func TestRescore(t *testing.T) {
	te := setupNodeStatTestEnv(t, map[string]string{"yz1": "YZ"}, pb.NodeFailureDomainType_PROCESS)
	ping1 := map[string]*NodePing{
		"node1": {true, "YZ", utils.FromHostPort("127.0.0.1:1001"), "12341", 2001, ""},
		"node2": {true, "YZ", utils.FromHostPort("127.0.0.2:1001"), "12341", 2001, ""},
	}

	te.lock.LockWrite()
	defer te.lock.UnlockWrite()
	te.nodeStats.UpdateStats(ping1)

	assert.Equal(t, len(te.nodeStats.idNodes), 2)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node1").Score, est.INVALID_SCORE)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2").Score, est.INVALID_SCORE)

	diskEst := est.NewEstimator(est.DISK_CAP_ESTIMATOR)

	logging.Info("can't refresh all scores")
	te.nodeStats.MustGetNodeInfo("node1").resource = utils.HardwareUnit{
		utils.DISK_CAP: est.GB_10 * 100,
	}

	ans := te.nodeStats.RefreshScore(diskEst, false)
	assert.Equal(t, ans, false)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node1").Score, est.ScoreType(100))
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2").Score, est.INVALID_SCORE)
	te.checkZkSynced()

	logging.Info("refresh all scores succeed")
	te.nodeStats.MustGetNodeInfo("node2").resource = utils.HardwareUnit{
		utils.DISK_CAP: est.GB_10 * 100,
	}
	ans = te.nodeStats.RefreshScore(diskEst, false)
	assert.Assert(t, ans)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node1").Score, est.ScoreType(100))
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2").Score, est.ScoreType(100))
	te.checkZkSynced()

	logging.Info(
		"refresh will succeed if can't score some already scored node as long as not force",
	)
	te.nodeStats.MustGetNodeInfo("node2").resource = nil
	te.nodeStats.MustGetNodeInfo("node1").resource = utils.HardwareUnit{
		utils.DISK_CAP: 200 * est.GB_10,
	}
	ans = te.nodeStats.RefreshScore(diskEst, false)
	assert.Assert(t, ans)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node1").Score, est.ScoreType(200))
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2").Score, est.ScoreType(100))
	te.checkZkSynced()

	logging.Info("force to rescore fail, as can't get score of node2")
	ans = te.nodeStats.RefreshScore(diskEst, true)
	assert.Equal(t, ans, false)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node1").Score, est.ScoreType(200))
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2").Score, est.ScoreType(100))

	logging.Info("force to rescore succeed, as node2 offline, and it's score is reset")
	err := te.nodeStats.AdminNodes(
		utils.FromHostPorts([]string{"127.0.0.2:1001"}),
		true,
		pb.AdminNodeOp_kOffline,
		nil,
	)
	assert.Equal(t, err[0].Code, est.ScoreType(pb.AdminError_kOk))
	ans = te.nodeStats.RefreshScore(diskEst, true)
	assert.Equal(t, ans, true)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node1").Score, est.ScoreType(200))
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2").Score, est.INVALID_SCORE)
	te.checkZkSynced()

	logging.Info("rescore succeed without force, as node2 is offline")
	ans = te.nodeStats.RefreshScore(diskEst, false)
	assert.Equal(t, ans, true)
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node1").Score, est.ScoreType(200))
	assert.Equal(t, te.nodeStats.MustGetNodeInfo("node2").Score, est.INVALID_SCORE)
}
