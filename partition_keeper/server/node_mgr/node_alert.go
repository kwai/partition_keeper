package node_mgr

import (
	"flag"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

var (
	flagNodeAlertRecoveryMinutes = flag.Int64(
		"node_alert_recovery_minutes",
		60,
		"node alert automatic recovery time",
	)
)

type NodeAlert struct {
	serviceName string
	alertTime   int64
}

func NewNodeAlert(serviceName string) *NodeAlert {
	ans := &NodeAlert{
		serviceName: serviceName,
		alertTime:   time.Now().Unix(),
	}
	return ans
}

func (n *NodeAlert) deadNodesAlert(deadCount int, deadNodes string) {
	if deadCount == 0 {
		third_party.PerfLog2(
			"reco",
			"reco.colossusdb.dead_nodes",
			third_party.GetKsnNameByServiceName(n.serviceName),
			n.serviceName,
			deadNodes,
			uint64(deadCount))
		return
	}

	if (time.Now().Unix() - n.alertTime) > (*flagNodeAlertRecoveryMinutes * 60) {
		// Long time in alert state, automatically recover a little
		third_party.PerfLog2(
			"reco",
			"reco.colossusdb.dead_nodes",
			third_party.GetKsnNameByServiceName(n.serviceName),
			n.serviceName,
			"",
			0)
		n.alertTime = time.Now().Unix()
		return
	}

	limit := int64(utils.Min(300, int(*flagNodeAlertRecoveryMinutes*30)))
	if (time.Now().Unix() - n.alertTime) > limit {
		third_party.PerfLog2(
			"reco",
			"reco.colossusdb.dead_nodes",
			third_party.GetKsnNameByServiceName(n.serviceName),
			n.serviceName,
			deadNodes,
			uint64(deadCount))
	} else {
		third_party.PerfLog2(
			"reco",
			"reco.colossusdb.dead_nodes",
			third_party.GetKsnNameByServiceName(n.serviceName),
			n.serviceName,
			deadNodes,
			0)
	}
}
