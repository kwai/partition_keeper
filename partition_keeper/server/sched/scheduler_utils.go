package sched

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
)

func HubAllNodesReady(logName, hub string, nodes []*node_mgr.NodeInfo) bool {
	activeNodes := 0
	for _, node := range nodes {
		if node.Op != pb.AdminNodeOp_kOffline {
			activeNodes++
			if node.WeightedScore() <= 0 {
				logging.Warning(
					"%s: node %s doesn't have valid score, not ready for schedule",
					logName,
					node.LogStr(),
				)
				return false
			}
		}
	}
	if activeNodes > 0 {
		return true
	} else {
		logging.Warning("%s: all nodes in %s are offline, not ready for schedule", logName, hub)
		return false
	}
}

func EstimateTableReplicas(
	logName string,
	nodes *node_mgr.NodeStats,
	tbl table_model.TableModel,
) (map[string]int, error) {
	output := make(map[string]int)
	hubs := nodes.DivideByHubs(true)
	for hubName, hub := range hubs {
		if len(hub) == 0 {
			continue
		}

		var totalScores int64 = 0
		for _, node := range hub {
			logging.Assert(node.WeightedScore() >= node_mgr.INVALID_WEIGHTED_SCORE, "")
			n := int64(node.WeightedScore())
			totalScores += n
			if totalScores < 0 || totalScores < n {
				logging.Assert(
					false,
					"%s: %s total scores overflow, please check",
					logName,
					tbl.GetInfo().TableName,
				)
			}
		}

		if totalScores <= 0 {
			logging.Warning(
				"%s %s: %s all node scores are 0, can't estimate replicas",
				logName,
				tbl.GetInfo().TableName,
				hubName,
			)
			return nil, fmt.Errorf("can't estimate %s@%s, missing info", hubName, logName)
		}

		remainedCount := int(tbl.GetInfo().PartsCount)
		for _, node := range hub {
			atLeastCount := int64(
				node.WeightedScore(),
			) * int64(
				tbl.GetInfo().PartsCount,
			) / totalScores
			logging.Assert(
				atLeastCount >= 0,
				"%s-%s: %d times %d overflow",
				logName,
				tbl.GetInfo().TableName,
				node.WeightedScore(),
				tbl.GetInfo().PartsCount,
			)
			output[node.Id] = int(atLeastCount)
			remainedCount -= int(atLeastCount)
		}
		logging.Assert(remainedCount < len(hub), "")

		sort.Slice(hub, func(i, j int) bool {
			n1, n2 := hub[i], hub[j]
			// for cloud deployed service, sort with node index
			// can ensure a indexed node's #replicas are stable
			if n1.NodeIndex != n2.NodeIndex {
				return n1.NodeIndex < n2.NodeIndex
			}
			return n1.Id < n2.Id
		})
		r := rand.New(rand.NewSource(int64(tbl.GetInfo().TableId)))
		r.Shuffle(len(hub), func(i, j int) {
			hub[i], hub[j] = hub[j], hub[i]
		})

		for i := 0; i < remainedCount; i++ {
			output[hub[i].Id]++
		}
	}
	return output, nil
}
