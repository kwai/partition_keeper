package clotho

import (
	"context"
	"flag"
	"strconv"
	"sync"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

var (
	flagClothoDisallowFallback = flag.Bool(
		"clotho_disallow_fallback",
		false,
		"disallow fallback when switch to primary",
	)
)

type ClothoStrategy struct {
	base.DummyStrategy
}

func (c *ClothoStrategy) GetType() pb.ServiceType {
	return pb.ServiceType_colossusdb_clotho
}

func (c *ClothoStrategy) SupportSplit() bool {
	return false
}

func (c *ClothoStrategy) GetStreamLoadOffset(infos map[string]string) (bool, int64) {
	key := pb.StdReplicaStat_name[int32(pb.StdReplicaStat_stream_load_offset)]
	if val, ok := infos[key]; !ok {
		return false, -1
	} else {
		ans, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return false, -1
		}
		return true, ans
	}
}

func (c *ClothoStrategy) StreamLoadPaused(infos map[string]string) bool {
	key := pb.StdReplicaStat_name[int32(pb.StdReplicaStat_stream_load_paused)]
	if val, ok := infos[key]; ok {
		return val == "true"
	}
	return false
}

func (c *ClothoStrategy) CheckSwitchPrimary(
	tpid utils.TblPartID,
	from *pb.PartitionReplica,
	to *pb.PartitionReplica,
) bool {
	if !*flagClothoDisallowFallback {
		return true
	}
	if !c.StreamLoadPaused(from.StatisticsInfo) {
		return false
	}

	ok, fromOffset := c.GetStreamLoadOffset(from.StatisticsInfo)
	if !ok {
		logging.Warning(
			"%s: no stream_load_offset reported from %s",
			tpid.String(),
			from.NodeUniqueId,
		)
		return false
	}
	ok, toOffset := c.GetStreamLoadOffset(to.StatisticsInfo)
	if !ok {
		logging.Warning(
			"%s: no stream_load_offset reported from %s",
			tpid.String(),
			to.NodeUniqueId,
		)
		return false
	}
	return fromOffset <= toOffset
}

func (c *ClothoStrategy) NotifySwitchPrimary(
	tpid utils.TblPartID,
	wg *sync.WaitGroup,
	address *utils.RpcNode,
	nodesConn *rpc.ConnPool,
) {
	if !*flagClothoDisallowFallback {
		return
	}
	pauseReq := &pb.PrepareSwitchPrimaryRequest{
		TableId:     tpid.Table(),
		PartitionId: tpid.Part(),
	}

	wg.Add(1)
	go nodesConn.Run(address, func(client interface{}) error {
		defer wg.Done()
		psc := client.(pb.PartitionServiceClient)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var e error
		status, e := psc.PrepareSwitchPrimary(ctx, pauseReq)

		if e != nil {
			logging.Warning("[change_state] ask %s to PrepareSwitchPrimary t%d.p%d failed: %s",
				address.String(), pauseReq.TableId, pauseReq.PartitionId, e.Error())
		} else if status.Code != int32(pb.PartitionError_kOK) {
			logging.Warning("[change_state] ask %s to PrepareSwitchPrimary t%d.p%d response: %s",
				address.String(), pauseReq.TableId, pauseReq.PartitionId, status.Message)
		} else {
			logging.Info("[change_state] ask %s to PrepareSwitchPrimary t%d.p%d succeed",
				address.String(), pauseReq.TableId, pauseReq.PartitionId)
		}
		return e
	})
}

func Create() base.StrategyBase {
	return &ClothoStrategy{}
}
