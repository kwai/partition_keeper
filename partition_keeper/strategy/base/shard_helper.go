package base

import (
	"encoding/json"
	"fmt"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched"
)

type msgQueueShardsJsonArgs struct {
	MsgQueueShards *int `json:"msg_queue_shards"`
}

func getMsgQueueShards(args string) (*int, error) {
	output := &msgQueueShardsJsonArgs{}
	if err := json.Unmarshal([]byte(args), output); err != nil {
		return nil, err
	}
	return output.MsgQueueShards, nil
}

func ValidateMsgQueueShards(partsCount int, tableJsonArgs string) error {
	shards, err := getMsgQueueShards(tableJsonArgs)
	if err != nil {
		return err
	}
	if shards == nil {
		return nil
	}
	if *shards <= 0 || partsCount%(*shards) != 0 {
		return fmt.Errorf("parts_count(%d) mod queue_shards(%d) != 0", partsCount, *shards)
	}
	return nil
}

func GetScheduler(partsCount int, jsonArgs string) (string, int) {
	shards, err := getMsgQueueShards(jsonArgs)
	if err != nil {
		return sched.ADAPTIVE_SCHEDULER, 0
	}
	if shards == nil {
		return sched.ADAPTIVE_SCHEDULER, 0
	}
	if *shards <= 0 || partsCount%(*shards) != 0 || *shards == partsCount {
		return sched.ADAPTIVE_SCHEDULER, *shards
	}
	return sched.HASH_GROUP_SCHEDULER, *shards
}
