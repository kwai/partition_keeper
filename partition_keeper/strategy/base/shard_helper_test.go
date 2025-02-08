package base

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched"
	"gotest.tools/assert"
)

func TestMsgShards(t *testing.T) {
	testCases := []struct {
		args         string
		parts        int
		validateOk   bool
		schedType    string
		outputShards int
	}{
		{"{invalid_json", 4, false, sched.ADAPTIVE_SCHEDULER, 0},
		{`{"no_btq_shards": true}`, 4, true, sched.ADAPTIVE_SCHEDULER, 0},
		{`{"msg_queue_shards": 0}`, 5, false, sched.ADAPTIVE_SCHEDULER, 0},
		{`{"msg_queue_shards": 4}`, 5, false, sched.ADAPTIVE_SCHEDULER, 4},
		{`{"msg_queue_shards": 4}`, 4, true, sched.ADAPTIVE_SCHEDULER, 4},
		{`{"msg_queue_shards": 4}`, 8, true, sched.HASH_GROUP_SCHEDULER, 4},
	}

	for i := range testCases {
		tc := &(testCases[i])

		err := ValidateMsgQueueShards(tc.parts, tc.args)
		assert.Equal(t, err == nil, tc.validateOk)

		schedType, shardCount := GetScheduler(tc.parts, tc.args)
		assert.Equal(t, schedType, tc.schedType)
		assert.Equal(t, shardCount, tc.outputShards)
	}
}
