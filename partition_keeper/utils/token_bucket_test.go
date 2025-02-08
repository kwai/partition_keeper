package utils

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"gotest.tools/assert"
)

func TestTokenBucket(t *testing.T) {
	workerCount := []int{2, 4, 8, 16, 32}
	for _, c := range workerCount {
		tokenBucket := NewTokenBucket(200, 100)
		accessCount := int64(0)
		exit := int32(0)

		wg := sync.WaitGroup{}
		for i := 0; i < c; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				count := 0
				for atomic.LoadInt32(&exit) == 0 {
					tokenBucket.AcquireToken()
					atomic.AddInt64(&accessCount, 1)
					count += 1
				}
				logging.Info("count: %d", count)
			}()
		}

		time.Sleep(time.Second)
		atomic.StoreInt32(&exit, 1)
		wg.Wait()
		logging.Info("worker count: %d, access count: %d", c, accessCount)
		assert.Assert(t, accessCount < 300, "%d", accessCount)
	}
}
