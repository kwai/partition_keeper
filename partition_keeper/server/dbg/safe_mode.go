package dbg

import (
	"errors"
	"sync/atomic"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
)

var (
	safeMode               = int32(0)
	ErrSkipRunAsInSafeMode = errors.New("skip run as in safe mode")
)

func RunInSafeMode() bool {
	return atomic.LoadInt32(&safeMode) != 0
}

func SetSafeMode(val int32) {
	logging.Info("set safe mode to %d", val)
	atomic.StoreInt32(&safeMode, val)
}
