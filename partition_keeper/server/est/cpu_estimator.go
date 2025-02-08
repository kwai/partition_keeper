package est

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

const (
	CPU_CORE_ESTIMATOR = "cpu_core"
)

type CpuCoreEstimator struct {
}

func (d *CpuCoreEstimator) Score(hu utils.HardwareUnit) ScoreType {
	cpu := hu[utils.CPU]
	if cpu == 0 {
		return INVALID_SCORE
	}

	ans := cpu / 10
	if ans >= int64(MAX_SCORE) {
		return MAX_SCORE
	} else if ans < int64(MIN_SCORE) {
		return MIN_SCORE
	} else {
		return ScoreType(ans)
	}
}

func (d *CpuCoreEstimator) Name() string {
	return CPU_CORE_ESTIMATOR
}

func init() {
	registerEstimatorCreator(CPU_CORE_ESTIMATOR, func() Estimator {
		return &CpuCoreEstimator{}
	})
}
