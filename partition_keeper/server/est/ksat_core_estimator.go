package est

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

const (
	KSAT_CORE_ESTIMATOR = "ksat_core"
)

type KsatCoreEstimator struct {
}

func (d *KsatCoreEstimator) Score(hu utils.HardwareUnit) ScoreType {
	cpu := hu[utils.KSAT_CPU]
	if cpu == 0 {
		return INVALID_SCORE
	}

	// add 5 so as to round to the nearest
	ans := (cpu + 5) / 10
	if ans >= int64(MAX_SCORE) {
		return MAX_SCORE
	} else if ans < int64(MIN_SCORE) {
		return MIN_SCORE
	} else {
		return ScoreType(ans)
	}
}

func (d *KsatCoreEstimator) Name() string {
	return KSAT_CORE_ESTIMATOR
}

func init() {
	registerEstimatorCreator(KSAT_CORE_ESTIMATOR, func() Estimator {
		return &KsatCoreEstimator{}
	})
}
