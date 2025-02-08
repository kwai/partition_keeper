package est

import "github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

const (
	DEFAULT_ESTIMATOR = "default"
)

type DefaultEstimator struct {
}

func (n *DefaultEstimator) Score(hu utils.HardwareUnit) ScoreType {
	return 100
}

func (n *DefaultEstimator) Name() string {
	return DEFAULT_ESTIMATOR
}

func init() {
	registerEstimatorCreator(DEFAULT_ESTIMATOR, func() Estimator {
		return &DefaultEstimator{}
	})
}
