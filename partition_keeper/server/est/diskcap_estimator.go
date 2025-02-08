package est

import "github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

const (
	GB_10              = int64(10 * 1024 * 1024 * 1024)
	DISK_CAP_ESTIMATOR = "disk_cap"
)

type DiskCapEstimator struct {
}

func (d *DiskCapEstimator) Score(hu utils.HardwareUnit) ScoreType {
	diskBytes := hu[utils.DISK_CAP]
	if diskBytes == 0 {
		return INVALID_SCORE
	}
	ans := (diskBytes + GB_10/2) / GB_10
	if ans >= int64(MAX_SCORE) {
		return MAX_SCORE
	} else if ans < int64(MIN_SCORE) {
		return MIN_SCORE
	} else {
		return ScoreType(ans)
	}
}

func (d *DiskCapEstimator) Name() string {
	return DISK_CAP_ESTIMATOR
}

func init() {
	registerEstimatorCreator(DISK_CAP_ESTIMATOR, func() Estimator {
		return &DiskCapEstimator{}
	})
}
