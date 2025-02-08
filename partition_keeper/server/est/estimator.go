package est

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type ScoreType = int32

const (
	INVALID_SCORE = ScoreType(0)
	MIN_SCORE     = ScoreType(1)
	MAX_SCORE     = ScoreType(10000)
)

type Estimator interface {
	Name() string
	// an estimator should give a score
	// between [0, MAX_SCORE].
	//
	// 0 stands for an invalid state which
	// means the estimator can't judge the score
	// of a node. this is a very strong
	// signal to the scheduler that the
	// node may serve in an abnormal state.
	Score(utils.HardwareUnit) ScoreType
}
