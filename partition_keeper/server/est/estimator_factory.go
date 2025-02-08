package est

import "github.com/kuaishou/open_partition_keeper/partition_keeper/logging"

type estimatorCreator func() Estimator

var estimatorFactories = map[string]estimatorCreator{}

func NewEstimator(name string) Estimator {
	if creator, ok := estimatorFactories[name]; ok {
		return creator()
	} else {
		return nil
	}
}

func registerEstimatorCreator(name string, creator estimatorCreator) {
	if _, ok := estimatorFactories[name]; ok {
		logging.Assert(false, "conflict score estimators %s", name)
	}
	estimatorFactories[name] = creator
}
