package server

import (
	"flag"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"
)

var (
	flagLimitAddLearnerGlobally = flag.Bool(
		"limit_add_learner_globally",
		false,
		"limit_add_learner_globally",
	)
)

type PlanConductor struct {
	schedOptions            *pb.ScheduleOptions
	learning                map[string]int
	learned                 map[string]int
	learningParts           map[int32]bool
	maxAllowedLearningParts int32
}

func NewPlanConductor(options *pb.ScheduleOptions, table table_model.TableModel) *PlanConductor {
	output := &PlanConductor{
		schedOptions:  options,
		learning:      make(map[string]int),
		learned:       make(map[string]int),
		learningParts: make(map[int32]bool),
	}
	if *flagLimitAddLearnerGlobally {
		output.maxAllowedLearningParts = options.MaxSchedRatio * table.GetInfo().PartsCount / sched.SCHEDULE_RATIO_MAX_VALUE
		if output.maxAllowedLearningParts == 0 {
			output.maxAllowedLearningParts = 1
		}
	} else {
		output.maxAllowedLearningParts = -1
	}

	for i := int32(0); i < table.GetInfo().PartsCount; i++ {
		membership := table.GetMembership(i)
		pri, _, learners := membership.DivideRoles()
		for _, node := range pri {
			output.learned[node] += len(learners)
		}
		for _, node := range learners {
			output.learning[node] += 1
		}
		if len(learners) > 0 {
			output.learningParts[i] = true
		}
	}
	return output
}

func (pc *PlanConductor) countAddLeaner(pid int32, pri []string, learner string) {
	for _, node := range pri {
		pc.learned[node] += 1
	}
	pc.learning[learner] += 1
	pc.learningParts[pid] = true
}

func (pc *PlanConductor) TryAddLearner(
	logPrefix string,
	pid int32,
	membership *table_model.PartitionMembership,
	learner string,
) bool {
	if pc.schedOptions.MaxLearningPartsPerNode < 0 {
		logging.Verbose(1, "%s: don't add learner %s as it's disabled", logPrefix, learner)
		return false
	}
	if _, ok := pc.learningParts[pid]; !ok {
		if pc.maxAllowedLearningParts >= 0 &&
			len(pc.learningParts) >= int(pc.maxAllowedLearningParts) {
			logging.Verbose(
				1,
				"%s: don't add learner %s, global learning parts(%d), max allowed(%d)",
				logPrefix,
				learner,
				len(pc.learningParts),
				pc.maxAllowedLearningParts,
			)
			return false
		}
	}

	pri, _, _ := membership.DivideRoles()
	if pc.schedOptions.MaxLearningPartsPerNode == 0 {
		pc.countAddLeaner(pid, pri, learner)
		return true
	}
	for _, node := range pri {
		learned := pc.learned[node]
		if learned >= int(pc.schedOptions.MaxLearningPartsPerNode) {
			logging.Verbose(
				1,
				"%s: don't add learner %s as primary %s already serve %d learners",
				logPrefix,
				learner,
				node,
				learned,
			)
			return false
		}
	}
	learning := pc.learning[learner]
	if learning >= int(pc.schedOptions.MaxLearningPartsPerNode) {
		logging.Verbose(
			1,
			"%s: don't add learner %s as it already serve %d learners",
			logPrefix,
			learner,
			learning,
		)
		return false
	}
	pc.countAddLeaner(pid, pri, learner)
	return true
}
