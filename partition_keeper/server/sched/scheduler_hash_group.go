package sched

const (
	HASH_GROUP_SCHEDULER = "hash_group"
)

type staticPartitionScheduler struct {
	schedulerBase
}

func (s *staticPartitionScheduler) Name() string {
	return HASH_GROUP_SCHEDULER
}

func (s *staticPartitionScheduler) SupportSplit() bool {
	return false
}

func newHashGroupScheduler(logName string) Scheduler {
	output := &staticPartitionScheduler{}
	output.appendSubScheduler(&adaptivePrimaryInspector{})
	output.appendSubScheduler(&hashGroupArranger{})
	output.appendSubScheduler(&restartOpInspector{})
	output.appendSubScheduler(&primaryBalancer{})
	output.installLogName(logName)
	return output
}

func init() {
	registerSchedulerCreator(HASH_GROUP_SCHEDULER, newHashGroupScheduler)
}
