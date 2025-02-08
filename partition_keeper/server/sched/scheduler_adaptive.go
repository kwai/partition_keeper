package sched

const (
	ADAPTIVE_SCHEDULER = "adaptive"
)

type adaptiveScheduler struct {
	schedulerBase
}

func (s *adaptiveScheduler) Name() string {
	return ADAPTIVE_SCHEDULER
}

func (s *adaptiveScheduler) SupportSplit() bool {
	return true
}

func newAdaptiveScheduler(logName string) Scheduler {
	output := &adaptiveScheduler{}
	output.appendSubScheduler(&adaptivePrimaryInspector{})
	output.appendSubScheduler(&adaptivePartitionInspector{})
	output.appendSubScheduler(&restartOpInspector{})
	output.appendSubScheduler(&adaptivePartitionBalancer{})
	output.appendSubScheduler(&primaryBalancer{})
	output.installLogName(logName)
	return output
}

func init() {
	registerSchedulerCreator(ADAPTIVE_SCHEDULER, newAdaptiveScheduler)
}
