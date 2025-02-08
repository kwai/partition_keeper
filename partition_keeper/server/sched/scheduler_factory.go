package sched

import "github.com/kuaishou/open_partition_keeper/partition_keeper/logging"

type schedulerCreator func(logName string) Scheduler

var (
	schedulerFactory = map[string]schedulerCreator{}
)

func NewScheduler(schedulerName string, logName string) Scheduler {
	creator, ok := schedulerFactory[schedulerName]
	logging.Assert(ok, "%s: can't create scheduler from %s", logName, schedulerName)
	return creator(logName)
}

func registerSchedulerCreator(schedulerName string, creator schedulerCreator) {
	_, ok := schedulerFactory[schedulerName]
	logging.Assert(!ok, "conflict scheduler type: %s", schedulerName)
	schedulerFactory[schedulerName] = creator
}
