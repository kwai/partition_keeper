package sched

import "github.com/kuaishou/open_partition_keeper/partition_keeper/server/table_model"

const (
	SHADED_SCHEDUELR = "shaded"
)

type ShadedScheduler struct {
	schedulerBase
}

func (s *ShadedScheduler) Name() string {
	return SHADED_SCHEDUELR
}

// TODO(huyifan03): support split for shaded scheduler
func (s *ShadedScheduler) SupportSplit() bool {
	return false
}

func NewShadedScheduler(logName string, baseTable table_model.TableModel) Scheduler {
	output := &ShadedScheduler{}

	output.appendSubScheduler(newTableShader(baseTable, true))
	output.appendSubScheduler(&adaptivePrimaryInspector{})
	output.appendSubScheduler(&restartOpInspector{})
	output.appendSubScheduler(newTableShader(baseTable, false))

	output.installLogName(logName)
	return output
}
