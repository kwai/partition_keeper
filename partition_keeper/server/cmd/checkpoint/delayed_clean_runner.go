package checkpoint

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/delay_execute"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	cmd_base "github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type specializationExecutorInfo struct {
	ExecuteContext cmd_base.HandlerParams `json:"execute_context"`
}

func cleanAllCheckpointsCallback(data []byte) delay_execute.ExecutorFunc {
	var cleanerInfo specializationExecutorInfo
	utils.UnmarshalJsonOrDie(data, &cleanerInfo)
	logging.Info("create checkpoint handler by args: %v", cleanerInfo.ExecuteContext)

	checkpointHandler := NewCheckpointHandler(&cleanerInfo.ExecuteContext)
	return checkpointHandler.CleanAllExecutions
}

func init() {
	delay_execute.RegisterExecutorCreatorByType(TASK_NAME, cleanAllCheckpointsCallback)
}
