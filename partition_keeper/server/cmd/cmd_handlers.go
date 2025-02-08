package cmd

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/backfill"
	cmd_base "github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/checkpoint"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/online_load"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/schema_change"
)

var (
	cmdHandlersFactory = map[string]cmd_base.CreateHandler{}
)

func RegisterCmdHandler(name string, creator cmd_base.CreateHandler) {
	if _, ok := cmdHandlersFactory[name]; ok {
		logging.Fatal("conflict handler name: %v", name)
	}
	cmdHandlersFactory[name] = creator
}

func UnregisterCmdHandler(name string) cmd_base.CreateHandler {
	output := cmdHandlersFactory[name]
	delete(cmdHandlersFactory, name)
	return output
}

func NameRegistered(name string) bool {
	_, ok := cmdHandlersFactory[name]
	return ok
}

func NewCmdHandler(name string, params *cmd_base.HandlerParams) cmd_base.CmdHandler {
	return cmdHandlersFactory[name](params)
}

func init() {
	RegisterCmdHandler(checkpoint.TASK_NAME, checkpoint.NewCheckpointHandler)
	RegisterCmdHandler(schema_change.TASK_NAME, schema_change.NewSchemaChangeHandler)
	RegisterCmdHandler(online_load.TASK_NAME, online_load.NewOnlineLoadHandler)
	RegisterCmdHandler(backfill.TASK_NAME, backfill.NewBackfillHandler)
}
