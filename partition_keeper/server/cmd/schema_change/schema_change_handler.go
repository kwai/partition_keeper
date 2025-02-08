package schema_change

import (
	"container/list"
	"fmt"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	cmd_base "github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/base"
)

type SchemaChangeHandler struct {
	*cmd_base.HandlerParams
	logName string
}

func NewSchemaChangeHandler(params *cmd_base.HandlerParams) cmd_base.CmdHandler {
	return &SchemaChangeHandler{
		HandlerParams: params,
		logName: fmt.Sprintf(
			"%s-%s-%s",
			params.ServiceName,
			params.TableName,
			params.TaskName,
		),
	}
}

func (s *SchemaChangeHandler) CheckMimicArgs(args map[string]string, tablePb *pb.Table) error {
	return nil
}

func (s *SchemaChangeHandler) BeforeExecutionDurable(execHistory *list.List) bool {
	return true
}

func (s *SchemaChangeHandler) BorrowFromHistory(
	execHistory *list.List,
	args map[string]string,
) int64 {
	return cmd_base.INVALID_SESSION_ID
}

func (s *SchemaChangeHandler) CleanOneExecution(execInfo *pb.TaskExecInfo) bool {
	return true
}

func (s *SchemaChangeHandler) CleanAllExecutions() bool {
	return true
}
