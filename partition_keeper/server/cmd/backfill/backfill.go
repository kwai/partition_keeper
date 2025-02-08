package backfill

import (
	"container/list"
	"fmt"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/base"
)

type BackfillHandler struct {
	*base.HandlerParams
	logName string
}

func NewBackfillHandler(p *base.HandlerParams) base.CmdHandler {
	return &BackfillHandler{
		HandlerParams: p,
		logName:       fmt.Sprintf("%s-%s-%s", p.ServiceName, p.TableName, p.TaskName),
	}
}

func (m *BackfillHandler) CheckMimicArgs(args map[string]string, tablePb *pb.Table) error {
	return nil
}

func (m *BackfillHandler) BeforeExecutionDurable(execHistory *list.List) bool {
	return true
}

func (m *BackfillHandler) BorrowFromHistory(
	execHistory *list.List,
	args map[string]string,
) int64 {
	return base.INVALID_SESSION_ID
}

func (m *BackfillHandler) CleanOneExecution(execInfo *pb.TaskExecInfo) bool {
	return true
}

func (m *BackfillHandler) CleanAllExecutions() bool {
	return true
}
