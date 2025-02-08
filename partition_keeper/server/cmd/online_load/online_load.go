package online_load

import (
	"container/list"
	"fmt"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/base"
)

type OnlineLoadHandler struct {
	*base.HandlerParams
	logName string
}

func NewOnlineLoadHandler(p *base.HandlerParams) base.CmdHandler {
	return &OnlineLoadHandler{
		HandlerParams: p,
		logName:       fmt.Sprintf("%s-%s-%s", p.ServiceName, p.TableName, p.TaskName),
	}
}

func (m *OnlineLoadHandler) CheckMimicArgs(args map[string]string, tablePb *pb.Table) error {
	return nil
}

func (m *OnlineLoadHandler) BeforeExecutionDurable(execHistory *list.List) bool {
	return true
}

func (m *OnlineLoadHandler) BorrowFromHistory(
	execHistory *list.List,
	args map[string]string,
) int64 {
	return base.INVALID_SESSION_ID
}

func (m *OnlineLoadHandler) CleanOneExecution(execInfo *pb.TaskExecInfo) bool {
	return true
}

func (m *OnlineLoadHandler) CleanAllExecutions() bool {
	return true
}
