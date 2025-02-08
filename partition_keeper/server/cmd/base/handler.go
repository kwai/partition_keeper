package base

import (
	"container/list"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
)

type CmdHandler interface {
	CheckMimicArgs(args map[string]string, tablePb *pb.Table) error
	BeforeExecutionDurable(execHistory *list.List) bool
	BorrowFromHistory(execHistory *list.List, args map[string]string) int64
	CleanOneExecution(execInfo *pb.TaskExecInfo) bool
	CleanAllExecutions() bool
}

type HandlerParams struct {
	TableId     int32             `json:"table_id"`
	ServiceName string            `json:"service_name"`
	TableName   string            `json:"table_name"`
	TaskName    string            `json:"task_name"`
	Regions     []string          `json:"regions"`
	KconfPath   string            `json:"kconf_path"`
	Args        map[string]string `json:"args"`
}

type CreateHandler func(params *HandlerParams) CmdHandler
