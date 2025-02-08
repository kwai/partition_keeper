package utils

import (
	"net/http"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"

	"google.golang.org/protobuf/proto"
)

func ParseListServicesReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.ListServicesRequest{}
	return result, nil
}

func ParseCreateServiceReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.CreateServiceRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseAddHubsReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.AddHubsRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseRemoveHubsReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.RemoveHubsRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseUpdateHubsReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.UpdateHubsRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseDeleteServiceReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.DeleteServiceRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseGiveHintsReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.GiveHintsRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseRecallHintsReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.RecallHintsRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseUpdateScheduleOptionsReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.UpdateScheduleOptionsRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseCreateTableReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.CreateTableRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseDeleteTableReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.DeleteTableRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseUpdateTableReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.UpdateTableRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseUpdateTableJsonArgsReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.UpdateTableJsonArgsRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseListTablesReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.ListTablesRequest{}
	ans := r.URL.Query().Get("service_name")
	if ans == "" {
		return result, nil
	} else {
		result.ServiceName = ans
		return result, nil
	}
}

func ParseQueryTableReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.QueryTableRequest{}
	var s *pb.ErrorStatus
	result.ServiceName, s = GetStringFromUrl(r, "service_name")
	if s != nil {
		return nil, s
	}
	result.TableName, s = GetStringFromUrl(r, "table_name")
	if s != nil {
		return nil, s
	}
	result.WithTasks = GetBoolFromUrl(r, "with_tasks")
	result.WithPartitions = GetBoolFromUrl(r, "with_partitions")

	return result, nil
}

func ParseManualRemoveReplicasRequest(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.ManualRemoveReplicasRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseRestoreTableRequest(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.RestoreTableRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseSplitTableReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.SplitTableRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseQueryPartitionReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.QueryPartitionRequest{}
	var s *pb.ErrorStatus
	result.ServiceName, s = GetStringFromUrl(r, "service_name")
	if s != nil {
		return nil, s
	}
	result.TableName, s = GetStringFromUrl(r, "table_name")
	if s != nil {
		return nil, s
	}

	var from_partition, to_partition int64
	from_partition, s = GetIntFromUrl(r, "from_partition")
	if s != nil {
		from_partition = 0
	}
	to_partition, s = GetIntFromUrl(r, "to_partition")
	if s != nil {
		to_partition = 0
	}
	result.FromPartition = (int32)(from_partition)
	result.ToPartition = (int32)(to_partition)

	return result, nil
}

func ParseOperateTaskReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.OperateTaskRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseDeleteTaskReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.DeleteTaskRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseQueryTaskReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.QueryTaskRequest{}
	var s *pb.ErrorStatus
	result.ServiceName, s = GetStringFromUrl(r, "service_name")
	if s != nil {
		return nil, s
	}
	result.TableName, s = GetStringFromUrl(r, "table_name")
	if s != nil {
		return nil, s
	}
	result.TaskName, s = GetStringFromUrl(r, "task_name")
	if s != nil {
		return nil, s
	}
	return result, nil
}

func ParseTriggerDeleteTaskSideEffectReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.TriggerDeleteTaskSideEffectRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseQueryTaskCurrentExecution(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.QueryTaskCurrentExecutionRequest{}
	var s *pb.ErrorStatus
	result.ServiceName, s = GetStringFromUrl(r, "service_name")
	if s != nil {
		return nil, s
	}
	result.TableName, s = GetStringFromUrl(r, "table_name")
	if s != nil {
		return nil, s
	}
	result.TaskName, s = GetStringFromUrl(r, "task_name")
	if s != nil {
		return nil, s
	}
	return result, nil
}

func ParseAdminNodeReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.AdminNodeRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseUpdateNodeWeightReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.UpdateNodeWeightRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseShrinkAzReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.ShrinkAzRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseExpandAzsReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.ExpandAzsRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseCancelExpandAzsReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.CancelExpandAzsRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseAssignHubReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.AssignHubRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseReplaceNodesReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.ReplaceNodesRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseListNodesReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.ListNodesRequest{}
	var s *pb.ErrorStatus
	result.ServiceName, s = GetStringFromUrl(r, "service_name")
	if s != nil {
		return nil, s
	}
	result.Az, s = GetStringFromUrl(r, "az")
	if s != nil {
		result.Az = ""
	}
	result.HubName, s = GetStringFromUrl(r, "hub_name")
	if s != nil {
		result.HubName = ""
	}
	result.TableName, s = GetStringFromUrl(r, "table_name")
	if s != nil {
		result.TableName = ""
	}
	return result, nil
}

func ParseQueryNodeInfoReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.QueryNodeInfoRequest{}
	var s *pb.ErrorStatus
	result.ServiceName, s = GetStringFromUrl(r, "service_name")
	if s != nil {
		return nil, s
	}
	result.NodeName, s = GetStringFromUrl(r, "node_name")
	if s != nil {
		return nil, s
	}
	var p int64
	p, s = GetIntFromUrl(r, "port")
	if s != nil {
		return nil, s
	}
	result.Port = int32(p)
	result.TableName, s = GetStringFromUrl(r, "table_name")
	if s != nil {
		result.TableName = ""
	}
	result.OnlyBrief = GetBoolFromUrl(r, "only_brief")
	result.MatchPort = GetBoolFromUrl(r, "match_port")
	return result, nil
}

func ParseQueryNodesInfoRequest(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.QueryNodesInfoRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseSwitchSchedulerStatusReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.SwitchSchedulerStatusRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseSwitchKessPollerStatusReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.SwitchKessPollerStatusRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

func ParseQueryServiceReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.QueryServiceRequest{}
	var s *pb.ErrorStatus
	result.ServiceName, s = GetStringFromUrl(r, "service_name")
	if s != nil {
		return nil, s
	}
	return result, nil
}

func ParseRemoveWatcherReq(r *http.Request) (proto.Message, *pb.ErrorStatus) {
	result := &pb.RemoveWatcherRequest{}
	if err := GetReqFromHttpBody(r, result); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}
