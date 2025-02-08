package third_party

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
)

func AddTrafficConfig(kconfPath, kessName, tableName string) *pb.ErrorStatus {
	return pb.AdminErrorMsg(pb.AdminError_kNotImplemented, "")
}
