package rodis_store

import (
	"strconv"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
)

func GetSequenceNumber(infos map[string]string) int64 {
	key := pb.RodisReplicaStat_name[int32(pb.RodisReplicaStat_seq)]
	if val, ok := infos[key]; !ok {
		return -1
	} else {
		ans, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return -1
		}
		return ans
	}
}
