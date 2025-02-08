package sd

import (
	"sync"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
)

var (
	connPoolOnce sync.Once
	connPool     *rpc.ConnPool
)

func getConnectionPoolInst() *rpc.ConnPool {
	connPoolOnce.Do(func() {
		builder := rpc.GrpcSDClientPoolBuilder{}
		connPool = builder.Build()
	})
	return connPool
}
