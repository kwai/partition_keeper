package rpc

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"google.golang.org/grpc"
	"gotest.tools/assert"
)

func TestRpcFail(t *testing.T) {
	builder := &GrpcPSClientPoolBuilder{}
	connPool := builder.Build()
	var old1 *grpc.ClientConn
	var new1 *grpc.ClientConn
	var old2 *grpc.ClientConn

	addr := utils.FromHostPort("127.0.0.1:1001")

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		old1 = connPool.Get(addr)
		connPool.Run(addr, func(psc interface{}) error {
			time.Sleep(time.Millisecond * 100)
			return errors.New("mock rpc failed")
		})
		time.Sleep(time.Millisecond * 100)
		new1 = connPool.Get(addr)
		connPool.Run(addr, func(psc interface{}) error {
			return nil
		})
	}()

	go func() {
		defer wg.Done()
		old2 = connPool.Get(addr)
		connPool.Run(addr, func(psc interface{}) error {
			time.Sleep(time.Second)
			return errors.New("mock rpc failed")
		})
	}()

	wg.Wait()
	assert.Equal(t, old1, old2)
	assert.Assert(t, old1 != new1)
	assert.Equal(t, connPool.Get(addr), new1)
}
