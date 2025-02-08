package rpc

import (
	"sync"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"google.golang.org/grpc"
)

type RpcHandler func(client interface{}) error

// the return value should be a grpc client
type CreateRpcClient func(conn *grpc.ClientConn) interface{}

type ConnPool struct {
	mu            sync.Mutex
	conns         map[string]*grpc.ClientConn
	clientCreator CreateRpcClient
}

func NewRpcPool(clientCreator CreateRpcClient) *ConnPool {
	return &ConnPool{
		conns:         make(map[string]*grpc.ClientConn),
		clientCreator: clientCreator,
	}
}

func (rc *ConnPool) Get(node *utils.RpcNode) *grpc.ClientConn {
	ipPort := node.String()
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if res, ok := rc.conns[ipPort]; ok {
		return res
	} else {
		conn, err := grpc.Dial(ipPort, grpc.WithInsecure())
		if err != nil {
			logging.Fatal("create rpc connection to %s failed, %s", ipPort, err.Error())
		}
		logging.Info("create a new rpc connection to %s succeed", ipPort)
		rc.conns[ipPort] = conn
		return conn
	}
}

func (rc *ConnPool) Remove(node *utils.RpcNode, conn *grpc.ClientConn) {
	ipPort := node.String()
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if cur, ok := rc.conns[ipPort]; !ok {
		return
	} else if cur == conn {
		delete(rc.conns, ipPort)
	} else {
		logging.Info("skip to close connection %s as the conn instance changed", node.String())
	}
}

func (rc *ConnPool) Run(node *utils.RpcNode, h RpcHandler) {
	conn := rc.Get(node)
	client := rc.clientCreator(conn)
	err := h(client)
	if err != nil {
		logging.Info("rpc to %s got error %s, close the connection", node.String(), err.Error())
		if err2 := conn.Close(); err2 != nil {
			logging.Error("close connection %s failed: %s", node.String(), err2.Error())
		}
		rc.Remove(node, conn)
	}
}
