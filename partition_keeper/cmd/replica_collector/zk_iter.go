package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type ZkIter struct {
	zkConn      metastore.MetaStore
	serviceRoot string

	hosts   []*utils.RpcNode
	current int
}

func NewZkIter(zkUrl string) HostIterator {
	pathStart := strings.Index(zkUrl, "/")
	if pathStart == -1 {
		logging.Fatal("can't find path in %s", zkUrl)
	}
	zkAddr := zkUrl[0:pathStart]
	path := zkUrl[pathStart:]

	output := &ZkIter{
		zkConn: metastore.CreateZookeeperStore(
			[]string{zkAddr},
			time.Second*10,
			zk.WorldACL(zk.PermRead),
			"",
			[]byte(""),
		),
		serviceRoot: fmt.Sprintf("%s/service", path),
	}
	output.initialize()
	return output
}

func (z *ZkIter) initialize() {
	children, exists, succ := z.zkConn.Children(context.Background(), z.serviceRoot)
	logging.Assert(succ && exists, "can't find path %s", z.serviceRoot)

	for _, service := range children {
		nodesPath := fmt.Sprintf("%s/%s/nodes", z.serviceRoot, service)
		logging.Info("get all nodes for service %s", nodesPath)
		nodes, exists, succ := z.zkConn.Children(context.Background(), nodesPath)
		logging.Assert(exists && succ, "")
		for _, nd := range nodes {
			nodePath := fmt.Sprintf("%s/%s", nodesPath, nd)
			data, exists, succ := z.zkConn.Get(context.Background(), nodePath)
			logging.Assert(exists && succ, "")

			info := &node_mgr.NodeInfo{}
			utils.UnmarshalJsonOrDie(data, info)
			z.hosts = append(z.hosts, info.Address)
		}
	}

	z.current = 0
}

func (z *ZkIter) Next() (res *utils.RpcNode) {
	if z.current < len(z.hosts) {
		res = z.hosts[z.current]
		z.current++
		return
	}
	res = nil
	return
}
