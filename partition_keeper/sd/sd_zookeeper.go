package sd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-zookeeper/zk"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
)

const (
	optZkHosts        = "zk_hosts"
	SdTypeZK   SdType = "zk"
)

func ZkSdSetZKHosts(zkHosts string) SDFactoryOpts {
	return func(opt map[string]string) map[string]string {
		return AddKeyValue(opt, optZkHosts, zkHosts)
	}
}

type zkServiceDiscovery struct {
	serviceName string
	metaStore   metastore.MetaStore
}

func zkServiceDiscoveryCreator(serviceName string, opts map[string]string) ServiceDiscovery {
	zkHosts := strings.Split(opts[optZkHosts], ",")
	return newZkServiceDiscovery(serviceName, zkHosts)
}

func newZkServiceDiscovery(serviceName string, zkHosts []string) *zkServiceDiscovery {
	return &zkServiceDiscovery{
		serviceName: serviceName,
		metaStore: metastore.GetGlobalPool().
			Get(zkHosts, zk.WorldACL(zk.PermAll), "", []byte("")),
	}
}

func (sd *zkServiceDiscovery) Stop() {}

func (sd *zkServiceDiscovery) UpdateAzs(az map[string]bool) error {
	return nil
}

func (sd *zkServiceDiscovery) GetNodes() (map[string]*pb.ServiceNode, error) {
	path := "/discovery/" + sd.serviceName
	nodes, exist, succ := sd.metaStore.Children(context.Background(), path)
	if !succ {
		return nil, fmt.Errorf("read zk error")
	}
	if !exist {
		return nil, nil
	}
	output := map[string]*pb.ServiceNode{}
	for _, node := range nodes {
		childPath := path + "/" + node
		data, exists, succ := sd.metaStore.Get(context.Background(), childPath)
		if !succ {
			return nil, fmt.Errorf("read zk error")
		}
		if exists {
			sn := &pb.ServiceNode{}
			err := json.Unmarshal(data, sn)
			if err != nil {
				return nil, err
			} else {
				output[node] = sn
			}
		}
	}
	return output, nil
}

func init() {
	sdFactory[SdTypeZK] = zkServiceDiscoveryCreator
}
