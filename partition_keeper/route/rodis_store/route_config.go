package rodis_store

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/route/base"
)

type RouteConfig struct {
	ZkHosts       []string `json:"zk_hosts"`
	ClusterPrefix string   `json:"cluster_prefix"`
}

func MakeOptions(service, table, jsonConfig string) (*base.StoreBaseOption, error) {
	rc := &RouteConfig{}

	err := json.Unmarshal([]byte(jsonConfig), &rc)
	if err != nil {
		return nil, fmt.Errorf(
			"table %s, parse rodis route config from %s failed: %w",
			table,
			jsonConfig,
			err,
		)
	}
	if rc.ZkHosts == nil || len(rc.ZkHosts) == 0 {
		return nil, nil
	}

	if rc.ClusterPrefix == "" {
		return nil, fmt.Errorf("can't get cluster_prefix from table %s json args", table)
	}

	opts := &base.StoreBaseOption{
		Url:     strings.Join(rc.ZkHosts, ","),
		Service: service,
		Table:   table,
		Args:    rc.ClusterPrefix,
	}
	return opts, nil
}
