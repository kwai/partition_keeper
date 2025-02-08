package pb

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"gotest.tools/assert"
)

func TestJsonEncode(t *testing.T) {
	entry := &RouteEntries{}
	entry.TableInfo = &TableInfo{
		HashMethod:   "crc32",
		SplitVersion: 0,
	}

	part := &PartitionLocation{}
	part.Replicas = append(part.Replicas, &ReplicaLocation{
		ServerIndex: 0,
		Role:        ReplicaRole_kPrimary,
		Info:        map[string]string{"a": "b"},
	})
	part.Version = 0
	part.SplitVersion = 0
	entry.Partitions = append(entry.Partitions, part)

	server := &ServerLocation{
		Host:  "127.0.0.1",
		Port:  0,
		HubId: 0,
		Info:  map[string]string{"x": "y"},
		Ip:    "127.0.0.1",
		Alive: true,
		Op:    AdminNodeOp_kNoop,
	}
	entry.Servers = append(entry.Servers, server)

	entry.ReplicaHubs = append(entry.ReplicaHubs, &ReplicaHub{
		Name: "YZ_1",
		Az:   "YZ",
	})

	data, err := json.Marshal(entry)
	assert.NilError(t, err)
	str := string(data)
	logging.Info(str)
	assert.Equal(t, strings.Count(str, "split_version"), 0)

	entry.TableInfo.SplitVersion = 1
	entry.Partitions[0].SplitVersion = 1
	data, err = json.Marshal(entry)
	assert.NilError(t, err)
	str = string(data)
	logging.Info(str)
	assert.Equal(t, strings.Count(str, "split_version"), 2)
}
