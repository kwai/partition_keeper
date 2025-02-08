package embedding_server

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"gotest.tools/assert"
)

func TestEmbeddingValidateCreateTableArgs(t *testing.T) {
	st := Create()

	table := &pb.Table{
		JsonArgs: "not a json",
	}

	err := st.ValidateCreateTableArgs(table)
	assert.Assert(t, err != nil)

	// no btq_prefix
	table.JsonArgs = `
	{
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_num_keys": 10995116277760,
		"table_size_bytes": 10995116277760,
		"table_inflation_ratio": 1.5
	}
	`
	err = st.ValidateCreateTableArgs(table)
	assert.Assert(t, err != nil)

	// no table_num_keys
	table.JsonArgs = `
	{
		"btq_prefix": "mio_embedding",
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_size_bytes": 10995116277760,
		"table_inflation_ratio": 1.5
	}
	`
	err = st.ValidateCreateTableArgs(table)
	assert.Assert(t, err != nil)

	// invalid table_num_keys
	table.JsonArgs = `
	{
		"btq_prefix": "mio_embedding",
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_size_bytes": 10995116277760,
		"table_num_keys": -1,
		"table_inflation_ratio": 1.5
	}
	`
	err = st.ValidateCreateTableArgs(table)
	assert.Assert(t, err != nil)

	// no table_size_bytes and table_size_gibibytes
	table.JsonArgs = `
	{
		"btq_prefix": "mio_embedding",
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_num_keys": 10995116277760,
		"table_inflation_ratio": 1.5
	}
	`
	err = st.ValidateCreateTableArgs(table)
	assert.Assert(t, err != nil)

	// invalid table_size_bytes
	table.JsonArgs = `
	{
		"btq_prefix": "mio_embedding",
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_num_keys": 10995116277760,
		"table_size_bytes": -1,
		"table_inflation_ratio": 1.5
	}
	`
	err = st.ValidateCreateTableArgs(table)
	assert.Assert(t, err != nil)

	// invalid table_size_gibibytes
	table.JsonArgs = `
	{
		"btq_prefix": "mio_embedding",
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_num_keys": 10995116277760,
		"table_size_gibibytes": -1,
		"table_inflation_ratio": 1.5
	}
	`
	err = st.ValidateCreateTableArgs(table)
	assert.Assert(t, err != nil)

	// no table_inflation_ratio
	table.JsonArgs = `
	{
		"btq_prefix": "mio_embedding",
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_num_keys": 10995116277760,
		"table_size_bytes": 10995116277760
	}
	`
	err = st.ValidateCreateTableArgs(table)
	assert.Assert(t, err != nil)

	// invalid table_inflation_ratio
	table.JsonArgs = `
	{
		"btq_prefix": "mio_embedding",
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_num_keys": 10995116277760,
		"table_size_bytes": 10995116277760,
		"table_inflation_ratio": 0.9
	}
	`
	err = st.ValidateCreateTableArgs(table)
	assert.Assert(t, err != nil)

	// valid
	table.JsonArgs = `
	{
		"btq_prefix": "mio_embedding",
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_num_keys": 10995116277760,
		"table_size_bytes": 10995116277760,
		"table_inflation_ratio": 1.5
	}
	`
	err = st.ValidateCreateTableArgs(table)
	assert.Assert(t, err == nil)

	// valid
	table.JsonArgs = `
	{
		"btq_prefix": "mio_embedding",
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_num_keys": 10995116277760,
		"table_size_gibibytes": 1024,
		"table_inflation_ratio": 1.5
	}
	`
	err = st.ValidateCreateTableArgs(table)
	assert.Assert(t, err == nil)
}

func TestEmbeddingGetResources(t *testing.T) {
	st := Create()

	// get table resource
	args := `
	{
		"btq_prefix": "mio_embedding",
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_num_keys": 10995116277760,
		"table_size_bytes": 10995116277760,
		"table_inflation_ratio": 1.5
	}
	`
	hu := st.GetTableResource(args)
	assert.Equal(t, len(hu), 1)
	assert.Equal(t, hu[utils.MEM_CAP], int64(10995116277760))

	// table_size_gibibytes first
	args = `
	{
		"btq_prefix": "mio_embedding",
		"read_slots": "",
		"sign_format": "",
		"min_embeddings_required_per_part": 1099511627776,
		"table_num_keys": 10995116277760,
		"table_size_bytes": 10995116277760,
		"table_size_gibibytes": 100,
		"table_inflation_ratio": 1.5
	}
	`
	hu = st.GetTableResource(args)
	assert.Equal(t, len(hu), 1)
	assert.Equal(t, hu[utils.MEM_CAP], int64(107374182400))

	// get server resource
	hu, err := st.GetServerResource(nil, "node1")
	assert.Assert(t, err != nil)
	assert.Assert(t, hu == nil)

	hu, err = st.GetServerResource(
		map[string]string{"instance_storage_limit_bytes": "not a integer"},
		"node1",
	)
	assert.Assert(t, err != nil)
	assert.Assert(t, hu == nil)

	hu, err = st.GetServerResource(map[string]string{
		"instance_storage_limit_bytes": "10995116277760",
	}, "node1")
	assert.Assert(t, err == nil)
	assert.Equal(t, hu[utils.MEM_CAP], int64(10995116277760))

	// get replica resource
	pr := &pb.PartitionReplica{
		Role:           pb.ReplicaRole_kLearner,
		Node:           utils.FromHostPort("127.0.0.1:1001").ToPb(),
		NodeUniqueId:   "node1",
		HubName:        "hub1",
		ReadyToPromote: false,
		StatisticsInfo: nil,
	}

	hu, err = st.GetReplicaResource(pr)
	assert.Assert(t, err != nil)
	assert.Assert(t, hu == nil)

	pr.StatisticsInfo = map[string]string{}
	hu, err = st.GetReplicaResource(pr)
	assert.Assert(t, err != nil)
	assert.Assert(t, hu == nil)

	pr.StatisticsInfo = map[string]string{
		"db_storage_limit_bytes": "not a integer",
	}
	hu, err = st.GetReplicaResource(pr)
	assert.Assert(t, err != nil)
	assert.Assert(t, hu == nil)

	pr.StatisticsInfo = map[string]string{
		"db_storage_limit_bytes": "10995116277760",
	}
	hu, err = st.GetReplicaResource(pr)
	assert.Assert(t, err == nil)
	assert.Equal(t, hu[utils.MEM_CAP], int64(10995116277760))
}
