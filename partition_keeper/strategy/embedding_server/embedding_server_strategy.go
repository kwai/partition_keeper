package embedding_server

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type embeddingJsonArgs struct {
	BtqPrefix                    string  `json:"btq_prefix"`
	ReadSlots                    string  `json:"read_slots"`
	SignFormat                   string  `json:"sign_format"`
	MinEmbeddingsRequiredPerPart int64   `json:"min_embeddings_required_per_part"`
	TableNumKeys                 int64   `json:"table_num_keys"`
	TableSizeBytes               int64   `json:"table_size_bytes"`
	TableSizeGibibytes           int64   `json:"table_size_gibibytes"`
	TableInflationRatio          float64 `json:"table_inflation_ratio"`
}

type EmbeddingServerStrategy struct {
	base.DummyStrategy
}

func (e *EmbeddingServerStrategy) GetType() pb.ServiceType {
	return pb.ServiceType_colossusdb_embedding_server
}

func (e *EmbeddingServerStrategy) CreateCheckpointByDefault() bool {
	return true
}

func (e *EmbeddingServerStrategy) SupportSplit() bool {
	return false
}

func (r *EmbeddingServerStrategy) ValidateCreateTableArgs(table *pb.Table) error {
	if err := base.ValidateMsgQueueShards(int(table.PartsCount), table.JsonArgs); err != nil {
		return err
	}
	args := embeddingJsonArgs{}
	err := json.Unmarshal([]byte(table.JsonArgs), &args)
	if err != nil {
		return err
	}
	if args.BtqPrefix == "" {
		return fmt.Errorf("btq_prefix is required")
	}
	if args.TableNumKeys <= 0 {
		return fmt.Errorf("table_num_keys is invalid")
	}
	if args.TableSizeBytes <= 0 && args.TableSizeGibibytes <= 0 {
		return fmt.Errorf("table_size_bytes and table_size_gibibytes are invalid")
	}
	if args.TableInflationRatio < 1.0 {
		return fmt.Errorf("table_inflation_ratio is invalid")
	}
	return nil
}

func (r *EmbeddingServerStrategy) GetTableResource(jsonArgs string) utils.HardwareUnit {
	args := embeddingJsonArgs{}
	// if config changed, we'd better fix the old ones manually
	utils.UnmarshalJsonOrDie([]byte(jsonArgs), &args)
	output := utils.HardwareUnit{}
	if args.TableSizeGibibytes > 0 {
		output[utils.MEM_CAP] = args.TableSizeGibibytes * 1024 * 1024 * 1024
	} else {
		output[utils.MEM_CAP] = args.TableSizeBytes
	}
	return output
}

func (r *EmbeddingServerStrategy) GetServerResource(
	info map[string]string, nodeId string,
) (utils.HardwareUnit, error) {
	key := pb.EmbdServerStat_name[int32(pb.EmbdServerStat_instance_storage_limit_bytes)]
	if info == nil {
		return nil, fmt.Errorf("%s don't have statistics key: %s", nodeId, key)
	}
	memBytes := info[key]
	if memBytes == "" {
		return nil, fmt.Errorf("%s don't have statistics key: %s", nodeId, key)
	}
	val, err := strconv.ParseInt(memBytes, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s's %s is not int64: %w", nodeId, memBytes, err)
	}
	output := utils.HardwareUnit{}
	output[utils.MEM_CAP] = val
	return output, nil
}

func (r *EmbeddingServerStrategy) GetReplicaResource(
	pr *pb.PartitionReplica,
) (utils.HardwareUnit, error) {
	key := pb.EmbdReplicaStat_name[int32(pb.EmbdReplicaStat_db_storage_limit_bytes)]
	if pr.StatisticsInfo == nil {
		return nil, fmt.Errorf("can't find key %s", key)
	}
	memBytes := pr.StatisticsInfo[key]
	if memBytes == "" {
		return nil, fmt.Errorf("can't find key %s", key)
	}
	val, err := strconv.ParseInt(memBytes, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s is not int64: %w", memBytes, err)
	}
	output := utils.HardwareUnit{}
	output[utils.MEM_CAP] = val
	return output, nil
}

func (r *EmbeddingServerStrategy) DisallowServiceAccessOnlyNearestNodes(
	ksn string,
	serviceName string,
	tableName string,
	regions map[string]bool,
) {
	third_party.DisallowServiceAccessOnlyNearestNodes(ksn, serviceName, tableName, regions)
}

func Create() base.StrategyBase {
	return &EmbeddingServerStrategy{}
}
