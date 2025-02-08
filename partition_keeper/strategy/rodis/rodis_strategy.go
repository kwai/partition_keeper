package rodis

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"sync"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route/rodis_store"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/strategy/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

var (
	flagRodisSecondaryMaxLag = flag.Int64(
		"rodis_secondary_max_lag",
		1000,
		"max lag for a rodis secondary fall behind primary",
	)
)

type DummyBtqPrefix struct {
	BtqPrefix string `json:"btq_prefix"`
}

type RodisStrategy struct {
}

func (r *RodisStrategy) GetType() pb.ServiceType {
	return pb.ServiceType_colossusdb_rodis
}

func (r *RodisStrategy) CreateCheckpointByDefault() bool {
	return false
}

func (r *RodisStrategy) SupportSplit() bool {
	return true
}

func (e *RodisStrategy) CheckSwitchPrimary(
	tpid utils.TblPartID,
	from *pb.PartitionReplica,
	to *pb.PartitionReplica,
) bool {
	return true
}

func (e *RodisStrategy) NotifySwitchPrimary(
	tpid utils.TblPartID,
	wg *sync.WaitGroup,
	address *utils.RpcNode,
	nodesConn *rpc.ConnPool,
) {
}

func (r *RodisStrategy) findPrimary(others []*pb.PartitionReplica) *pb.PartitionReplica {
	for _, peer := range others {
		if peer.Role == pb.ReplicaRole_kPrimary {
			return peer
		}
	}
	return nil
}

func (r *RodisStrategy) lagNearPrimary(
	tpid utils.TblPartID,
	node string,
	seq, priSeq int64,
) bool {
	logging.Verbose(1, "%s peer %s has sequence id %d vs primary's %d",
		tpid.String(),
		node,
		seq,
		priSeq,
	)
	return seq+*flagRodisSecondaryMaxLag > priSeq
}

func (r *RodisStrategy) checkLearner(
	tpid utils.TblPartID,
	self *pb.PartitionReplica,
	others []*pb.PartitionReplica,
	opts *base.TableRunningOptions,
) pb.ReplicaRole {
	if base.StdTransformLearner(self) == pb.ReplicaRole_kLearner {
		return pb.ReplicaRole_kLearner
	}
	selfSeq := rodis_store.GetSequenceNumber(self.StatisticsInfo)
	if selfSeq == -1 {
		return pb.ReplicaRole_kLearner
	}

	var primary *pb.PartitionReplica = r.findPrimary(others)
	if primary == nil {
		if opts.IsRestoring {
			logging.Info(
				"%s learner %s seq:%d allow to promote to secondary as in restore stage",
				tpid.String(),
				self.NodeUniqueId,
				selfSeq,
			)
			return pb.ReplicaRole_kSecondary
		}
		larger, noLarger := 0, 0
		for _, peer := range others {
			if peer.NodeUniqueId == self.NodeUniqueId {
				continue
			}
			peerSeq := rodis_store.GetSequenceNumber(peer.StatisticsInfo)
			if peerSeq == -1 {
				return pb.ReplicaRole_kLearner
			}
			if peerSeq > selfSeq {
				larger++
			} else {
				noLarger++
			}
		}
		if larger > noLarger {
			logging.Info(
				"%s reject to promote learner %s seq:%d as %d seq larger than it, while %d not larger",
				tpid.String(),
				self.NodeUniqueId,
				selfSeq,
				larger,
				noLarger,
			)
			return pb.ReplicaRole_kLearner
		} else {
			return pb.ReplicaRole_kSecondary
		}
	}

	primarySeq := rodis_store.GetSequenceNumber(primary.StatisticsInfo)

	if selfSeq == -1 || primarySeq == -1 {
		return pb.ReplicaRole_kLearner
	}
	if r.lagNearPrimary(tpid, self.Node.ToHostPort(), selfSeq, primarySeq) {
		return pb.ReplicaRole_kSecondary
	}
	return pb.ReplicaRole_kLearner
}

func (r *RodisStrategy) checkSecondary(
	tpid utils.TblPartID,
	self *pb.PartitionReplica,
	others []*pb.PartitionReplica,
) pb.ReplicaRole {
	primary := r.findPrimary(others)
	if primary == nil {
		return pb.ReplicaRole_kSecondary
	}
	selfSeq := rodis_store.GetSequenceNumber(self.StatisticsInfo)
	primarySeq := rodis_store.GetSequenceNumber(primary.StatisticsInfo)

	if selfSeq == -1 || primarySeq == -1 {
		return pb.ReplicaRole_kSecondary
	}
	if r.lagNearPrimary(tpid, self.Node.ToHostPort(), selfSeq, primarySeq) {
		return pb.ReplicaRole_kSecondary
	}
	return pb.ReplicaRole_kLearner
}

func (r *RodisStrategy) checkPrimary(
	tpid utils.TblPartID,
	self *pb.PartitionReplica,
	others []*pb.PartitionReplica,
) pb.ReplicaRole {
	// TODO(huyifan03): rodis may report if disk has error,
	// in which case we must remove the primary and select a new node
	return pb.ReplicaRole_kPrimary
}

func (r *RodisStrategy) independentReplicaTransform(
	tpid utils.TblPartID,
	self *pb.PartitionReplica,
	others []*pb.PartitionReplica,
	opts *base.TableRunningOptions,
) pb.ReplicaRole {
	switch self.Role {
	case pb.ReplicaRole_kLearner:
		return r.checkLearner(tpid, self, others, opts)
	case pb.ReplicaRole_kSecondary:
		return r.checkSecondary(tpid, self, others)
	case pb.ReplicaRole_kPrimary:
		return r.checkPrimary(tpid, self, others)
	default:
		logging.Fatal("")
		return pb.ReplicaRole_kInvalid
	}
}

func (r *RodisStrategy) checkChildLeaner(
	tpid utils.TblPartID,
	self *pb.PartitionReplica,
	parents []*pb.PartitionReplica,
	opts *base.TableRunningOptions,
) pb.ReplicaRole {
	if base.StdTransformLearner(self) == pb.ReplicaRole_kLearner {
		return pb.ReplicaRole_kLearner
	}
	selfSeq := rodis_store.GetSequenceNumber(self.StatisticsInfo)
	if selfSeq == -1 {
		return pb.ReplicaRole_kLearner
	}

	primary := r.findPrimary(parents)
	if primary == nil {
		return pb.ReplicaRole_kLearner
	}
	primarySeq := rodis_store.GetSequenceNumber(primary.StatisticsInfo)
	if primarySeq == -1 {
		return pb.ReplicaRole_kLearner
	}
	if r.lagNearPrimary(tpid, self.Node.ToHostPort(), selfSeq, primarySeq) {
		return pb.ReplicaRole_kSecondary
	}
	return pb.ReplicaRole_kLearner
}

func (r *RodisStrategy) childReplicaTransform(
	tpid utils.TblPartID,
	self *pb.PartitionReplica,
	parents []*pb.PartitionReplica,
	opts *base.TableRunningOptions,
) pb.ReplicaRole {
	switch self.Role {
	case pb.ReplicaRole_kLearner:
		return r.checkChildLeaner(tpid, self, parents, opts)
	case pb.ReplicaRole_kSecondary:
		return r.checkSecondary(tpid, self, parents)
	default:
		logging.Fatal("")
		return self.Role
	}
}

func (r *RodisStrategy) Transform(
	tpid utils.TblPartID,
	self *pb.PartitionReplica,
	siblings []*pb.PartitionReplica,
	parents []*pb.PartitionReplica,
	opts *base.TableRunningOptions,
) pb.ReplicaRole {
	if parents == nil {
		return r.independentReplicaTransform(tpid, self, siblings, opts)
	} else {
		return r.childReplicaTransform(tpid, self, parents, opts)
	}
}

func (r *RodisStrategy) ValidateCreateTableArgs(tbl *pb.Table) error {
	args := &DummyBtqPrefix{}
	if err := json.Unmarshal([]byte(tbl.JsonArgs), args); err != nil {
		return err
	}
	if args.BtqPrefix != "dummy" {
		return errors.New("please specify dummy btq_prefix for rodis table")
	}
	return nil
}

func (r *RodisStrategy) GetSchedulerInfo(tbl *pb.Table) (string, int) {
	return sched.ADAPTIVE_SCHEDULER, 0
}

func (r *RodisStrategy) MakeStoreOpts(
	regions map[string]bool, service, table, jsonArgs string,
) ([]*route.StoreOption, error) {
	output := []*route.StoreOption{}

	{
		// zk route
		opt, err := rodis_store.MakeOptions(service, table, jsonArgs)
		if opt == nil && err == nil {
			logging.Info("%s:%s disable publish zk", service, table)
		} else {
			if err != nil {
				return nil, err
			} else {
				storeOpt := &route.StoreOption{
					Media:           rodis_store.MediaType,
					StoreBaseOption: *opt,
				}
				output = append(output, storeOpt)
			}
		}
	}

	return output, nil
}

func (r *RodisStrategy) GetServerResource(
	info map[string]string,
	nodeId string,
) (utils.HardwareUnit, error) {
	if info == nil {
		return nil, fmt.Errorf("%s don't have statistics info", nodeId)
	}
	dbcap_key := pb.RodisServerStat_dbcap_bytes.String()
	dbcpu_key := pb.RodisServerStat_dbcpu_cores.String()
	ksat_cpu := pb.StdStat_cpu_percent.String()

	all_keys := []string{dbcap_key, dbcpu_key, ksat_cpu}
	output := utils.HardwareUnit{}

	for _, key := range all_keys {
		value_string := info[key]
		if value_string == "" {
			continue
		}
		val, err := strconv.ParseInt(value_string, 10, 64)
		if err != nil {
			logging.Error("%s's %s is not int64: %s", nodeId, value_string, err)
			continue
		}

		if key == dbcap_key {
			output[utils.DISK_CAP] = val
		} else if key == dbcpu_key {
			output[utils.CPU] = val
		} else if key == ksat_cpu {
			output[utils.KSAT_CPU] = val
		}
	}

	if len(output) == 0 {
		return nil, fmt.Errorf("%s don't have statistics key", nodeId)
	}
	return output, nil
}

func (r *RodisStrategy) GetTableResource(jsonArgs string) utils.HardwareUnit {
	return nil
}

func (r *RodisStrategy) GetReplicaResource(pr *pb.PartitionReplica) (utils.HardwareUnit, error) {
	return nil, nil
}

func (r *RodisStrategy) DisallowServiceAccessOnlyNearestNodes(
	ksn string,
	serviceName string,
	tableName string,
	regions map[string]bool,
) {
}

func Create() base.StrategyBase {
	return &RodisStrategy{}
}
