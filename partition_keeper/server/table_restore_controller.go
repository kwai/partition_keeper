package server

import (
	"context"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/sched/actions"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"google.golang.org/protobuf/proto"
)

type RestoreStage string

const (
	RestorePending  RestoreStage = "pending"
	RestoreRunning  RestoreStage = "running"
	RestoreFinished RestoreStage = "finished"
)

type RestoreStableInfos struct {
	*pb.RestoreOpts
	RestorePath string       `json:"restore_path"`
	Stage       RestoreStage `json:"stage"`
}

type perHubProgress struct {
	pendingCount int
	runningCount int
	finishCount  int
	partStage    map[int]RestoreStage
	runningNodes map[string]int
}

func createPerHubProgress() *perHubProgress {
	return &perHubProgress{
		pendingCount: 0,
		runningCount: 0,
		finishCount:  0,
		partStage:    make(map[int]RestoreStage),
		runningNodes: make(map[string]int),
	}
}

func (p *perHubProgress) summary(logName string) {
	p.pendingCount = 0
	p.runningCount = 0
	p.finishCount = 0

	for _, stage := range p.partStage {
		switch stage {
		case RestorePending:
			p.pendingCount++
		case RestoreRunning:
			p.runningCount++
		case RestoreFinished:
			p.finishCount++
		default:
			logging.Fatal("unreachable")
		}
	}
	logging.Info(
		"%s: restore progress, pending(%d), running(%d), runningNodes(%d), finished(%d)",
		logName,
		p.pendingCount,
		p.runningCount,
		len(p.runningNodes),
		p.finishCount,
	)
}

type TableRestoreController struct {
	logName  string
	zkPath   string
	table    *TableStats
	stats    *RestoreStableInfos
	progress map[string]*perHubProgress
	nextHub  string
}

func SerializeRestoreStableInfo(req *pb.RestoreTableRequest) []byte {
	infos := &RestoreStableInfos{
		RestoreOpts: proto.Clone(req.Opts).(*pb.RestoreOpts),
		RestorePath: req.RestorePath,
		Stage:       RestorePending,
	}
	return utils.MarshalJsonOrDie(infos)
}

func NewTableRestoreController(table *TableStats, zkPath string) *TableRestoreController {
	ans := &TableRestoreController{
		logName:  table.TableName,
		zkPath:   zkPath,
		table:    table,
		stats:    nil,
		progress: nil,
	}
	return ans
}

func (r *TableRestoreController) InitializeNew(req *pb.RestoreTableRequest) {
	succ := r.table.zkConn.Create(context.Background(), r.zkPath, SerializeRestoreStableInfo(req))
	logging.Assert(succ, "")
	succ = r.LoadFromZookeeper()
	logging.Assert(succ, "")
}

func (r *TableRestoreController) LoadFromZookeeper() bool {
	restoreData, exists, succ := r.table.zkConn.Get(context.Background(), r.zkPath)
	logging.Assert(succ, "")
	if exists {
		restoreInfo := &RestoreStableInfos{}
		utils.UnmarshalJsonOrDie(restoreData, restoreInfo)
		r.stats = restoreInfo
		logging.Info("%s: load restore info: %v", r.logName, restoreInfo)
		return true
	} else {
		logging.Info("%s: can't find restore info", r.logName)
		return false
	}
}

func (r *TableRestoreController) Update(req *pb.RestoreTableRequest) {
	if r.stats.RestorePath != req.RestorePath {
		logging.Info(
			"%s restore path %s -> %s, must reset restore stage",
			r.logName,
			r.stats.RestorePath,
			req.RestorePath,
		)
		r.stats.RestorePath = req.RestorePath
		r.stats.Stage = RestorePending
	}
	r.stats.RestoreOpts = proto.Clone(req.Opts).(*pb.RestoreOpts)
	data := utils.MarshalJsonOrDie(r.stats)
	succ := r.table.zkConn.Set(context.Background(), r.zkPath, data)
	logging.Assert(succ, "")
}

func (r *TableRestoreController) GenerateRestorePlan() (map[int32]*actions.PartitionActions, bool) {
	if r.stats.Stage == RestorePending {
		return nil, false
	}
	if r.stats.Stage == RestoreFinished {
		return nil, true
	}

	if !r.primariesDowngraded() {
		return nil, false
	}
	if !r.refreshRestoreProgress() {
		return nil, false
	}
	if r.summaryHubs() {
		r.markFinished()
		return nil, true
	} else {
		return r.makePlan(), false
	}
}

func (r *TableRestoreController) StartRunning() {
	logging.Assert(r.stats.Stage == RestorePending, "")
	r.stats.Stage = RestoreRunning
	data := utils.MarshalJsonOrDie(r.stats)
	succ := r.table.zkConn.Set(context.Background(), r.zkPath, data)
	logging.Assert(succ, "")
}

func (r *TableRestoreController) markFinished() {
	logging.Assert(r.stats.Stage == RestoreRunning, "")
	r.stats.Stage = RestoreFinished
	data := utils.MarshalJsonOrDie(r.stats)
	succ := r.table.zkConn.Set(context.Background(), r.zkPath, data)
	logging.Assert(succ, "")
}

func (r *TableRestoreController) primariesDowngraded() bool {
	for i := range r.table.currParts {
		p := &(r.table.currParts[i])
		for node, role := range p.members.Peers {
			if role == pb.ReplicaRole_kPrimary {
				logging.Info(
					"%s t%d.p%d still have primary %s, reset",
					r.logName,
					r.table.TableId,
					i,
					node,
				)
				r.reset()
				return false
			}
		}
	}
	return true
}

func (r *TableRestoreController) getHubProgress(hubname string) *perHubProgress {
	logging.Assert(r.progress != nil, "")
	if hp, ok := r.progress[hubname]; ok {
		return hp
	} else {
		hp = createPerHubProgress()
		r.progress[hubname] = hp
		return hp
	}
}

func (r *TableRestoreController) refreshPartProgress(part *partition, hubname string, node string) {
	hp := r.getHubProgress(hubname)
	currentVersion := part.members.RestoreVersion[node]
	logging.Assert(currentVersion <= r.table.RestoreVersion, "")
	if currentVersion < r.table.RestoreVersion {
		hp.partStage[int(part.partId)] = RestorePending
		return
	}

	for i := 0; i < 1; i++ {
		fact := part.getReplicaFact(node)
		if fact == nil {
			hp.partStage[int(part.partId)] = RestoreRunning
			break
		}

		if fact.RestoreVersion < currentVersion {
			hp.partStage[int(part.partId)] = RestoreRunning
			break
		}

		if part.members.GetMember(node) == pb.ReplicaRole_kSecondary {
			hp.partStage[int(part.partId)] = RestoreFinished
		} else {
			hp.partStage[int(part.partId)] = RestoreRunning
		}
	}

	if hp.partStage[int(part.partId)] == RestoreRunning {
		if count, ok := hp.runningNodes[node]; ok {
			count++
		} else {
			hp.runningNodes[node] = 1
		}
	}
}

func (r *TableRestoreController) refreshRestoreProgress() bool {
	r.progress = make(map[string]*perHubProgress)
	for i := range r.table.currParts {
		p := &(r.table.currParts[i])
		inhub, outofhub := p.members.DivideByHubs(r.table.nodes, r.table.hubs)
		if len(outofhub) > 0 {
			logging.Info(
				"%s t%d.p%d hasn't clean extra hubs, reset",
				r.logName,
				r.table.TableId,
				i,
			)
			r.reset()
			return false
		}
		for _, hub := range r.table.hubs {
			nodesOnHub := inhub[hub.Name]
			if len(nodesOnHub) != 1 {
				logging.Info(
					"%s t%d.p%d hub %s has %d nodes, reset",
					r.logName,
					r.table.TableId,
					i,
					hub.Name,
					len(nodesOnHub),
				)
				r.reset()
				return false
			} else {
				r.refreshPartProgress(p, hub.Name, nodesOnHub[0])
			}
		}
	}
	return true
}

func (r *TableRestoreController) summaryHubs() bool {
	r.nextHub = ""
	for _, hub := range r.table.hubs {
		hp := r.getHubProgress(hub.Name)
		hp.summary(r.logName)
		if hp.finishCount < int(r.table.PartsCount) && r.nextHub == "" {
			r.nextHub = hub.Name
		}
	}
	return r.nextHub == ""
}

func (r *TableRestoreController) makePlan() map[int32]*actions.PartitionActions {
	hp := r.getHubProgress(r.nextHub)

	output := make(map[int32]*actions.PartitionActions)
	for i := range r.table.currParts {
		part := &(r.table.currParts[i])
		for node := range part.members.Peers {
			info := r.table.nodes.MustGetNodeInfo(node)
			if info.Hub == r.nextHub && hp.partStage[i] == RestorePending {
				if r.stats.MaxConcurrentNodesPerHub != -1 &&
					len(hp.runningNodes) >= int(r.stats.MaxConcurrentNodesPerHub) {
					if _, ok := hp.runningNodes[node]; !ok {
						continue
					}
				}

				logging.Info(
					"%s plan to recreate t%d.p%d at node %s",
					r.logName,
					r.table.TableId,
					i,
					info.LogStr(),
				)
				act := &actions.PartitionActions{}
				act.AddAction(&actions.RemoveNodeAction{Node: node})
				act.AddAction(&actions.AddLearnerAction{Node: node})
				act.AddAction(&actions.PromoteToSecondaryAction{Node: node})
				output[int32(i)] = act
				hp.runningCount++
				if count, ok := hp.runningNodes[node]; ok {
					count++
				} else {
					hp.runningNodes[node] = 1
				}

			}
		}
	}
	return output
}

func (d *TableRestoreController) reset() {
	d.progress = nil
}
