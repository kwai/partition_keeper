package node_mgr

import (
	"flag"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/sd"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

var (
	flagPollKessIntervalSecs = flag.Int64(
		"poll_kess_interval_secs",
		5,
		"poll_kess_interval_secs",
	)
	flagDisablePollerAlertMinutes = flag.Int64(
		"disable_poller_alert_minutes",
		30,
		"disable_poller_alert_minutes",
	)
	flagAllowEmptyAz       = flag.Bool("allow_empty_az", true, "allow_empty_az")
	flagAllowConflictIndex = flag.Bool("allow_conflict_index", false, "allow_conflict_index")
	flagDiscoveryZk        = flag.String("service_discovery_zk", "", "service_discovery_zk")
)

type NodePing struct {
	IsAlive   bool           `json:"alive"`
	Az        string         `json:"az"`
	Address   *utils.RpcNode `json:"address"`
	ProcessId string         `json:"process_id"`
	BizPort   int32          `json:"biz_port"`
	// if node report it's index with format "a.b"
	// then partition-keeper will assign it to hub "${Az}_a"
	// and its instance index is b with this hub.
	//
	// if two nodes have same instance index,
	// the latter will replace the former
	//
	// this feature is mainly used for cloud platform,
	// where hub and instance id are statically allocated
	// by the platform.
	// it's better for nodes deployed on physical machine
	// to keep this variable empty.
	NodeIndex string `json:"node_index"`
}

func (np *NodePing) ExtractNodeIndex() (string, int) {
	hub, sub, err := utils.ParseNodeIndex(np.NodeIndex)
	if err != nil {
		return "", -1
	}
	return fmt.Sprintf("%s_%d", np.Az, hub), sub
}

func (np *NodePing) Clone() *NodePing {
	result := *np
	result.Address = np.Address.Clone()
	return &result
}

// TEST only usage
func (np *NodePing) ToServiceNode(nodeId string) *pb.ServiceNode {
	pid, _ := strconv.ParseInt(np.ProcessId, 10, 64)
	payload := &utils.PartitionServicePayload{
		NodeId:    nodeId,
		ProcessId: pid,
		BizPort:   np.BizPort,
		NodeIndex: np.NodeIndex,
	}

	location := ""
	paz := ""
	if utils.PazNameValid(np.Az) {
		paz = np.Az
	} else {
		location = "BJ." + np.Az
	}
	if np.Az == "STAGING" {
		paz = "STAGING"
		location = "BJ." + "STAGING"
	}

	node := &pb.ServiceNode{
		Id:       fmt.Sprintf("grpc:%s", np.Address.String()),
		Protocol: "grpc",
		Host:     np.Address.NodeName,
		Port:     int32(np.Address.Port),
		Payload:  string(utils.MarshalJsonOrDie(payload)),
		Weight:   1,
		Shard:    "0",
		// TODO: support more az to location
		Location:        location,
		InitialLiveTime: 1,
		Paz:             paz,
	}
	return node
}

type KessBasedNodeDetector struct {
	mu                   sync.Mutex
	serviceName          string
	staticIndexed        bool
	usePaz               bool
	sd                   sd.ServiceDiscovery
	hubs                 *utils.HubHandle
	azs                  map[string]bool
	activeList           map[string]*NodePing
	pollKessIntervalSecs int64
	enablePoller         bool
	disablePollerTime    int64
	dispatcher           chan map[string]*NodePing
	trigger              chan bool
	quit                 chan bool
}

type kessOpts func(d *KessBasedNodeDetector)

func WithPollKessIntervalSecs(i int64) kessOpts {
	return func(d *KessBasedNodeDetector) {
		d.pollKessIntervalSecs = i
	}
}

func WithServiceDiscovery(sd sd.ServiceDiscovery) kessOpts {
	return func(d *KessBasedNodeDetector) {
		d.sd = sd
	}
}

func NewNodeDetector(serviceName string, opts ...kessOpts) *KessBasedNodeDetector {
	DcBlackListInitialize()
	ans := &KessBasedNodeDetector{
		serviceName:          serviceName,
		pollKessIntervalSecs: *flagPollKessIntervalSecs,
		enablePoller:         false,
		disablePollerTime:    time.Now().Unix(),
		dispatcher:           make(chan map[string]*NodePing, 100),
		trigger:              make(chan bool),
		quit:                 make(chan bool),
	}
	for _, opt := range opts {
		opt(ans)
	}
	return ans
}

func (k *KessBasedNodeDetector) Start(
	initList map[string]*NodePing,
	staticIndexed bool,
	enablePoller bool,
	usePaz bool,
) <-chan map[string]*NodePing {
	k.activeList = make(map[string]*NodePing)
	for id, ping := range initList {
		if ping.IsAlive {
			k.activeList[id] = ping.Clone()
		}
	}
	k.staticIndexed = staticIndexed
	k.enablePoller = enablePoller
	if !enablePoller {
		k.disablePollerTime = time.Now().Unix()
	}
	k.usePaz = usePaz
	if k.sd == nil {
		k.sd = sd.NewServiceDiscovery(
			sd.SdTypeZK,
			k.serviceName,
			sd.ZkSdSetZKHosts(*flagDiscoveryZk),
		)
	}

	logging.Info(
		"%s: start node detection, initial nodes count: %d, enable: %v",
		k.serviceName,
		len(initList),
		k.enablePoller,
	)
	go k.pollKess()
	return k.dispatcher
}

func (k *KessBasedNodeDetector) Stop() {
	logging.Info("%s: notify detector to quit", k.serviceName)
	k.quit <- true
	logging.Info("%s: finish notifying detector to quit", k.serviceName)
	if k.sd != nil {
		k.sd.Stop()
	}
}

func (k *KessBasedNodeDetector) UpdateHubs(hubs []*pb.ReplicaHub) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	handle := utils.MapHubs(hubs)
	if k.sd == nil {
		return fmt.Errorf("%s detector is not started, do not use", k.serviceName)
	}
	if err := k.sd.UpdateAzs(handle.GetAzs()); err != nil {
		return err
	}
	k.hubs = handle
	k.azs = k.hubs.GetAzs()
	return nil
}

func (k *KessBasedNodeDetector) GetHubs() []*pb.ReplicaHub {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.hubs == nil {
		return nil
	} else {
		return k.hubs.ListHubs()
	}
}

func (k *KessBasedNodeDetector) EnablePoller(flag bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.enablePoller = flag
	if !flag {
		k.disablePollerTime = time.Now().Unix()
	}
}

func (k *KessBasedNodeDetector) pollerEnabled() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.enablePoller
}

func (k *KessBasedNodeDetector) TriggerDetection() {
	k.trigger <- true
}

func (k *KessBasedNodeDetector) detectThisAz(az string) bool {
	k.mu.Lock()
	defer k.mu.Unlock()

	_, ok := k.azs[az]
	return ok
}

func (k *KessBasedNodeDetector) azIsEmpty() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.azs) == 0
}

func (k *KessBasedNodeDetector) addSub(
	output map[string]map[int]string,
	hub string,
	subNo int,
	node string,
) error {
	if hubIdx, ok := output[hub]; ok {
		// for pre indexed service(namely cloud deployed service),
		// same index for two nodes means a instance is schedule from one host to another.
		// we need to replace the former using the latter.
		// so the detection order is very important that we need to judge which one registers to
		// kess earlier.
		//
		// if in a node detection response more than 1 nodes have same index, we'd better skip this
		// round until
		// the redundant ones to disappear.
		if existingNode, ok := hubIdx[subNo]; ok {
			return fmt.Errorf("conflict index %d in %s. %s vs %s", subNo, hub, existingNode, node)
		} else {
			hubIdx[subNo] = node
			return nil
		}
	} else {
		output[hub] = map[int]string{subNo: node}
		return nil
	}
}

func (k *KessBasedNodeDetector) getRefreshedNodeList() map[string]*NodePing {
	if k.sd == nil {
		logging.Error("%s detector is not started, do not use", k.serviceName)
		return nil
	}
	result, err := k.sd.GetNodes()
	if err != nil {
		logging.Info("%s: get service nodes failed: %s", k.serviceName, err.Error())
		return nil
	}
	if len(result) == 0 {
		logging.Info("%s: can't get any service nodes", k.serviceName)
		if k.azIsEmpty() {
			return make(map[string]*NodePing)
		} else {
			return nil
		}
	}

	output := map[string]*NodePing{}
	subPerHub := map[string]map[int]string{}

	dcBlackList := GetDcBlackListConfigs()
	for _, node := range result {
		hostname := sd.GetHostnameByKessId(node.Id)
		logging.Assert(hostname != "", "invalid hostname from %s", node.Id)
		az := ""
		if k.usePaz {
			az = node.Paz
		} else {
			az = sd.KessLocationAz(node.Location)
		}
		logging.Assert(az != "", "invalid az from location %s for %s", node.Location, node.Id)
		if !k.detectThisAz(az) {
			logging.Warning("%s: skip node %s in az %s, location: %s",
				k.serviceName, node.Id, az, node.Location)
			continue
		}
		if node.Kws != nil && dcBlackList.dcDisabled(k.serviceName, node.Kws.Dc) {
			logging.Warning(
				"%s: skip node %s of dc %s as it's in dc blacklist",
				k.serviceName,
				node.Id,
				node.Kws.Dc,
			)
			continue
		}

		payload, err := utils.ExtractPayload(node)
		if err != nil {
			logging.Warning(
				"%s: node %s's payload %v az %s is not valid: %s",
				k.serviceName,
				node.Id,
				node.Payload,
				node.Location,
				err.Error(),
			)
			return nil
		}
		if k.staticIndexed {
			hubNo, subNo, err := utils.ParseNodeIndex(payload.NodeIndex)
			if err != nil {
				logging.Info(
					"%s: node %s's invalid index(%s): %s",
					k.serviceName,
					node.Id,
					payload.NodeIndex,
					err.Error(),
				)
				return nil
			}
			if !*flagAllowConflictIndex {
				hubName := fmt.Sprintf("%s_%d", az, hubNo)
				if err := k.addSub(subPerHub, hubName, subNo, node.Id); err != nil {
					logging.Error("%s: %s", k.serviceName, err.Error())
					return nil
				}
			}
		} else {
			payload.NodeIndex = ""
		}
		if existing, ok := output[payload.NodeId]; ok {
			logging.Warning("%s: node %s and %s has conflict id %s",
				k.serviceName,
				node.Id, existing.Address.String(), payload.NodeId)
			return nil
		}
		output[payload.NodeId] = &NodePing{
			IsAlive: true,
			Az:      az,
			Address: &utils.RpcNode{
				NodeName: hostname,
				Port:     int32(node.Port),
			},
			ProcessId: fmt.Sprintf("%d", payload.ProcessId),
			BizPort:   payload.BizPort,
			NodeIndex: payload.NodeIndex,
		}
	}

	return output
}

func (k *KessBasedNodeDetector) cmpNodeList(newList map[string]*NodePing) {
	changed := map[string]*NodePing{}

	for id, ping := range k.activeList {
		if newPing, ok := newList[id]; !ok {
			logging.Info(
				"%s: can't get node %s addr %s from kess result set, mark it as dead",
				k.serviceName,
				id,
				ping.Address.String(),
			)
			ping.IsAlive = false
			changed[id] = ping
		} else {
			metaChanged := false
			logging.Assert(ping.Az == newPing.Az,
				"%s: node %s az changed from %s to %s",
				k.serviceName, ping.Address, ping.Az, newPing.Az)
			if ping.ProcessId != newPing.ProcessId {
				logging.Info("%s: node %s change process id %s -> %s, treat it dead",
					k.serviceName, id, ping.ProcessId, newPing.ProcessId)
				ping.ProcessId = newPing.ProcessId
				ping.IsAlive = false
				metaChanged = true
			}
			if ping.BizPort != newPing.BizPort {
				logging.Info("%s: node %s change biz port %d -> %d", k.serviceName, id, ping.BizPort, newPing.BizPort)
				ping.BizPort = newPing.BizPort
				metaChanged = true
			}
			if !ping.Address.Equals(newPing.Address) {
				logging.Info("%s: node %s change address %v -> %v", k.serviceName, id, ping.Address, newPing.Address)
				ping.Address = newPing.Address.Clone()
				metaChanged = true
			}
			if metaChanged {
				changed[id] = ping
			}
			if !ping.IsAlive {
				logging.Verbose(1, "%s: node %s marked dead due to process id changed, remove it from new list",
					k.serviceName, id)
				delete(newList, id)
			}
		}
	}

	for id, newPing := range newList {
		if _, ok := k.activeList[id]; !ok {
			logging.Info("%s: detect new node %s, %v", k.serviceName, id, newPing)
			changed[id] = newPing.Clone()
		}
	}

	k.activeList = newList

	if len(changed) > 0 {
		k.dispatcher <- changed
	}
}

func (k *KessBasedNodeDetector) updateNodeList() {
	if !k.pollerEnabled() {
		logging.Info("%s: detector poller disabled, don't do update", k.serviceName)
		if (time.Now().Unix() - k.disablePollerTime) > (*flagDisablePollerAlertMinutes * 60) {
			third_party.PerfLog1(
				"reco",
				"reco.colossusdb.disable_detector_long_time",
				third_party.GetKsnNameByServiceName(k.serviceName),
				k.serviceName,
				1)
		}

		return
	}

	newList := k.getRefreshedNodeList()
	if newList == nil {
		return
	}
	k.cmpNodeList(newList)
}

func (k *KessBasedNodeDetector) pollKess() {
	logging.Info("%s: start poll kess loop", k.serviceName)
	tick := time.NewTicker(time.Second * time.Duration(k.pollKessIntervalSecs))
	for {
		select {
		case <-tick.C:
			k.updateNodeList()
		case <-k.trigger:
			k.updateNodeList()
		case <-k.quit:
			tick.Stop()
			return
		}
	}
}
