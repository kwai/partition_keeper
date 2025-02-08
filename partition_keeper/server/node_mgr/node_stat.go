package node_mgr

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"sync"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/est"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/recorder"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"github.com/emirpasic/gods/maps/treemap"
	"google.golang.org/protobuf/proto"
)

var (
	validOpTransform = [][]bool{
		{true, true, true},
		{true, true, true},
		{true, false, true},
	}
	flagSkipHintCheck     = flag.Bool("skip_hint_check", false, "skip hint check")
	flagSkipAuditResource = flag.Bool("skip_audit_resource", false, "skip audit resource")
)

type addrNodes struct {
	// utils.RpcNode -> map[id]*NodeInfo
	nodes *treemap.Map
}

func newAddrNodes() *addrNodes {
	return &addrNodes{
		nodes: treemap.NewWith(func(a, b interface{}) int {
			addr1, addr2 := a.(*utils.RpcNode), b.(*utils.RpcNode)
			return addr1.Compare(addr2)
		}),
	}
}

func (a *addrNodes) getAll(key *utils.RpcNode) map[string]*NodeInfo {
	val, ok := a.nodes.Get(key)
	if !ok {
		return nil
	}
	return val.(map[string]*NodeInfo)
}

func (a *addrNodes) get(addr *utils.RpcNode, id string) *NodeInfo {
	val := a.getAll(addr)
	if len(val) == 0 {
		return nil
	}
	return val[id]
}

func (a *addrNodes) getUnique(key *utils.RpcNode) *NodeInfo {
	valMap := a.getAll(key)
	if len(valMap) != 1 {
		return nil
	}
	for _, item := range valMap {
		return item
	}
	return nil
}

func (a *addrNodes) remove(addr *utils.RpcNode, id string) {
	valMap := a.getAll(addr)
	if valMap == nil {
		return
	}
	delete(valMap, id)
	if len(valMap) == 0 {
		a.nodes.Remove(addr)
	}
}

func (a *addrNodes) addOrUpdate(info *NodeInfo) int {
	key := info.Address
	valMap := a.getAll(key)
	if len(valMap) == 0 {
		a.nodes.Put(key.Clone(), map[string]*NodeInfo{info.Id: info})
		return 1
	} else {
		valMap[info.Id] = info
		return len(valMap)
	}
}

func (a *addrNodes) count(key *utils.RpcNode) int {
	return len(a.getAll(key))
}

func (a *addrNodes) countAlive(key *utils.RpcNode) int {
	ans := 0
	val := a.getAll(key)
	for _, item := range val {
		if item.IsAlive {
			ans++
		}
	}
	return ans
}

type NodeStatsUpdateNotifier func()

type NodeStats struct {
	logName string
	// this lock is mostly used for collecting replicas & report to dispatcher
	// as they are from go routines outside the ServiceStat
	//
	// otherwise, all functions are called by the ServiceStat,
	// the thread-safety should be guaranteed by the ServiceStat itself
	serviceLock *utils.LooseLock

	// id -> *NodeInfo
	idNodes map[string]*NodeInfo

	// addr -> map[id]*NodeInfo
	addrNodes *addrNodes

	// give each node an order, which is very useful for gray-scale update
	// or other occasions where the node must be ordered
	nodeOrder map[string]int

	// hubname -> non-removing nodes count
	hubSize     map[string]int
	hubmap      *utils.HubHandle
	statUpdated map[string]NodeStatsUpdateNotifier
	hints       map[string]*pb.NodeHints
	estReplicas *EstimatedReplicaHandler

	nodesPath         string
	hintsPath         string
	zkConn            metastore.MetaStore
	failureDomainType pb.NodeFailureDomainType
	skipHintCheck     bool
}

func NewNodeStats(
	name string,
	l *utils.LooseLock,
	nodesPath string,
	hintsPath string,
	conn metastore.MetaStore,
) *NodeStats {
	result := &NodeStats{
		logName:           name,
		serviceLock:       l,
		idNodes:           make(map[string]*NodeInfo),
		addrNodes:         newAddrNodes(),
		nodeOrder:         make(map[string]int),
		hubSize:           make(map[string]int),
		statUpdated:       make(map[string]NodeStatsUpdateNotifier),
		hints:             make(map[string]*pb.NodeHints),
		estReplicas:       NewEstimatedReplicasHandler(),
		nodesPath:         nodesPath,
		hintsPath:         hintsPath,
		zkConn:            conn,
		failureDomainType: pb.NodeFailureDomainType_HOST,
		skipHintCheck:     *flagSkipHintCheck,
	}

	return result
}

func (n *NodeStats) WithFailureDomainType(fd pb.NodeFailureDomainType) *NodeStats {
	n.failureDomainType = fd
	return n
}

func (n *NodeStats) WithSkipHintCheck(flag bool) *NodeStats {
	n.skipHintCheck = flag
	return n
}

func (n *NodeStats) getNodeZkPath(id string) string {
	return fmt.Sprintf("%s/%s", n.nodesPath, id)
}

func (n *NodeStats) GetHubSize() map[string]int {
	return n.hubSize
}

func (n *NodeStats) GetHubMap() *utils.HubHandle {
	return n.hubmap
}

func (n *NodeStats) DivideByHubs(excludeOffline bool) map[string][]*NodeInfo {
	output := map[string][]*NodeInfo{}
	iter := n.hubmap.GetMap().Iterator()
	for iter.Next() {
		hubname := iter.Key().(string)
		output[hubname] = []*NodeInfo{}
	}

	for _, info := range n.idNodes {
		if !excludeOffline || info.Op != pb.AdminNodeOp_kOffline {
			output[info.Hub] = append(output[info.Hub], info)
		}
	}
	return output
}

func (n *NodeStats) fetchHubs(hubs []string, excludeOffline bool) map[string][]*NodeInfo {
	output := map[string][]*NodeInfo{}
	for _, name := range hubs {
		output[name] = []*NodeInfo{}
	}

	for _, info := range n.idNodes {
		if !excludeOffline || info.Op != pb.AdminNodeOp_kOffline {
			if _, ok := output[info.Hub]; ok {
				output[info.Hub] = append(output[info.Hub], info)
			}
		}
	}

	return output
}

func (n *NodeStats) searchHint(node *utils.RpcNode) (string, *pb.NodeHints) {
	if hint := n.hints[node.String()]; hint != nil {
		return node.String(), hint
	}
	if hint := n.hints[node.NodeName]; hint != nil {
		return node.NodeName, hint
	}
	return "", nil
}

func (n *NodeStats) getHubHint(node *utils.RpcNode, candidates []string) string {
	_, hint := n.searchHint(node)
	if hint == nil {
		return ""
	} else {
		idx := utils.FindIndex(candidates, hint.Hub)
		if idx == -1 {
			logging.Warning("%s: can't find hub hint %s in %v, maybe remove hub happened",
				n.logName,
				hint.Hub,
				candidates,
			)
			return ""
		}
		logging.Info("%s: allocate hub %s for %s as hint", n.logName, hint.Hub, node.String())
		return hint.Hub
	}
}

func (n *NodeStats) dynamicAllocateHub(hubs map[string][]*NodeInfo, info *NodeInfo) {
	thisAzHubs := n.hubmap.HubsOnAz(info.Az)

	logging.Assert(
		len(thisAzHubs) > 0,
		"%s: can't find hub for az %s node %s",
		n.logName,
		info.Az,
		info.LogStr(),
	)
	useHub := ""
	defer func() {
		info.Hub = useHub
		hubs[info.Hub] = append(hubs[info.Hub], info)
	}()

	if len(thisAzHubs) == 1 {
		useHub = thisAzHubs[0]
		return
	}

	if hintHub := n.getHubHint(info.Address, thisAzHubs); hintHub != "" {
		useHub = hintHub
		return
	}

	minHubSize := 0x7fffffff
	for _, hub := range thisAzHubs {
		nodes := hubs[hub]
		uniqNodes := map[string]bool{}
		for _, node := range nodes {
			if node.sameFailureDomain(info, n.failureDomainType) {
				logging.Info(
					"%s: new node %s under same failure domain with %s, use it's hub %s",
					n.logName,
					info.LogStr(),
					node.LogStr(),
					hub,
				)
				useHub = hub
				return
			}
			uniqNodes[node.Address.String()] = true
		}
		if len(uniqNodes) < minHubSize {
			minHubSize = len(uniqNodes)
			useHub = hub
		}
	}
}

func (n *NodeStats) staticAllocateHub(hubs map[string][]*NodeInfo, info *NodeInfo) []string {
	hubName, index := info.ExtractNodeIndex()
	logging.Assert(hubName != "" && index >= 0, "")
	info.Hub = hubName
	hubs[info.Hub] = append(hubs[info.Hub], info)

	replaced := []string{}
	for _, otherInfo := range n.idNodes {
		if otherInfo.Az == info.Az && otherInfo.NodeIndex == info.NodeIndex &&
			otherInfo.Op != pb.AdminNodeOp_kOffline {
			logging.Info(
				"%s: node %s replace %s with same index: %s",
				n.logName,
				info.LogStr(),
				otherInfo.LogStr(),
				info.NodeIndex,
			)
			replaced = append(replaced, otherInfo.Id)
		}
	}

	// there's a tiny probability that the following events happens:
	//   1. node detector detects a node
	//   2. user triggers remove hubs
	//   3. the node detection event dispatches to node stats,
	//      but it's hubs has been removed now
	if n.hubmap.GetAz(hubName) == "" {
		info.Op = pb.AdminNodeOp_kOffline
	}
	return replaced
}

func (n *NodeStats) allocateHub(hubs map[string][]*NodeInfo, info *NodeInfo) []string {
	if info.NodeIndex == "" {
		n.dynamicAllocateHub(hubs, info)
		return nil
	} else {
		return n.staticAllocateHub(hubs, info)
	}
}

func (n *NodeStats) generateNewNodeInfo(
	id string,
	r *NodePing,
	recordHubDist map[string][]*NodeInfo,
	output map[string]*NodeInfo,
) {
	getCurrentNodeInfo := func(stable, unstable map[string]*NodeInfo, id string) *NodeInfo {
		if info, ok := unstable[id]; ok {
			return info
		}
		if info, ok := stable[id]; ok {
			return info.Clone()
		}
		return nil
	}

	if !r.IsAlive {
		node := getCurrentNodeInfo(n.idNodes, output, id)
		logging.Assert(
			node != nil,
			"%s: can't find dead node %v from the node status map",
			n.logName,
			id,
		)
		node.IsAlive = false
		output[id] = node
		logging.Info("%s: mark node %s dead", n.logName, node.LogStr())
	} else {
		node := getCurrentNodeInfo(n.idNodes, output, id)
		if node != nil {
			oldOp := node.Op
			if node.Op == pb.AdminNodeOp_kRestart && node.ProcessId != r.ProcessId {
				node.Op = pb.AdminNodeOp_kNoop
			}
			logging.Info("%s: node change props old: %v, new: %v, op: %s -> %s",
				n.logName, &node.NodePing, r,
				oldOp.String(), node.Op.String(),
			)
			node.Id = id
			node.NodePing = *r
			node.Address = r.Address.Clone()
			output[id] = node
		} else {
			newInfo := &NodeInfo{}
			newInfo.Id = id
			newInfo.NodePing = *r
			newInfo.Address = r.Address.Clone()
			newInfo.Op = pb.AdminNodeOp_kNoop
			newInfo.Weight = DEFAULT_WEIGHT
			newInfo.Score = est.INVALID_SCORE
			replaced := n.allocateHub(recordHubDist, newInfo)
			for _, rid := range replaced {
				oldInfo := getCurrentNodeInfo(n.idNodes, output, rid)
				logging.Assert(oldInfo != nil, "%s: can't find replaced node info for %s", n.logName, rid)
				oldInfo.Op = pb.AdminNodeOp_kOffline
				output[rid] = oldInfo
			}
			output[id] = newInfo
			logging.Info("%s: detect new node: %v, allocate hub %s, node index: %s",
				n.logName, r, newInfo.Hub, newInfo.NodeIndex,
			)
		}
	}
}

func (n *NodeStats) HubUnits() utils.HardwareUnits {
	output := utils.HardwareUnits{}
	for _, info := range n.idNodes {
		if info.Op != pb.AdminNodeOp_kOffline {
			output.AddUnit(info.Hub, info.resource)
		}
	}
	return output
}

func (n *NodeStats) MustGetRpcNode(nodeId string) *utils.RpcNode {
	info, ok := n.idNodes[nodeId]
	if !ok {
		logging.Fatal("%s: get rpc node for %s failed", n.logName, nodeId)
	}
	return info.Address
}

func (n *NodeStats) MustGetNodeOrder(nid string) int {
	order, ok := n.nodeOrder[nid]
	logging.Assert(ok, "%s: can't find node order for %s", n.logName, nid)
	return order
}

func (n *NodeStats) MustGetNodeInfo(nodeId string) *NodeInfo {
	info, ok := n.idNodes[nodeId]
	if !ok {
		logging.Fatal("%s: get node info failed: %s", n.logName, nodeId)
	}
	return info
}

func (n *NodeStats) GetNodeInfo(nodeId string) *NodeInfo {
	return n.idNodes[nodeId]
}

func (n *NodeStats) AllNodes() map[string]*NodeInfo {
	return n.idNodes
}

func (n *NodeStats) GetEstReplicas() *EstimatedReplicaHandler {
	return n.estReplicas
}

func (n *NodeStats) GetNodeInfoByAddr(node *utils.RpcNode, matchPort bool) *NodeInfo {
	return n.GetNodeInfoByAddrs([]*utils.RpcNode{node}, matchPort)[0]
}

func (n *NodeStats) GetNodeInfoByAddrs(nodes []*utils.RpcNode, matchPort bool) []*NodeInfo {
	tryGetNodeByAddr := func(node *utils.RpcNode) *NodeInfo {
		matchedNodes := n.addrNodes.getAll(node)
		if len(matchedNodes) == 1 {
			for _, n := range matchedNodes {
				return n
			}
		} else if len(matchedNodes) == 0 {
			if matchPort {
				return nil
			} else {
				for _, info := range n.idNodes {
					if info.Address.NodeName == node.NodeName {
						return info
					}
				}
				return nil
			}
		} else {
			if matchPort {
				return nil
			} else {
				for _, n := range matchedNodes {
					return n
				}
			}
		}
		return nil
	}

	var result []*NodeInfo
	for _, addr := range nodes {
		result = append(result, tryGetNodeByAddr(addr))
	}
	return result
}

func (n *NodeStats) FilterNodes(filters ...NodeFilter) (result []string) {
	for id, info := range n.idNodes {
		skipped := false
		for _, f := range filters {
			if !f(info) {
				skipped = true
				break
			}
		}
		if !skipped {
			result = append(result, id)
		}
	}
	return
}

func (n *NodeStats) FilterGivenNodes(input []string, filters ...NodeFilter) (result []string) {
	for _, i := range input {
		info := n.MustGetNodeInfo(i)
		skipped := false
		for _, f := range filters {
			if !f(info) {
				skipped = true
				break
			}
		}
		if !skipped {
			result = append(result, i)
		}
	}
	return
}

func (n *NodeStats) SubscribeUpdate(key string, notifier NodeStatsUpdateNotifier) {
	n.statUpdated[key] = notifier
}

func (n *NodeStats) UnsubscribeUpdate(key string) {
	delete(n.statUpdated, key)
}

func (n *NodeStats) UpdateStats(rep map[string]*NodePing) {
	infos := map[string]*NodeInfo{}

	hubs := n.DivideByHubs(true)
	for id, r := range rep {
		n.generateNewNodeInfo(id, r, hubs, infos)
	}

	n.durableAndApplyNodeInfo(infos)
}

func (n *NodeStats) calculateHubSize() {
	n.hubSize = make(map[string]int)
	for _, info := range n.idNodes {
		if info.Op != pb.AdminNodeOp_kOffline {
			n.hubSize[info.Hub]++
		}
	}
}

func (n *NodeStats) reorderNodes() {
	n.nodeOrder = make(map[string]int)
	nodes := []*NodeInfo{}
	for _, node := range n.idNodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		n1, n2 := nodes[i], nodes[j]
		if n1.Hub != n2.Hub {
			return n1.Hub < n2.Hub
		}
		return n1.Id < n2.Id
	})
	for i, node := range nodes {
		n.nodeOrder[node.Id] = i
	}
}

func (n *NodeStats) publishUpdate() {
	for key, cb := range n.statUpdated {
		logging.Info("%s: run registered func %s as stat is updated", n.logName, key)
		cb()
	}
}

func (n *NodeStats) removeNodeInfo(nodes []*NodeInfo) {
	n.serviceLock.AllowRead()
	wg := sync.WaitGroup{}
	wg.Add(len(nodes))
	for _, node := range nodes {
		go func(info *NodeInfo) {
			defer wg.Done()
			logging.Assert(info.Op == pb.AdminNodeOp_kOffline, "")
			zkPath := n.getNodeZkPath(info.Id)
			logging.Assert(n.zkConn.Delete(context.Background(), zkPath), "")
		}(node)
	}
	wg.Wait()
	n.serviceLock.DisallowRead()

	for _, node := range nodes {
		delete(n.idNodes, node.Id)
		n.addrNodes.remove(node.Address, node.Id)
	}
	n.reorderNodes()
}

func (n *NodeStats) CountResource(filters ...NodeFilter) utils.HardwareUnits {
	filters = append(filters, ExcludeNodeAdminByOp(pb.AdminNodeOp_kOffline))
	nodes := n.FilterNodes(filters...)

	handled := map[string]bool{}
	output := utils.HardwareUnits{}

	for _, node := range nodes {
		info := n.idNodes[node]
		if _, ok := handled[info.Address.String()]; ok {
			continue
		}
		output.AddUnit(info.Hub, info.resource)
		logging.Verbose(2, "%s: add %s total: %v", n.logName, info.LogStr(), output)
		handled[info.Address.String()] = true
	}

	return output
}

func (n *NodeStats) AuditGivenResource(
	hubs utils.HardwareUnits,
	perHub utils.HardwareUnit,
) *pb.ErrorStatus {
	if *flagSkipAuditResource {
		return nil
	}
	for hubName, res := range hubs {
		if err := res.EnoughFor(perHub); err != nil {
			logging.Info(
				"%s: audit resource failed for hub %s: %s",
				n.logName,
				hubName,
				err.Error(),
			)
			return pb.AdminErrorMsg(pb.AdminError_kNotEnoughResource, err.Error())
		}
	}
	return nil
}

func (n *NodeStats) auditResource(toUpdate []*NodeInfo, perHub utils.HardwareUnit) *pb.ErrorStatus {
	if len(perHub) == 0 {
		return nil
	}
	updateMap := map[string]*NodeInfo{}
	for _, i := range toUpdate {
		updateMap[i.Id] = i
	}
	planningOffline := func(info *NodeInfo) bool {
		if got := updateMap[info.Id]; got == nil {
			return false
		} else {
			return got.Op == pb.AdminNodeOp_kOffline
		}
	}
	hubResource := n.CountResource(ReverseFilter(planningOffline))
	return n.AuditGivenResource(hubResource, perHub)
}

func (n *NodeStats) durableAndApplyNodeInfo(nodes map[string]*NodeInfo) {
	n.serviceLock.AllowRead()

	wg := sync.WaitGroup{}
	wg.Add(len(nodes))
	for _, info := range nodes {
		go func(info *NodeInfo) {
			defer wg.Done()
			data := utils.MarshalJsonOrDie(info)
			zkPath := n.getNodeZkPath(info.Id)

			_, oldNode := n.idNodes[info.Id]
			if oldNode {
				logging.Assert(n.zkConn.Set(context.Background(), zkPath, data), "")
			} else {
				logging.Assert(n.zkConn.Create(context.Background(), zkPath, data), "")
			}
		}(info)
	}

	hintKeys := map[string]bool{}
	for _, info := range nodes {
		key, _ := n.searchHint(info.Address)
		if key != "" {
			hintKeys[key] = true
		}
	}
	for key := range hintKeys {
		// TODO(huyifan03): maybe we need a better strategy to mark hint as used
		// if we have more info in this hint
		n.bgCleanRemoteHint(key, &wg)
	}
	wg.Wait()

	n.serviceLock.DisallowRead()
	for key := range hintKeys {
		delete(n.hints, key)
	}
	for _, info := range nodes {
		oldInfo, hasOld := n.idNodes[info.Id]
		if hasOld {
			// in case that the address changed
			n.addrNodes.remove(oldInfo.Address, oldInfo.Id)
		}
		n.idNodes[info.Id] = info
		n.addrNodes.addOrUpdate(info)
	}

	n.calculateHubSize()
	n.reorderNodes()
	n.publishUpdate()
}

func (n *NodeStats) auditedDurableAndApply(
	nodes []*NodeInfo,
	perHub utils.HardwareUnit,
) *pb.ErrorStatus {
	if err := n.auditResource(nodes, perHub); err != nil {
		return err
	}
	n.durableAndApplyNodeInfo(MapNodeInfos(nodes))
	return nil
}

func (n *NodeStats) transferSome(
	infoByHubs map[string][]*NodeInfo,
	fromHub, toHub string,
) []*NodeInfo {
	fromNodes := infoByHubs[fromHub]
	toNodes := infoByHubs[toHub]

	var transferred []*NodeInfo
	transferred, infoByHubs[fromHub] = PopNodesWithinFailureDomain(
		fromNodes,
		fromNodes[len(fromNodes)-1],
		n.failureDomainType,
	)

	for _, info := range transferred {
		logging.Info("%s: change node %s hub %s -> %s", n.logName, info.LogStr(), info.Hub, toHub)
		info.Hub = toHub
		toNodes = append(toNodes, info)
	}
	infoByHubs[toHub] = toNodes
	return transferred
}

func (n *NodeStats) generateBalanceHubPlan(hubmap *utils.HubHandle) []*NodeInfo {
	azGroups := hubmap.DivideByAz()
	hubChanged := []*NodeInfo{}
	for az, hubs := range azGroups {
		if len(hubs) < 2 {
			logging.Info("%s: don't balance %s as only has one hub %v", n.logName, az, hubs)
			continue
		}
		infos := n.fetchHubs(hubs, true)
		min, max, minVal, maxVal := "", "", -1, -1
		for {
			min, max, minVal, maxVal = FindMinMax(infos)
			if maxVal-minVal <= 1 {
				break
			}
			transferred := n.transferSome(infos, max, min)
			if minVal+len(transferred) >= maxVal {
				break
			}
			hubChanged = append(hubChanged, transferred...)
		}
		logging.Info(
			"%s: finish balance %s hubs %v with min %d@%s max %d@%s",
			n.logName,
			az,
			hubs,
			minVal, min,
			maxVal, max,
		)
	}
	return hubChanged
}

func (n *NodeStats) removeNodesOnHubs(hubs []string) []*NodeInfo {
	var toRemove []*NodeInfo

	for _, info := range n.idNodes {
		if utils.FindIndex(hubs, info.Hub) != -1 {
			if info.Op != pb.AdminNodeOp_kOffline {
				logging.Info(
					"%s: try to mark %s as removing as it's hub %s is gone, current op: %s",
					n.logName,
					info.LogStr(),
					info.Hub,
					info.Op.String(),
				)
				newInfo := info.Clone()
				newInfo.Op = pb.AdminNodeOp_kOffline
				toRemove = append(toRemove, newInfo)
			}
		}
	}

	if len(toRemove) == 0 {
		return nil
	}
	return toRemove
}

func (n *NodeStats) scatterNodesToHubs(fromHubs, toHubs []string) []*NodeInfo {
	fromInfos := n.fetchHubs(fromHubs, true)
	toInfos := n.fetchHubs(toHubs, true)

	updated := []*NodeInfo{}
	for _, infos := range fromInfos {
		for len(infos) > 0 {
			last := infos[len(infos)-1]
			transferred, remained := PopNodesWithinFailureDomain(infos, last, n.failureDomainType)

			min, _, _, _ := FindMinMax(toInfos)
			for _, node := range transferred {
				logging.Info(
					"%s: change node %s hub %s -> %s",
					n.logName,
					node.LogStr(),
					node.Hub,
					min,
				)
				node.Hub = min
				toInfos[min] = append(toInfos[min], node)
			}
			updated = append(updated, transferred...)
			infos = remained
		}
	}

	return updated
}

func (n *NodeStats) removeHubs(removed *utils.HubHandle, adjust bool) {
	azGroups := removed.DivideByAz()

	var updateList []*NodeInfo
	for az, hubs := range azGroups {
		remain := n.hubmap.HubsOnAz(az)
		if !adjust || len(remain) == 0 {
			logging.Info("%s: remove nodes on hubs %s directly, adjust: %v, remain: %v",
				n.logName, hubs, adjust, remain)
			updated := n.removeNodesOnHubs(hubs)
			if len(updated) > 0 {
				updateList = append(updateList, updated...)
			}
		} else {
			updated := n.scatterNodesToHubs(hubs, remain)
			if len(updated) > 0 {
				updateList = append(updateList, updated...)
			}
		}
	}

	if len(updateList) > 0 {
		n.durableAndApplyNodeInfo(MapNodeInfos(updateList))
	} else {
		n.calculateHubSize()
	}
}

func (n *NodeStats) staticSelectOfflineNodes(
	azName string,
	hubs []string,
	newSize int32,
) ([]*NodeInfo, *pb.ErrorStatus) {
	if int(newSize)%len(hubs) != 0 {
		return nil, pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"new_size(%d) can't be evenly divided by hubsize(%d) in az(%s)",
			newSize,
			len(hubs),
			azName,
		)
	}
	maxIndex := (int(newSize) / len(hubs)) - 1
	var offline []*NodeInfo
	for _, info := range n.idNodes {
		if info.Az == azName {
			_, index := info.ExtractNodeIndex()
			if index > maxIndex {
				logging.Info(
					"%s: plan to offline node %s at hub %s",
					n.logName,
					info.LogStr(),
					info.Hub,
				)
				tmp := info.Clone()
				tmp.Op = pb.AdminNodeOp_kOffline
				offline = append(offline, tmp)
			}
		}
	}
	return offline, nil
}

func (n *NodeStats) dynamicSelectOfflineNodes(
	hubs []string,
	newSize int32,
) ([]*NodeInfo, *pb.ErrorStatus) {
	nodes := n.fetchHubs(hubs, true)
	count := 0
	for _, infos := range nodes {
		count += len(infos)
	}
	var offline []*NodeInfo
	for count > 0 && count > int(newSize) {
		_, max, _, _ := FindMinMax(nodes)
		infos := nodes[max]
		index := utils.FindMinIndex(len(infos), func(i, j int) bool {
			left, right := infos[i], infos[j]
			if left.IsAlive != right.IsAlive {
				return !left.IsAlive
			}
			if left.Op != right.Op {
				return left.Op == pb.AdminNodeOp_kRestart
			}
			return false
		})
		logging.Assert(index != -1, "%s: can't find any node to remove in hub %s", n.logName, max)
		removed, remain := PopNodesWithinFailureDomain(infos, infos[index], n.failureDomainType)
		nodes[max] = remain
		for _, r := range removed {
			logging.Info("%s: plan to offline node %s at hub %s", n.logName, r.LogStr(), max)
			tmp := r.Clone()
			tmp.Op = pb.AdminNodeOp_kOffline
			offline = append(offline, tmp)
		}
		count -= len(removed)
	}
	return offline, nil
}

func (n *NodeStats) ShrinkAz(
	azName string,
	newSize int32,
	hubRes utils.HardwareUnit,
	staticIndexed bool,
	resp *pb.ShrinkAzResponse,
) {
	hubs := n.hubmap.HubsOnAz(azName)
	if len(hubs) == 0 {
		resp.Status = pb.AdminErrorMsg(pb.AdminError_kInvalidParameter,
			"can't find any hub for az %s", azName)
		return
	}
	if newSize <= 0 {
		resp.Status = pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"new size should large than 0",
		)
		return
	}

	var offline []*NodeInfo
	var selectError *pb.ErrorStatus
	if staticIndexed {
		offline, selectError = n.staticSelectOfflineNodes(azName, hubs, newSize)
	} else {
		offline, selectError = n.dynamicSelectOfflineNodes(hubs, newSize)
	}
	if selectError != nil {
		resp.Status = selectError
		return
	}

	if err := n.auditedDurableAndApply(offline, hubRes); err != nil {
		resp.Status = err
	} else {
		resp.Status = pb.ErrStatusOk()
		for _, info := range offline {
			resp.Shrinked = append(resp.Shrinked, info.Address.ToPb())
		}
	}
}

func (n *NodeStats) AssignHub(node *utils.RpcNode, hub string) *pb.ErrorStatus {
	az := n.hubmap.GetAz(hub)
	if az == "" {
		return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, "can't recognize hub %s", hub)
	}
	info := n.GetNodeInfoByAddr(node, true)
	if info == nil {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"can't find node %s",
			node.String(),
		)
	}
	if info.Hub == hub {
		return pb.AdminErrorMsg(
			pb.AdminError_kDuplicateRequest,
			"node %s already assigned to hub %s",
			node.String(),
			hub,
		)
	}
	if info.Az != az {
		return pb.AdminErrorMsg(
			pb.AdminError_kInvalidParameter,
			"node %s located in %s, not allowed to assign to %s@%s",
			node.String(),
			info.Az,
			hub,
			az,
		)
	}
	newInfo := info.Clone()
	newInfo.Hub = hub
	n.durableAndApplyNodeInfo(map[string]*NodeInfo{newInfo.Id: newInfo})
	return pb.ErrStatusOk()
}

func (n *NodeStats) AuditUpdateHubs(
	hubmap *utils.HubHandle,
	perHub utils.HardwareUnit,
) *pb.ErrorStatus {
	// TODO(huyifan03): currently, several run to generateBalanceHubPlan
	// may lead to different plans
	// which means the audit is not strictly correct
	changed := n.generateBalanceHubPlan(hubmap)
	return n.auditResource(changed, perHub)
}

func (n *NodeStats) UpdateHubs(hubs *utils.HubHandle, adjustHubs bool) {
	removed, added := n.hubmap.Diff(hubs)
	logging.Info(
		"%s: update hub removed: %s, added: %s",
		n.logName,
		removed.GetMap().String(),
		added.GetMap().String(),
	)
	if removed.GetMap().Size() > 0 {
		n.hubmap = hubs.Clone()
		logging.Assert(
			added.GetMap().Size() == 0,
			"%s: removed hubs: %s, added hubs: %s",
			n.logName,
			removed.GetMap().String(),
			added.GetMap().String(),
		)
		n.removeHubs(removed, adjustHubs)
	} else {
		n.hubmap = hubs.Clone()
		if adjustHubs {
			hubChanged := n.generateBalanceHubPlan(n.hubmap)
			if len(hubChanged) > 0 {
				n.durableAndApplyNodeInfo(MapNodeInfos(hubChanged))
			} else {
				n.calculateHubSize()
			}
		} else {
			n.calculateHubSize()
		}
	}
}

func (n *NodeStats) AdminNodes(
	nodes []*utils.RpcNode,
	matchPort bool,
	op pb.AdminNodeOp,
	hubRes utils.HardwareUnit,
) (res []*pb.ErrorStatus) {
	logging.Info("%s: try to admin nodes: %v with op %s", n.logName, nodes, op.String())
	// if match_port is false, we may get duplicate infos by GetNodeInfoByAddrs
	// but it doesn't matter as we will do deduplication when we do durable
	infos := n.GetNodeInfoByAddrs(nodes, matchPort)

	var newInfos []*NodeInfo

	for i := range infos {
		if infos[i] == nil {
			res = append(
				res,
				pb.AdminErrorMsg(
					pb.AdminError_kNodeNotExisting,
					"%s: can't find %v",
					n.logName,
					nodes[i],
				),
			)
			continue
		}
		if infos[i].Op == op {
			res = append(
				res,
				pb.AdminErrorMsg(
					pb.AdminError_kDuplicateRequest,
					"%s: node %v is already %s",
					n.logName,
					infos[i].Address,
					op.String(),
				),
			)
			continue
		}
		if !validOpTransform[infos[i].Op][op] {
			res = append(
				res,
				pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, "%s: node %v can't from %s to %s",
					n.logName,
					infos[i].Address,
					pb.AdminNodeOp_name[int32(infos[i].Op)],
					pb.AdminNodeOp_name[int32(op)]),
			)
			continue
		}
		res = append(res, pb.ErrStatusOk())
		newInfo := *infos[i]
		newInfo.Op = op
		newInfos = append(newInfos, &newInfo)
	}

	if len(newInfos) == 0 {
		return
	}

	if op != pb.AdminNodeOp_kOffline {
		n.durableAndApplyNodeInfo(MapNodeInfos(newInfos))
		return
	}
	if err := n.auditedDurableAndApply(newInfos, hubRes); err != nil {
		for i := range res {
			res[i] = err
		}
	}
	return
}

func (n *NodeStats) UpdateWeight(nodes []*utils.RpcNode, weights []float32) []*pb.ErrorStatus {
	output := []*pb.ErrorStatus{}
	minItems := utils.Min(len(nodes), len(weights))
	if minItems <= 0 {
		return output
	}
	nodes = nodes[0:minItems]
	weights = weights[0:minItems]
	infos := n.GetNodeInfoByAddrs(nodes, true)
	var newInfos []*NodeInfo
	for i := range infos {
		if infos[i] == nil {
			output = append(
				output,
				pb.AdminErrorMsg(
					pb.AdminError_kNodeNotExisting,
					"%s: can't find %v",
					n.logName,
					nodes[i],
				),
			)
			continue
		}
		if infos[i].Weight == weights[i] {
			output = append(output, pb.ErrStatusOk())
			continue
		}
		if weights[i] < MIN_WEIGHT || weights[i] > MAX_WEIGHT {
			output = append(
				output,
				pb.AdminErrorMsg(
					pb.AdminError_kInvalidParameter,
					"invalid weight %f of %s, should in [%f,%f])",
					weights[i],
					nodes[i].String(),
					MIN_WEIGHT,
					MAX_WEIGHT,
				),
			)
			continue
		}
		output = append(output, pb.ErrStatusOk())
		newInfo := *infos[i]
		newInfo.Weight = weights[i]
		newInfos = append(newInfos, &newInfo)
	}
	if len(newInfos) == 0 {
		return output
	}
	n.durableAndApplyNodeInfo(MapNodeInfos(newInfos))
	return output
}

func (n *NodeStats) bgUpdateHint(
	ipPort string,
	hint *pb.NodeHints,
	create bool,
	wg *sync.WaitGroup,
) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		data := utils.MarshalJsonOrDie(hint)
		path := fmt.Sprintf("%s/%s", n.hintsPath, ipPort)
		if create {
			succ := n.zkConn.Create(context.Background(), path, data)
			logging.Assert(succ, "")
		} else {
			succ := n.zkConn.Set(context.Background(), path, data)
			logging.Assert(succ, "")
		}
	}()
}

func (n *NodeStats) durableHints(hints map[string]*pb.NodeHints) {
	n.serviceLock.AllowRead()
	defer n.serviceLock.DisallowRead()

	wg := sync.WaitGroup{}
	for hintKey, hint := range hints {
		if oldHint, ok := n.hints[hintKey]; ok {
			if proto.Equal(hint, oldHint) {
				logging.Info("%s: %s's hint kept unchanged, skip request", n.logName, hintKey)
			} else {
				logging.Info("%s: %s's hint update from %v to %v", n.logName, hintKey, oldHint, hint)
				n.bgUpdateHint(hintKey, hint, false, &wg)
			}
		} else {
			logging.Info("%s: add new hint %s -> %v", n.logName, hintKey, hint)
			n.bgUpdateHint(hintKey, hint, true, &wg)
		}
	}
	wg.Wait()
}

func (n *NodeStats) checkHints(hints map[string]*pb.NodeHints) *pb.ErrorStatus {
	for hintKey, hint := range hints {
		rpcNode := utils.FromHostPort(hintKey)
		if rpcNode == nil {
			rpcNode = &utils.RpcNode{
				NodeName: hintKey,
				Port:     -1,
			}
		}
		if n.skipHintCheck {
			continue
		}
		rms, err := third_party.QueryHostInfo(rpcNode.NodeName)
		if err != nil || rms == nil {
			logging.Warning("%s: can't get rms for %s, err: %v", n.logName, hintKey, err)
			return pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"query rms for %s failed: %v",
				hintKey,
				err,
			)
		}
		if az := n.hubmap.GetAz(hint.Hub); az == "" {
			logging.Warning("%s: hub %s doesn't exists for %s", n.logName, hint.Hub, hintKey)
			return pb.AdminErrorMsg(
				pb.AdminError_kInvalidParameter,
				"%s invalid hubname %s",
				hintKey,
				hint.Hub,
			)
		} else if az != rms.Az {
			logging.Warning("%s: node %s hint on hub %s of %s, not match with it's az %s", n.logName, hintKey, hint.Hub, az, rms.Az)
			return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, "%s on %s not match %s", hintKey, rms.Az, az)
		}
	}
	return nil
}

func (n *NodeStats) AddHints(hints map[string]*pb.NodeHints, matchPort bool) *pb.ErrorStatus {
	stripPort := func(hints map[string]*pb.NodeHints) map[string]*pb.NodeHints {
		output := map[string]*pb.NodeHints{}
		for key, hint := range hints {
			node := utils.FromHostPort(key)
			if node == nil {
				output[key] = hint
			} else {
				output[node.NodeName] = hint
			}
		}
		return output
	}

	if !matchPort {
		hints = stripPort(hints)
	}
	if err := n.checkHints(hints); err != nil {
		return err
	}

	n.durableHints(hints)
	for hintKey, hint := range hints {
		n.hints[hintKey] = hint
	}
	return pb.ErrStatusOk()
}

func (n *NodeStats) bgCleanRemoteHint(hintKey string, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		path := fmt.Sprintf("%s/%s", n.hintsPath, hintKey)
		succ := n.zkConn.Delete(context.Background(), path)
		logging.Assert(succ, "")
	}()
}

func (n *NodeStats) tryCleanRemoteHints(keys []string) *pb.ErrorStatus {
	n.serviceLock.AllowRead()
	defer n.serviceLock.DisallowRead()

	for _, key := range keys {
		if n.hints[key] == nil {
			logging.Info("%s: can't find hint for %s, reject whole clean", n.logName, key)
			return pb.AdminErrorMsg(pb.AdminError_kInvalidParameter, "can't find hint for %s", key)
		}
	}

	wg := sync.WaitGroup{}
	for _, key := range keys {
		n.bgCleanRemoteHint(key, &wg)
	}
	wg.Wait()
	return nil
}

func (n *NodeStats) RemoveHints(nodes []*pb.RpcNode, matchPort bool) *pb.ErrorStatus {
	var key []string
	for _, node := range nodes {
		if matchPort {
			key = append(key, node.ToHostPort())
		} else {
			key = append(key, node.NodeName)
		}
	}

	if err := n.tryCleanRemoteHints(key); err != nil {
		return err
	}
	for _, node := range key {
		delete(n.hints, node)
	}
	return pb.ErrStatusOk()
}

func (n *NodeStats) GetNodeLivenessMap(withLock bool) map[string]*NodePing {
	if withLock {
		n.serviceLock.LockRead()
		defer n.serviceLock.UnlockRead()
	}

	result := make(map[string]*NodePing)
	for id, info := range n.idNodes {
		result[id] = info.NodePing.Clone()
	}
	return result
}

func (n *NodeStats) updateHubsInLoading(hubs *utils.HubHandle, adjustHubs bool) {
	n.serviceLock.LockWrite()
	defer n.serviceLock.UnlockWrite()
	n.UpdateHubs(hubs, adjustHubs)
}

func (n *NodeStats) CheckNodeIndex(staticIndexed bool) int {
	if !staticIndexed {
		return 0
	}
	index := map[string]*NodeInfo{}
	conflictNodes := 0
	for _, info := range n.idNodes {
		if info.Op != pb.AdminNodeOp_kOffline {
			hubIndex := fmt.Sprintf("%s-%s", info.Az, info.NodeIndex)
			current := index[hubIndex]
			if current != nil {
				logging.Warning(
					"%s: %s and %s has conflict index %s",
					n.logName,
					current.LogStr(),
					info.LogStr(),
					hubIndex,
				)
				conflictNodes++
			} else {
				index[hubIndex] = info
			}
		}
	}
	return conflictNodes
}

func (n *NodeStats) RemoveUnusedNodeInfos(rec *recorder.AllNodesRecorder) {
	dupDead := []*NodeInfo{}
	offlineDone := []*NodeInfo{}

	iter := n.addrNodes.nodes.Iterator()
	for iter.Next() {
		addr, nodes := iter.Key().(*utils.RpcNode), iter.Value().(map[string]*NodeInfo)
		logging.Verbose(
			1,
			"%s: check addr %s, nodes count: %d",
			n.logName,
			addr.String(),
			len(nodes),
		)
		if len(nodes) == 1 {
			for _, info := range nodes {
				if info.Op == pb.AdminNodeOp_kOffline && !info.IsAlive &&
					rec.CountAll(info.Id) == 0 {
					logging.Info(
						"%s: %s has no replicas, has been totally offline, remove entry",
						n.logName, info.LogStr(),
					)
					offlineDone = append(offlineDone, info)
				}
			}
			continue
		}
		aliveCount := CountNodes(nodes, GetAliveNode)
		if aliveCount > 1 {
			logging.Info(
				"%s: %s alive count: %d, don't remove anyone",
				n.logName, addr.String(), aliveCount,
			)
			continue
		}
		for _, info := range nodes {
			if info.IsAlive {
				continue
			}
			if info.Op != pb.AdminNodeOp_kOffline {
				if aliveCount == 1 {
					logging.Info(
						"%s: remove dead node %s as %s conflict",
						n.logName, info.Id, addr.String(),
					)
					dupDead = append(dupDead, info.Clone())
					dupDead[len(dupDead)-1].Op = pb.AdminNodeOp_kOffline
				} else {
					logging.Info("%s: don't mark %s as offline as we can't judge which one is old among all the %s, alive count: %d",
						n.logName, info.Id, addr.String(), aliveCount,
					)
				}
			} else {
				if rec.CountAll(info.Id) == 0 {
					logging.Info(
						"%s: %s has no replicas, has been totally offline, remove entry",
						n.logName, info.LogStr(),
					)
					offlineDone = append(offlineDone, info)
				} else {
					logging.Info("%s: %s nodes count %d, don't remove entry", n.logName, info.LogStr(), rec.CountAll(info.Id))
				}
			}
		}
	}

	if len(dupDead) > 0 {
		n.durableAndApplyNodeInfo(MapNodeInfos(dupDead))
	}
	if len(offlineDone) > 0 {
		n.removeNodeInfo(offlineDone)
	}
}

func (n *NodeStats) RefreshScore(estimator est.Estimator, force bool) bool {
	scoreRefreshed := map[string]*NodeInfo{}

	checkPass := true
	for _, node := range n.idNodes {
		newScore := estimator.Score(node.resource)
		if newScore != est.INVALID_SCORE {
			if newScore != node.Score {
				logging.Info(
					"%s: %s score update %d -> %d",
					n.logName,
					node.LogStr(),
					node.Score,
					newScore,
				)
				newInfo := node.Clone()
				newInfo.Score = newScore
				scoreRefreshed[newInfo.Id] = newInfo
			}
		} else {
			if node.Op == pb.AdminNodeOp_kOffline {
				if node.Score != est.INVALID_SCORE {
					logging.Info("%s: %s is offline, reset score %d -> %d", n.logName, node.LogStr(), node.Score, est.INVALID_SCORE)
					newInfo := node.Clone()
					newInfo.Score = est.INVALID_SCORE
					scoreRefreshed[newInfo.Id] = newInfo
				}
			} else {
				if force {
					logging.Info("%s: %s can't refresh score, which will block schedule", n.logName, node.LogStr())
					checkPass = false
				} else if node.Score == est.INVALID_SCORE {
					logging.Info("%s: %s don't have a valid score, which will block schedule", n.logName, node.LogStr())
					checkPass = false
				} else {
					logging.Info("%s: can't estimate new score for %s, use old: %d", n.logName, node.LogStr(), node.Score)
				}
			}
		}
	}

	if len(scoreRefreshed) > 0 {
		n.durableAndApplyNodeInfo(scoreRefreshed)
	}
	return checkPass
}

func (n *NodeStats) loadNodes() {
	logging.Info("%s: load nodes from %s", n.logName, n.nodesPath)

	children, exists, succ := n.zkConn.Children(context.Background(), n.nodesPath)
	logging.Assert(succ && exists, "")

	hubs := utils.NewHubHandle()
	for _, node := range children {
		nodePath := fmt.Sprintf("%s/%s", n.nodesPath, node)
		nodeProps, exists, succ := n.zkConn.Get(context.Background(), nodePath)
		logging.Assert(succ && exists, "load path %s succ: %v, exists: %v", nodePath, exists, succ)

		info := &NodeInfo{}
		utils.UnmarshalJsonOrDie(nodeProps, info)
		logging.Info("%s: load node properties: %s", n.logName, string(nodeProps))

		_, ok := n.idNodes[info.Id]
		logging.Assert(!ok, "conflict node id: %s", info.Id)
		n.idNodes[info.Id] = info
		n.addrNodes.addOrUpdate(info)

		if info.Op != pb.AdminNodeOp_kOffline {
			hubs.AddHub(&pb.ReplicaHub{Name: info.Hub, Az: info.Az})
		}
	}

	n.hubmap = hubs
	n.reorderNodes()
}

func (n *NodeStats) loadHints() {
	logging.Info("%s: load hints from %s", n.logName, n.hintsPath)

	children, exists, succ := n.zkConn.Children(context.Background(), n.hintsPath)
	logging.Assert(succ && exists, "")

	for _, child := range children {
		hintPath := fmt.Sprintf("%s/%s", n.hintsPath, child)
		data, exists, succ := n.zkConn.Get(context.Background(), hintPath)
		logging.Assert(succ && exists, "load path %s succ %v, exists: %v", hintPath, succ, exists)

		hint := &pb.NodeHints{}
		utils.UnmarshalJsonOrDie(data, hint)
		logging.Info("%s: %s has hint %v", n.logName, child, hint)
		n.hints[child] = hint
	}
}

func (n *NodeStats) LoadFromZookeeper(hubs *utils.HubHandle, adjustHubs bool) {
	n.loadNodes()
	n.loadHints()
	n.updateHubsInLoading(hubs, adjustHubs)
}
