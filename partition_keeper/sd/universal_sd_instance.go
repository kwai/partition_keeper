package sd

import (
	"flag"
	"sort"
	"sync"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
)

var (
	universalDiscoveryOnce    sync.Once
	universalDiscoveryPazOnce sync.Once
	universalInst             *UniversalDiscovery
	universalPazInst          *UniversalDiscovery
	flagUpdateSecs            = flag.Int64(
		"univeralsd_update_interval_secs",
		600,
		"global discovery update interval secs",
	)
	flagUniversalSdService = flag.String(
		"universalsd_service_name",
		"recoUniversalDiscovery",
		"global discovery service name",
	)
	flagUniversalPazSdService = flag.String(
		"universalsd_paz_service_name",
		"recoUniversalPazDiscovery",
		"global discovery paz service name",
	)
)

type UniversalDiscovery struct {
	sd ServiceDiscovery

	mu            sync.RWMutex
	updateRound   int
	updateCond    *sync.Cond
	azRefs        map[string]int
	sdServers     map[string][]*pb.ServiceNode
	updateTrigger chan bool
	quit          chan bool
	usePaz        bool
}

func newUniversalDiscovery(givenDirectUrl string, usePaz bool) *UniversalDiscovery {
	var directSd ServiceDiscovery
	sdService := *flagUniversalSdService
	if usePaz {
		sdService = *flagUniversalPazSdService
	}
	if givenDirectUrl == "" {
		directSd = NewServiceDiscovery(
			SdTypeDirectUrl,
			sdService,
			DirectSdUsePaz(usePaz),
		)
	} else {
		directSd = NewServiceDiscovery(SdTypeDirectUrl, sdService, DirectSdGivenUrl(givenDirectUrl), DirectSdUsePaz(usePaz))
	}

	ans := &UniversalDiscovery{
		sd:            directSd,
		updateRound:   0,
		azRefs:        make(map[string]int),
		sdServers:     make(map[string][]*pb.ServiceNode),
		updateTrigger: make(chan bool, 1),
		quit:          make(chan bool),
		usePaz:        usePaz,
	}
	ans.updateCond = sync.NewCond(&ans.mu)
	go ans.startPoller()
	ans.triggerUpdate()
	return ans
}

func GetUniversalDiscoveryInstance(usePaz bool) *UniversalDiscovery {
	if usePaz {
		universalDiscoveryPazOnce.Do(func() {
			universalPazInst = newUniversalDiscovery("", true)
		})
		return universalPazInst
	} else {
		universalDiscoveryOnce.Do(func() {
			universalInst = newUniversalDiscovery("", false)
		})
		return universalInst
	}
}

func (c *UniversalDiscovery) Stop() {
	c.quit <- true
}

func (c *UniversalDiscovery) WaitUpdateRoundBeyond(r int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.updateRound <= r {
		c.updateCond.Wait()
	}
}

func (c *UniversalDiscovery) GetDiscoveryRound() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.updateRound
}

func (c *UniversalDiscovery) updateDiscoveryServers() {
	nodes, err := c.sd.GetNodes()

	c.mu.Lock()
	defer func() {
		c.updateRound++
		c.updateCond.Signal()
		c.mu.Unlock()
	}()

	if err != nil {
		logging.Warning("universal discovery update servers failed: %s", err.Error())
		return
	}

	servers := map[string][]*pb.ServiceNode{}
	for _, node := range nodes {
		nodeAz := ""
		if c.usePaz {
			nodeAz = node.Paz
		} else {
			nodeAz = KessLocationAz(node.Location)
		}
		logging.Info("universal discovery got node: %s@%s", node.Id, nodeAz)
		servers[nodeAz] = append(servers[nodeAz], node)
	}
	for _, nodes := range servers {
		sort.Slice(nodes, func(i, j int) bool {
			left, right := nodes[i], nodes[j]
			return left.Id < right.Id
		})
	}
	c.sdServers = servers
}

func (c *UniversalDiscovery) getCurrentAzSet() map[string]bool {
	ans := map[string]bool{}
	for az := range c.azRefs {
		ans[az] = true
	}
	return ans
}

func (c *UniversalDiscovery) refAzs(azs map[string]bool) {
	for az := range azs {
		c.azRefs[az]++
	}
}

func (c *UniversalDiscovery) unrefAzs(azs map[string]bool) {
	for az := range azs {
		c.azRefs[az]--
		logging.Assert(c.azRefs[az] >= 0, "")
		if c.azRefs[az] == 0 {
			delete(c.azRefs, az)
		}
	}
}

func (c *UniversalDiscovery) updateAzs(oldAz, newAz map[string]bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.unrefAzs(oldAz)
	c.refAzs(newAz)
	if err := c.sd.UpdateAzs(c.getCurrentAzSet()); err != nil {
		c.unrefAzs(newAz)
		c.refAzs(oldAz)
		return err
	}
	return nil
}

func (c *UniversalDiscovery) UpdateAzs(oldAz, newAz map[string]bool) error {
	logging.Info("universal discovery update az from %v to %v", oldAz, newAz)
	if err := c.updateAzs(oldAz, newAz); err != nil {
		return err
	}
	c.triggerUpdate()
	return nil
}

func (c *UniversalDiscovery) triggerUpdate() {
	select {
	case c.updateTrigger <- true:
		logging.Info("trigger universal discovery update node list")
	default:
		logging.Verbose(1, "universal discovery has been triggered")
	}
}

// if we have more than one discovery servers, we'd better ensure every call to
// GetNode to have the ability to return exactly the same node. otherwise, the user
// of UniversalDiscovery may use different server to do service discovery,
// which may lead to inconsistent result as the KESS system
// can't guarantee external-consistency
//
// TestCascadeDiscovery/inconsistent_servers shows this case
func (c *UniversalDiscovery) GetNode(az string, token uint32) *pb.ServiceNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := c.sdServers[az]
	if len(nodes) == 0 {
		logging.Warning("can't find any node for region %s", az)
		return nil
	}

	return nodes[token%uint32(len(nodes))]
}

func (c *UniversalDiscovery) startPoller() {
	ticker := time.NewTicker(time.Second * time.Duration(*flagUpdateSecs))
	for {
		select {
		case <-ticker.C:
			c.updateDiscoveryServers()
		case <-c.updateTrigger:
			c.updateDiscoveryServers()
		case <-c.quit:
			ticker.Stop()
			return
		}
	}
}
