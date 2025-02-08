package node_mgr

import (
	"encoding/json"
	"flag"
	"sync"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
)

var (
	flagDcBlackListKconf = flag.String("dc_blacklist_kconf", "", "dc_blacklist_kconf")
	dcBlackListInst      = dcBlackList{
		cfg: &dcBlackListConfigs{
			servicesMap: make(map[string]bool),
			dcMap:       make(map[string]bool),
		},
	}
	dcBlackListOnce sync.Once
)

type dcBlackListConfigs struct {
	Services    []string `json:"services"`
	Dcs         []string `json:"dcs"`
	servicesMap map[string]bool
	dcMap       map[string]bool
}

func (c *dcBlackListConfigs) initializeMap() {
	c.servicesMap = make(map[string]bool)
	for _, service := range c.Services {
		c.servicesMap[service] = true
	}
	c.dcMap = make(map[string]bool)
	for _, dc := range c.Dcs {
		c.dcMap[dc] = true
	}
}

func (c *dcBlackListConfigs) dcDisabled(serviceName, dcName string) bool {
	if _, ok := c.dcMap[dcName]; !ok {
		return false
	}
	if _, ok := c.servicesMap["*"]; ok {
		return true
	}
	_, ok := c.servicesMap[serviceName]
	return ok
}

type dcBlackList struct {
	// TODO(huyifan03): improve this with atomic of golang 1.19
	mu  sync.RWMutex
	cfg *dcBlackListConfigs
}

func (c *dcBlackList) updateConfigs(cfg *dcBlackListConfigs) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cfg = cfg
}

func (c *dcBlackList) getConfigs() *dcBlackListConfigs {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cfg
}

type configWatcher struct {
}

func (w *configWatcher) OnStringValueUpdated(key string, data string) error {
	newConfigs := &dcBlackListConfigs{}
	err := json.Unmarshal([]byte(data), newConfigs)
	if err != nil {
		logging.Warning("update kconf %s error: %v", key, err)
		return err
	}
	newConfigs.initializeMap()
	logging.Info(
		"dc black list kconf %v updated, service_list: %v, dcs: %v",
		key,
		newConfigs.Services,
		newConfigs.Dcs,
	)
	dcBlackListInst.updateConfigs(newConfigs)
	return nil
}

func (w *configWatcher) OnValueRemoved(key string) error {
	return nil
}

func DcBlackListInitialize() {
	dcBlackListOnce.Do(func() {
		defaultConfigs := &dcBlackListConfigs{}
		defaultConfigs.initializeMap()
		dcBlackListInst.updateConfigs(defaultConfigs)
		if *flagDcBlackListKconf != "" {
			err := third_party.ConfigAddStringWatcher(*flagDcBlackListKconf, &configWatcher{})
			logging.Info("set watch for %v result: %v", *flagDcBlackListKconf, err)
		}
	})
}

func GetDcBlackListConfigs() *dcBlackListConfigs {
	return dcBlackListInst.getConfigs()
}
