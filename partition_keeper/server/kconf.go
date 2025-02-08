package server

import (
	"encoding/json"
	"flag"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/dbg"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

var (
	flagKconfPath  = flag.String("kconf_path", "", "kconf path")
	currentConfigs = &dynamicConfigs{
		SafeMode:        false,
		VerboseLogLevel: 0,
	}
)

type dynamicConfigs struct {
	SafeMode        bool  `json:"safe_mode"`
	VerboseLogLevel int32 `json:"verbose_log_level"`
}

func updateValue(cfg *dynamicConfigs) {
	logging.SetVerboseLevel(cfg.VerboseLogLevel)
	dbg.SetSafeMode(int32(utils.Bool2Int(cfg.SafeMode)))
}

type configWatcher struct {
}

func (w *configWatcher) OnStringValueUpdated(key string, data string) error {
	newConfigs := &dynamicConfigs{}
	err := json.Unmarshal([]byte(data), newConfigs)
	if err != nil {
		logging.Warning("kconf updated error: %v", err)
		return err
	}
	updateValue(newConfigs)
	logging.Info("kconf updated from %v to %v", currentConfigs, newConfigs)
	currentConfigs = newConfigs
	return nil
}

func (w *configWatcher) OnValueRemoved(key string) error {
	return nil
}

func KconfInitialize() {
	updateValue(currentConfigs)
	if *flagKconfPath != "" {
		// kconf framework will ensure the OnXXXValueUpdated to be called
		// after we add a watcher
		err := third_party.ConfigAddStringWatcher(*flagKconfPath, &configWatcher{})
		logging.Info("set watch result: %v", err)
	}
}
