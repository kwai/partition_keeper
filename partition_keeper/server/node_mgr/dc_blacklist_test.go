package node_mgr

import (
	"testing"

	"gotest.tools/assert"
)

func TestDcBlackListConfig(t *testing.T) {
	cfg := &dcBlackListConfigs{}
	cfg.Services = []string{}
	cfg.Dcs = []string{}

	cfg.initializeMap()
	assert.Equal(t, cfg.dcDisabled("test", "test"), false)

	cfg.Services = []string{"*"}
	cfg.Dcs = []string{"XY", "MT"}
	cfg.initializeMap()

	assert.Equal(t, cfg.dcDisabled("test", "XY"), true)
	assert.Equal(t, cfg.dcDisabled("test2", "XY"), true)
	assert.Equal(t, cfg.dcDisabled("test", "ZEY"), false)

	cfg.Services = []string{"test"}
	cfg.Dcs = []string{"XY", "MT"}
	cfg.initializeMap()

	assert.Equal(t, cfg.dcDisabled("test", "XY"), true)
	assert.Equal(t, cfg.dcDisabled("test2", "XY"), false)
	assert.Equal(t, cfg.dcDisabled("test", "ZEY"), false)
}

func TestBlackListKconf(t *testing.T) {
	*flagDcBlackListKconf = "reco.partitionKeeper.testDcBlackList"
	DcBlackListInitialize()
	configs := GetDcBlackListConfigs()
	assert.DeepEqual(t, configs.servicesMap, map[string]bool{
		"test_service":  true,
		"test_service2": true,
	})
	assert.DeepEqual(t, configs.dcMap, map[string]bool{
		"TEST_DC1": true,
		"TEST_DC2": true,
	})

	assert.Equal(t, configs.dcDisabled("test_service", "TEST_DC1"), true)
	assert.Equal(t, configs.dcDisabled("test_service", "XY"), false)
}
