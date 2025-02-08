package sd

import (
	"testing"

	"gotest.tools/assert"
)

func TestDirectUrlDiscovery(t *testing.T) {
	usePazs := []bool{false, true}
	for _, usePaz := range usePazs {
		sd := NewServiceDiscovery(SdTypeDirectUrl, "rodisRecoUC", DirectSdUsePaz(usePaz)).(*directUrlDiscovery)
		nodes, err := sd.GetNodes()
		assert.Equal(t, len(nodes), 0)
		assert.NilError(t, err)

		azs := map[string]bool{"YZ": true, "BR": true}
		if usePaz {
			azs = map[string]bool{"HB1AZ1": true, "BR": true}
		}
		err = sd.UpdateAzs(azs)
		assert.Assert(t, err != nil)

		nodes, err = sd.GetNodes()
		assert.Equal(t, len(nodes), 0)
		assert.NilError(t, err)

		azs = map[string]bool{"YZ": true, "ZW": true}
		if usePaz {
			azs = map[string]bool{"HB1AZ1": true, "HB1AZ2": true}
		}
		err = sd.UpdateAzs(azs)
		assert.DeepEqual(t, sd.wantAzs, azs)
		assert.NilError(t, err)

		nodes, err = sd.GetNodes()
		assert.NilError(t, err)
		assert.Assert(t, len(nodes) > 0)

		azCount := map[string]int{}
		for _, node := range nodes {
			assert.Assert(t, len(node.Location) > 0)
			az := KessLocationAz(node.Location)
			if usePaz {
				az = node.Paz
			}
			azCount[az]++
		}
		assert.Equal(t, len(azCount), 2)

		azs = map[string]bool{"YZ": true}
		if usePaz {
			azs = map[string]bool{"HB1AZ1": true}
		}
		err = sd.UpdateAzs(azs)
		assert.DeepEqual(t, sd.wantAzs, azs)
		assert.NilError(t, err)

		nodes, err = sd.GetNodes()
		assert.NilError(t, err)
		assert.Assert(t, len(nodes) > 0)

		azCount = map[string]int{}
		for _, node := range nodes {
			az := ""
			if usePaz {
				assert.Assert(t, len(node.Paz) > 0)
				az = node.Paz
			} else {
				assert.Assert(t, len(node.Location) > 0)
				az = KessLocationAz(node.Location)
			}
			azCount[az]++
		}
		assert.Equal(t, len(azCount), 1)
		if usePaz {
			assert.Assert(t, azCount["HB1AZ1"] > 0)
			assert.Equal(t, azCount["HB1AZ2"], 0)
		} else {
			assert.Assert(t, azCount["YZ"] > 0)
			assert.Equal(t, azCount["ZW"], 0)
		}

		sd2 := NewServiceDiscovery(SdTypeDirectUrl, "rodisNotExists", DirectSdUsePaz(usePaz))
		azs = map[string]bool{"YZ": true, "ZW": true}
		if usePaz {
			azs = map[string]bool{"HB1AZ1": true, "HB1AZ2": true}
		}
		err = sd2.UpdateAzs(azs)
		assert.NilError(t, err)

		nodes, err = sd2.GetNodes()
		assert.Assert(t, err != nil)
		assert.Equal(t, len(nodes), 0)
	}
}

func TestSGPDirectUrlDiscovery(t *testing.T) {
	usePazs := []bool{false, true}
	for _, usePaz := range usePazs {
		sd := NewServiceDiscovery(SdTypeDirectUrl, "rodisKsibData", DirectSdUsePaz(usePaz)).(*directUrlDiscovery)
		nodes, err := sd.GetNodes()
		assert.Equal(t, len(nodes), 0)
		assert.NilError(t, err)

		azs := map[string]bool{"TXSGP1": true, "BR": true}
		if usePaz {
			azs = map[string]bool{"SGPAZ1": true, "BR": true}
		}
		err = sd.UpdateAzs(azs)
		assert.Assert(t, err != nil)

		nodes, err = sd.GetNodes()
		assert.Equal(t, len(nodes), 0)
		assert.NilError(t, err)

		azs = map[string]bool{"TXSGP1": true, "GCPSGPC": true}
		if usePaz {
			azs = map[string]bool{"SGPAZ1": true, "SGPAZ2": true}
		}
		err = sd.UpdateAzs(azs)
		assert.DeepEqual(t, sd.wantAzs, azs)
		assert.NilError(t, err)

		nodes, err = sd.GetNodes()
		assert.NilError(t, err)
		assert.Assert(t, len(nodes) > 0)

		azCount := map[string]int{}
		for _, node := range nodes {
			assert.Assert(t, len(node.Location) > 0)
			az := KessLocationAz(node.Location)
			if usePaz {
				az = node.Paz
			}
			azCount[az]++
		}
		assert.Equal(t, len(azCount), 2)

		azs = map[string]bool{"TXSGP1": true}
		if usePaz {
			azs = map[string]bool{"SGPAZ1": true}
		}
		err = sd.UpdateAzs(azs)
		assert.DeepEqual(t, sd.wantAzs, azs)
		assert.NilError(t, err)

		nodes, err = sd.GetNodes()
		assert.NilError(t, err)
		assert.Assert(t, len(nodes) > 0)

		azCount = map[string]int{}
		for _, node := range nodes {
			az := ""
			if usePaz {
				assert.Assert(t, len(node.Paz) > 0)
				az = node.Paz
			} else {
				assert.Assert(t, len(node.Location) > 0)
				az = KessLocationAz(node.Location)
			}
			azCount[az]++
		}
		assert.Equal(t, len(azCount), 1)
		if usePaz {
			assert.Assert(t, azCount["SGPAZ1"] > 0)
			assert.Equal(t, azCount["SGPAZ2"], 0)
		} else {
			assert.Assert(t, azCount["TXSGP1"] > 0)
			assert.Equal(t, azCount["GCPSGPC"], 0)
		}

		sd2 := NewServiceDiscovery(SdTypeDirectUrl, "rodisNotExists", DirectSdUsePaz(usePaz))
		azs = map[string]bool{"TXSGP1": true, "GCPSGPC": true}
		if usePaz {
			azs = map[string]bool{"SGPAZ1": true, "SGPAZ2": true}
		}
		err = sd2.UpdateAzs(azs)
		assert.NilError(t, err)

		nodes, err = sd2.GetNodes()
		assert.Assert(t, err != nil)
		assert.Equal(t, len(nodes), 0)
	}
}
func TestKessSgpWorkaround(t *testing.T) {
	sd := newDirectUrlSD("recoUniversalDiscovery", false)
	sd.UpdateAzs(map[string]bool{"TXSGP1": true})

	nodes, err := sd.GetNodes()
	assert.NilError(t, err)
	assert.Assert(t, len(nodes) > 0)
}

func TestUseast(t *testing.T) {
	sd := newDirectUrlSD("recoUniversalDiscovery", false)
	sd.UpdateAzs(map[string]bool{"TXVA2": true})

	nodes, err := sd.GetNodes()
	assert.NilError(t, err)
	assert.Equal(t, len(nodes), 2)
}

func TestKwsInfo(t *testing.T) {
	sd := newDirectUrlSD("recoUniversalDiscovery", false)
	sd.UpdateAzs(map[string]bool{"YZ": true})

	nodes, err := sd.GetNodes()
	assert.NilError(t, err)
	assert.Assert(t, len(nodes) > 0)
	for _, node := range nodes {
		assert.Equal(t, node.Kws.Region, "HB1")
		assert.Equal(t, node.Kws.Az, "YZ")
		assert.Assert(t, len(node.Kws.Dc) > 0)
	}

	sd.UpdateAzs(map[string]bool{"STAGING": true})
	nodes, err = sd.GetNodes()
	assert.NilError(t, err)
	assert.Assert(t, len(nodes) > 0)
	for _, node := range nodes {
		assert.Equal(t, node.Kws.Region, "STAGING")
		assert.Equal(t, node.Kws.Az, "STAGING")
		assert.Equal(t, node.Kws.Dc, "STAGING")
	}
}
