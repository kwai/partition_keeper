package utils

import (
	"testing"

	"gotest.tools/assert"
)

func TestHardwareUnit(t *testing.T) {
	h1 := HardwareUnit{}
	h1["cpu"] = 10

	h2 := HardwareUnit{}
	h2["mem"] = 20

	h1.Add(h2)
	assert.DeepEqual(t, h1, HardwareUnit{
		"cpu": 10,
		"mem": 20,
	})

	h2 = HardwareUnit{
		"cpu": 10,
		"mem": 10,
	}
	h1.Sub(h2)
	assert.DeepEqual(t, h1, HardwareUnit{
		"mem": 10,
	})

	h1.Add(HardwareUnit{
		"ssd": 20,
	})
	h1.Divide(2)
	assert.DeepEqual(t, h1, HardwareUnit{
		"mem": 5,
		"ssd": 10,
	})

	err := h1.EnoughFor(HardwareUnit{
		"mem": 5,
		"ssd": 5,
	})
	assert.Assert(t, err == nil)

	err = h1.EnoughFor(HardwareUnit{
		"mem": 10,
		"ssd": 5,
	})
	assert.Assert(t, err != nil)

	err = h1.EnoughFor(HardwareUnit{
		"cpu": 2,
	})
	assert.Assert(t, err != nil)

	h1.Upper(HardwareUnit{"cpu": 10})
	assert.DeepEqual(t, h1, HardwareUnit{
		"cpu": 10,
		"mem": 5,
		"ssd": 10,
	})

	h1.Upper(HardwareUnit{"cpu": 20})
	assert.DeepEqual(t, h1, HardwareUnit{
		"cpu": 20,
		"mem": 5,
		"ssd": 10,
	})

	assert.DeepEqual(t, h1, h1.Clone())

	var nilUnit HardwareUnit = nil
	assert.DeepEqual(t, nilUnit, nilUnit.Clone())

	emptyUnit := HardwareUnit{}
	assert.DeepEqual(t, emptyUnit, emptyUnit.Clone())
}

func TestHardwareUnits(t *testing.T) {
	h1 := HardwareUnits{}
	h1.AddUnit("yz_1", HardwareUnit{
		"cpu": 10,
		"mem": 20,
	})
	assert.DeepEqual(t, h1, HardwareUnits{
		"yz_1": HardwareUnit{
			"cpu": 10,
			"mem": 20,
		},
	})

	h1.AddUnit("yz_1", HardwareUnit{
		"cpu": 10,
		"mem": 20,
	})
	assert.DeepEqual(t, h1, HardwareUnits{
		"yz_1": HardwareUnit{
			"cpu": 20,
			"mem": 40,
		},
	})

	h1.RemoveUnit("yz_2", HardwareUnit{
		"cpu": 20,
		"mem": 40,
	})
	assert.DeepEqual(t, h1, HardwareUnits{
		"yz_1": HardwareUnit{
			"cpu": 20,
			"mem": 40,
		},
	})

	h1.RemoveUnit("yz_1", HardwareUnit{
		"cpu": 20,
	})
	assert.DeepEqual(t, h1, HardwareUnits{
		"yz_1": HardwareUnit{
			"mem": 40,
		},
	})

	h1.RemoveUnit("yz_1", HardwareUnit{
		"mem": 40,
	})
	assert.DeepEqual(t, h1, HardwareUnits{})
}
