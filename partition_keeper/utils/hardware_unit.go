package utils

import (
	"fmt"
)

// standard resource
const (
	CPU       = "CPU"
	KSAT_CPU  = "KSAT_CPU"
	MEM_CAP   = "MEM_CAP"
	DISK_CAP  = "DISK_CAP"
	DISK_BAND = "DISK_BAND"
	NET_BAND  = "NET_BAND"
)

type HardwareUnit map[string]int64

func (h HardwareUnit) Add(h2 HardwareUnit) {
	for name, val := range h2 {
		h[name] += val
	}
}

func (h HardwareUnit) Upper(h2 HardwareUnit) {
	for name, val := range h2 {
		if h[name] < val {
			h[name] = val
		}
	}
}

func (h HardwareUnit) Sub(h2 HardwareUnit) {
	for name, val := range h2 {
		current := h[name]
		if current <= val {
			delete(h, name)
		} else {
			h[name] = current - val
		}
	}
}

func (h HardwareUnit) Divide(parts int64) {
	for name, val := range h {
		h[name] = (val + parts - 1) / parts
	}
}

func (h HardwareUnit) EnoughFor(h2 HardwareUnit) error {
	for name, val := range h2 {
		contain := h[name]
		if contain < val {
			return fmt.Errorf("not enough %s, has %d, need %d", name, contain, val)
		}
	}
	return nil
}

func (h HardwareUnit) Clone() HardwareUnit {
	if h == nil {
		return nil
	}
	output := HardwareUnit{}
	for name, val := range h {
		output[name] = val
	}
	return output
}

type HardwareUnits map[string]HardwareUnit

func (h HardwareUnits) AddUnit(name string, h2 HardwareUnit) {
	if hu, ok := h[name]; ok {
		hu.Add(h2)
	} else {
		hu := HardwareUnit{}
		hu.Add(h2)
		h[name] = hu
	}
}

func (h HardwareUnits) RemoveUnit(name string, h2 HardwareUnit) {
	if hu, ok := h[name]; ok {
		hu.Sub(h2)
		if len(hu) == 0 {
			delete(h, name)
		}
	}
}
