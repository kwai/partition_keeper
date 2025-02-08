package utils

import (
	"sync"
)

type DroppableState int32

const (
	StateInitializing DroppableState = iota
	StateNormal
	StateDropping
	StateDropped
)

var stateRep = []string{
	"initializing",
	"normal",
	"dropping",
	"dropped",
}

func (s DroppableState) String() string {
	if int32(s) > int32(StateDropped) {
		return "unknown"
	}
	return stateRep[int32(s)]
}

type DroppableStateHolder struct {
	mu    sync.Mutex
	State DroppableState
}

func (d *DroppableStateHolder) Set(new DroppableState) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.State = new
}

func (d *DroppableStateHolder) Get() DroppableState {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.State
}

func (d *DroppableStateHolder) Cas(old, new DroppableState) (current DroppableState, swapped bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.State == old {
		d.State = new
		return new, true
	} else {
		return d.State, false
	}
}
