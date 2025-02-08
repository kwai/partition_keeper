package delay_execute

import (
	"flag"
	"sync"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

var (
	FlagScheduleExecuteDuration = flag.Duration(
		"schedule_execute_interval_secs",
		60*time.Second,
		"schedule execute interval secs",
	)
)

type ExecutorFunc func() bool

type DelayedExecutor struct {
	state        utils.DroppableStateHolder
	mu           sync.Mutex
	quit         chan bool
	executeTime  int64
	executorFunc ExecutorFunc
}

func NewDelayedExecutor(
	executeTime int64,
	executorFunc ExecutorFunc,
) *DelayedExecutor {
	result := &DelayedExecutor{
		state:        utils.DroppableStateHolder{State: utils.StateInitializing},
		quit:         make(chan bool),
		executeTime:  executeTime,
		executorFunc: executorFunc,
	}
	return result
}

func (d *DelayedExecutor) Start() {
	d.state.Set(utils.StateNormal)
	go d.executeLoop()
}

func (d *DelayedExecutor) Stop() {
	logging.Info("start stop delayed executor")
	d.quit <- true
	logging.Info("finish stop delayed executor")
}

func (d *DelayedExecutor) State() utils.DroppableState {
	return d.state.Get()
}

func (d *DelayedExecutor) tryExecute() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state.Get() == utils.StateDropped {
		return
	}
	now := time.Now().Unix()
	if d.executeTime > now {
		return
	}

	if d.executorFunc() {
		logging.Info("The executor function was successfully executed")
		d.state.Set(utils.StateDropped)
	} else {
		logging.Warning("Failed to execute the function, execution will continue")
	}
}
func (d *DelayedExecutor) executeLoop() {
	logging.Info("start delay execute schedule loop")
	tick := time.NewTicker(*FlagScheduleExecuteDuration)
	for {
		select {
		case <-tick.C:
			d.tryExecute()
		case <-d.quit:
			tick.Stop()
			return
		}
	}
}
