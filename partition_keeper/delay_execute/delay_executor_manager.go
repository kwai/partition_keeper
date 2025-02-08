package delay_execute

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type ExecutorCreator func(data []byte) ExecutorFunc

var (
	executorTypeMap = map[string]ExecutorCreator{}
)

type ExecuteInfo struct {
	ExecuteType    string      `json:"execute_type"`
	ExecuteName    string      `json:"execute_name"`
	ExecuteTime    int64       `json:"execute_time"`
	ExecuteContext interface{} `json:"execute_context"`
}

type DelayedExecutorManager struct {
	mu                 sync.Mutex
	quit               chan bool
	metaStore          metastore.MetaStore
	zkPath             string
	delayedExecutorMap map[string]*DelayedExecutor
}

func RegisterExecutorCreatorByType(executorType string, executorCreator ExecutorCreator) {
	executorTypeMap[executorType] = executorCreator
}

func NewDelayedExecutorManager(zkStore metastore.MetaStore, zkPath string) *DelayedExecutorManager {
	ans := &DelayedExecutorManager{
		quit:               make(chan bool),
		metaStore:          zkStore,
		zkPath:             zkPath,
		delayedExecutorMap: make(map[string]*DelayedExecutor),
	}
	return ans
}

func (d *DelayedExecutorManager) InitFromZookeeper() {
	children, exists, succ := d.metaStore.Children(context.Background(), d.zkPath)
	logging.Assert(succ, "")
	if exists {
		for _, child := range children {
			executorPath := d.GetExecutorZkPath(child)
			data, exists, succ := d.metaStore.Get(context.Background(), executorPath)
			logging.Assert(exists && succ, "")

			executorInfo := ExecuteInfo{}
			utils.UnmarshalJsonOrDie(data, &executorInfo)
			d.AddTask(executorInfo)
		}
	} else {
		succ := d.metaStore.RecursiveCreate(context.Background(), d.zkPath)
		logging.Assert(succ, "")
	}
	go d.delayedExecutorLoop()
}

func (d *DelayedExecutorManager) GetExecutorZkPath(path string) string {
	return fmt.Sprintf("%s/%s", d.zkPath, path)
}

func (d *DelayedExecutorManager) Stop() {
	logging.Info("start to stop delayed executor manager")
	for _, task := range d.delayedExecutorMap {
		task.Stop()
	}
	d.quit <- true
	logging.Info("delayed executor manager has stopped")
}

func (d *DelayedExecutorManager) AddTask(executeInfo ExecuteInfo) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if executorTypeMap[executeInfo.ExecuteType] == nil {
		logging.Info("This type:%s of execute is not registered", executeInfo.ExecuteType)
		return false
	}

	if d.delayedExecutorMap[executeInfo.ExecuteName] != nil {
		logging.Info(
			"Fail to create delayed executor:%s, because it already exists",
			executeInfo.ExecuteName,
		)
		return false
	}

	taskZkPath := d.GetExecutorZkPath(executeInfo.ExecuteName)
	executeInfoJs := utils.MarshalJsonOrDie(executeInfo)

	succ := d.metaStore.Create(context.Background(), taskZkPath, executeInfoJs)
	logging.Assert(succ, "")

	executorCreator := executorTypeMap[executeInfo.ExecuteType]
	logging.Assert(executorCreator != nil, "")
	executorFunc := executorCreator(executeInfoJs)

	executor := NewDelayedExecutor(executeInfo.ExecuteTime, executorFunc)
	executor.Start()
	d.delayedExecutorMap[executeInfo.ExecuteName] = executor
	logging.Info(
		"Successfully adding a delay executor. task name:%s, task type:%s",
		executeInfo.ExecuteName,
		executeInfo.ExecuteType,
	)
	return true
}

func (d *DelayedExecutorManager) checkExecutorState() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for name, executor := range d.delayedExecutorMap {
		if executor.State() == utils.StateDropped {
			taskZkPath := d.GetExecutorZkPath(name)
			executor.Stop()
			succ := d.metaStore.Delete(context.Background(), taskZkPath)
			logging.Assert(succ, "")
			delete(d.delayedExecutorMap, name)
			logging.Info("Successful execution:%s", name)
		}
	}
}

func (d *DelayedExecutorManager) ExecutorExist(executorName string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	executor := d.delayedExecutorMap[executorName]
	return executor != nil
}

func (d *DelayedExecutorManager) delayedExecutorLoop() {
	logging.Info("start delayed executor loop")
	tick := time.NewTicker(*FlagScheduleExecuteDuration)
	for {
		select {
		case <-tick.C:
			d.checkExecutorState()
		case <-d.quit:
			tick.Stop()
			return
		}
	}
}
