package delay_execute

import (
	"context"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

func executorCallback() bool {
	return true
}
func executorCreator(data []byte) ExecutorFunc {
	return executorCallback
}

func TestDelayExecutor(t *testing.T) {
	*FlagScheduleExecuteDuration = 1 * time.Second
	now := time.Now().Unix()
	executor := NewDelayedExecutor(now+1, executorCallback)
	executor.Start()
	time.Sleep(2 * time.Second)
	assert.Assert(t, executor.State() == utils.StateDropped)
	executor.Stop()
	*FlagScheduleExecuteDuration = 60 * time.Second
}

func TestDelayExecutorManager(t *testing.T) {
	*FlagScheduleExecuteDuration = 1 * time.Second
	acls := zk.WorldACL(zk.PermRead)
	digest := zk.DigestACL(zk.PermAll, "keeper", "admin")
	acls = append(acls, digest...)

	zkPath := utils.SpliceZkRootPath("/test/pk/delay_executor")
	zkStore := metastore.CreateZookeeperStore(
		[]string{"127.0.0.1:2181"},
		time.Second*10,
		acls,
		"digest",
		[]byte("keeper"+":"+"admin"),
	)

	succ := zkStore.RecursiveDelete(context.Background(), zkPath)
	assert.Assert(t, succ)

	RegisterExecutorCreatorByType("checkpoint", executorCreator)
	manager := NewDelayedExecutorManager(zkStore, zkPath+"/delayed_executor")
	manager.InitFromZookeeper()

	now := time.Now().Unix()
	info := ExecuteInfo{
		ExecuteType:    "checkpoint",
		ExecuteName:    "table1_checkpoint",
		ExecuteTime:    now + 1,
		ExecuteContext: "test_context",
	}

	assert.Equal(t, manager.ExecutorExist(info.ExecuteName), false)
	// type is not registered
	info.ExecuteType = "checkpoint99"
	succ = manager.AddTask(info)
	assert.Equal(t, succ, false)
	info.ExecuteType = "checkpoint"
	succ = manager.AddTask(info)
	assert.Equal(t, succ, true)
	assert.Equal(t, manager.ExecutorExist(info.ExecuteName), true)

	// executor already exists
	succ = manager.AddTask(info)
	assert.Equal(t, succ, false)

	time.Sleep(3 * time.Second)
	assert.Equal(t, manager.ExecutorExist(info.ExecuteName), false)

	// load from zk
	succ = manager.AddTask(info)
	assert.Equal(t, succ, true)
	assert.Equal(t, manager.ExecutorExist(info.ExecuteName), true)
	manager.Stop()
	manager = NewDelayedExecutorManager(zkStore, zkPath+"/delayed_executor")
	manager.InitFromZookeeper()
	assert.Equal(t, manager.ExecutorExist(info.ExecuteName), true)
	time.Sleep(4 * time.Second)
	assert.Equal(t, manager.ExecutorExist(info.ExecuteName), false)

	// clean
	manager.Stop()
	succ = zkStore.RecursiveDelete(context.Background(), zkPath)
	assert.Assert(t, succ)
	zkStore.Close()
	*FlagScheduleExecuteDuration = 60 * time.Second
}
