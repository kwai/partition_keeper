package checkpoint

import (
	"container/list"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/fs"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	cmd_base "github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"gotest.tools/assert"
)

func TestCheckpointJointHdfsPath(t *testing.T) {
	now := time.Now().Unix()
	info := cmd_base.HandlerParams{
		TableId:     1,
		ServiceName: "test",
		TableName:   "test",
		TaskName:    TASK_NAME,
		Regions:     []string{"STAGING"},
		KconfPath:   "reco.rodisFea.partitionKeeperHDFSTest",
		Args:        map[string]string{"a": "b"},
	}
	cleaner := CheckpointHandler{
		HandlerParams: &info,
	}

	// kconf does not exist
	cleaner.KconfPath = "not_exist_kconf"
	_, prefix, _ := cleaner.getCheckpointFsContext()
	assert.Equal(t, prefix, "")

	// no checkpoint in the kconf
	cleaner.KconfPath = "reco.rodisV1.ONEBOX_TEST"
	_, prefix, _ = cleaner.getCheckpointFsContext()
	assert.Equal(t, prefix, "")

	cleaner.KconfPath = "reco.rodisFea.partitionKeeperHDFSTest"
	_, prefix, _ = cleaner.getCheckpointFsContext()
	assert.Equal(t, prefix, "/tmp/colossusdb_test")

	// clean with not exist kconf
	info.KconfPath = "not_exist_kconf"
	checkpointCleaner := NewCheckpointHandler(&info)

	execInfo := &pb.TaskExecInfo{
		SessionId:    now,
		StartSecond:  now,
		FinishSecond: now,
	}
	assert.Equal(t, checkpointCleaner.CleanOneExecution(execInfo), false)
	assert.Equal(t, checkpointCleaner.CleanAllExecutions(), false)
}

func TestCheckpointClean(t *testing.T) {
	kconfPath := "reco.rodisFea.partitionKeeperHDFSTest"
	now := time.Now().Unix()
	var path, path1 string
	var fileSystem fs.Hdfs

	{
		data, err := third_party.GetStringConfig(kconfPath)
		assert.Assert(t, err == nil)

		value := &KConfValue{}
		err = json.Unmarshal([]byte(data), value)
		assert.Assert(t, err == nil)

		path = (value.CheckpointPath + "/" + "table_" +
			strconv.FormatInt(int64(1), 10) + "/" +
			strconv.FormatInt(now, 10) + "_" +
			strconv.FormatInt(now, 10))
		path1 = path + "_test"
		assert.Equal(t, fileSystem.RecursiveMkdir(path, "STAGING", ""), true)
		assert.Equal(t, fileSystem.RecursiveMkdir(path1, "STAGING", ""), true)
	}

	regions := []string{"STAGING"}
	info := cmd_base.HandlerParams{
		TableId:     1,
		ServiceName: "test",
		TableName:   "test",
		TaskName:    TASK_NAME,
		Regions:     regions,
		KconfPath:   kconfPath,
		Args:        map[string]string{"a": "b"},
	}
	checkpointCleaner := NewCheckpointHandler(&info)
	_, succ := fileSystem.ListDirectory(path, "STAGING", "")
	assert.Equal(t, succ, true)
	// hdfs user not exist
	info.Args["hdfs_user"] = "err_test"

	execInfo := &pb.TaskExecInfo{
		SessionId:    now,
		StartSecond:  now,
		FinishSecond: now,
	}
	assert.Equal(t, checkpointCleaner.CleanOneExecution(execInfo), false)
	assert.Equal(t, checkpointCleaner.CleanAllExecutions(), false)

	info.Args["hdfs_user"] = "reco"
	assert.Equal(t, checkpointCleaner.CleanOneExecution(execInfo), true)
	_, succ = fileSystem.ListDirectory(path, "STAGING", "")
	assert.Equal(t, succ, false)

	assert.Equal(t, checkpointCleaner.CleanAllExecutions(), true)
	_, succ = fileSystem.ListDirectory(path1, "STAGING", "")
	assert.Equal(t, succ, false)
}

func TestGetCheckpointPathFromExecInfo(t *testing.T) {
	prefix := "/tmp/partition_keeper_unit_test"

	taskEffectHandlerParams := &cmd_base.HandlerParams{
		TableId:     1001,
		ServiceName: "test",
		TableName:   "test",
		TaskName:    "checkpoint",
		Regions:     []string{"STAGING"},
		KconfPath:   "reco.rodisFea.partitionKeeperUnitTest",
		Args:        nil,
	}
	checkpointHandler := NewCheckpointHandler(taskEffectHandlerParams).(*CheckpointHandler)

	execInfo := &pb.TaskExecInfo{
		SessionId:    66666,
		StartSecond:  66666,
		FinishSecond: 66667,
		Args:         map[string]string{"restore_path": "/tmp/another_test/restored_path1"},
	}
	path, restored, shared := checkpointHandler.getCheckpointPathFromExecInfo(prefix, execInfo)
	assert.Equal(t, path, "/tmp/another_test/restored_path1")
	assert.Equal(t, restored, true)
	assert.Equal(t, shared, true)

	execInfo = &pb.TaskExecInfo{
		SessionId:    66666,
		StartSecond:  66666,
		FinishSecond: 66667,
		Args:         map[string]string{"restore_path": "/tmp/another_test/table_1001/6666_7777"},
	}
	path, restored, shared = checkpointHandler.getCheckpointPathFromExecInfo(prefix, execInfo)
	assert.Equal(t, path, "/tmp/another_test/table_1001/6666_7777")
	assert.Equal(t, restored, true)
	assert.Equal(t, shared, true)

	execInfo = &pb.TaskExecInfo{
		SessionId:    66666,
		StartSecond:  66666,
		FinishSecond: 66667,
		Args: map[string]string{
			"restore_path": "/tmp/partition_keeper_unit_test/table_1002/6666_7777",
		},
	}
	path, restored, shared = checkpointHandler.getCheckpointPathFromExecInfo(prefix, execInfo)
	assert.Equal(t, path, "/tmp/partition_keeper_unit_test/table_1002/6666_7777")
	assert.Equal(t, restored, true)
	assert.Equal(t, shared, true)

	execInfo = &pb.TaskExecInfo{
		SessionId:    66666,
		StartSecond:  66666,
		FinishSecond: 66667,
		Args: map[string]string{
			"restore_path": "/tmp/partition_keeper_unit_test/table_1001/6666_7777",
		},
	}
	path, restored, shared = checkpointHandler.getCheckpointPathFromExecInfo(prefix, execInfo)
	assert.Equal(t, path, "/tmp/partition_keeper_unit_test/table_1001/6666_7777")
	assert.Equal(t, restored, true)
	assert.Equal(t, shared, false)

	execInfo = &pb.TaskExecInfo{
		SessionId:    66666,
		StartSecond:  66666,
		FinishSecond: 66667,
		Args:         nil,
	}
	path, restored, shared = checkpointHandler.getCheckpointPathFromExecInfo(prefix, execInfo)
	assert.Equal(t, path, "/tmp/partition_keeper_unit_test/table_1001/66666_66666")
	assert.Equal(t, restored, false)
	assert.Equal(t, shared, false)
}

func TestCheckpointNotCleanShared(t *testing.T) {
	kconfContent := "local://web_server@/tmp/partition_keeper_unit_test"

	taskEffectHandlerParams := &cmd_base.HandlerParams{
		TableId:     1001,
		ServiceName: "test",
		TableName:   "test",
		TaskName:    "checkpoint",
		Regions:     []string{"STAGING"},
		KconfPath:   "reco.rodisFea.partitionKeeperUnitTest",
		Args:        nil,
	}
	checkpointHandler := NewCheckpointHandler(taskEffectHandlerParams)

	localFs, _, _ := fs.GetFsContextByKconfPath(kconfContent)
	path := "/tmp/partition_keeper_unit_test/table_1001/66666_66666"
	assert.Equal(t, localFs.RecursiveMkdir(path, "STAGING", ""), true)

	logging.Info("clean private checkpoint")
	execInfo := &pb.TaskExecInfo{
		SessionId:    66666,
		StartSecond:  66666,
		FinishSecond: 66667,
		Args:         nil,
	}
	ans := checkpointHandler.CleanOneExecution(execInfo)
	assert.Equal(t, ans, true)
	exists, succ := localFs.Exist(path, "STAGING", "")
	assert.Assert(t, succ && !exists)

	logging.Info("not clean shared checkpoint")
	path = "/tmp/partition_keeper_unit_test/table_1005/66666_66667"
	assert.Equal(t, localFs.RecursiveMkdir(path, "STAGING", ""), true)
	execInfo = &pb.TaskExecInfo{
		SessionId:    66666,
		StartSecond:  66666,
		FinishSecond: 66667,
		Args: map[string]string{
			"restore_path": "/tmp/partition_keeper_unit_test/table_1005/66666_66667",
		},
	}
	ans = checkpointHandler.CleanOneExecution(execInfo)
	assert.Equal(t, ans, true)
	exists, succ = localFs.Exist(path, "STAGING", "")
	assert.Assert(t, succ && exists)
}

func TestCheckpointHandlerBorrowExecutionHistory(t *testing.T) {
	params := &cmd_base.HandlerParams{
		TableId:     1001,
		ServiceName: "test",
		TableName:   "test",
		TaskName:    "checkpoint",
		Regions:     []string{"HB1"},
		KconfPath:   "reco.rodisFea.partitionKeeperHDFSTest",
		Args:        map[string]string{},
	}

	handler := NewCheckpointHandler(params)

	execHistory := list.New()
	execHistory.PushBack(&pb.TaskExecInfo{
		StartSecond:  1001,
		FinishSecond: 1002,
		SessionId:    1001,
		Args:         nil,
		FinishStatus: pb.TaskExecInfo_kFinished,
	})
	execHistory.PushBack(&pb.TaskExecInfo{
		StartSecond:  2001,
		FinishSecond: 2002,
		SessionId:    2001,
		Args:         nil,
		FinishStatus: pb.TaskExecInfo_kFinished,
	})

	ans := handler.BorrowFromHistory(execHistory, map[string]string{})
	assert.Equal(t, ans, int64(cmd_base.INVALID_SESSION_ID))

	ans = handler.BorrowFromHistory(
		execHistory,
		map[string]string{"restore_path": "/tmp/checkpoint/user_generated_path"},
	)
	assert.Equal(t, ans, int64(cmd_base.INVALID_SESSION_ID))

	ans = handler.BorrowFromHistory(
		execHistory,
		map[string]string{"restore_path": "/tmp/checkpoint/table_1/123_123"},
	)
	assert.Equal(t, ans, int64(cmd_base.INVALID_SESSION_ID))

	ans = handler.BorrowFromHistory(
		execHistory,
		map[string]string{"restore_path": "/tmp/checkpoint/table_1001/123_123"},
	)
	assert.Equal(t, ans, int64(cmd_base.INVALID_SESSION_ID))

	ans = handler.BorrowFromHistory(
		execHistory,
		map[string]string{"restore_path": "/tmp/checkpoint/table_1001/2001_2001"},
	)
	assert.Equal(t, ans, int64(2001))
	assert.Equal(t, execHistory.Len(), 1)

	info := execHistory.Front().Value.(*pb.TaskExecInfo)
	assert.Equal(t, info.SessionId, int64(1001))
}

func TestCheckpointHandlerBeforeExecDurable(t *testing.T) {
	params := &cmd_base.HandlerParams{
		TableId:     1001,
		ServiceName: "test",
		TableName:   "test",
		TaskName:    "checkpoint",
		Regions:     []string{"STAGING"},
		KconfPath:   "reco.rodisFea.partitionKeeperHDFSTest",
		Args:        map[string]string{},
	}
	handler := NewCheckpointHandler(params)

	execHistory := list.New()
	execHistory.PushBack(&pb.TaskExecInfo{
		StartSecond:  1001,
		FinishSecond: 1002,
		SessionId:    1001,
		Args:         nil,
		FinishStatus: pb.TaskExecInfo_kFinished,
	})
	execHistory.PushBack(&pb.TaskExecInfo{
		StartSecond:  2001,
		FinishSecond: 2002,
		SessionId:    2001,
		Args:         nil,
		FinishStatus: pb.TaskExecInfo_kCancelled,
	})
	execHistory.PushBack(&pb.TaskExecInfo{
		StartSecond:  3001,
		FinishSecond: 3002,
		SessionId:    3001,
		Args:         map[string]string{"restore_path": "/tmp/colossusdb/table_202/12345_6789"},
		FinishStatus: pb.TaskExecInfo_kFinished,
	})

	filesystem := &fs.Hdfs{}
	path := "/tmp/colossusdb_test/table_1001/checkpoint_list"

	logging.Info("create checkpoint list from empty")
	ans := handler.BeforeExecutionDurable(execHistory)
	assert.Assert(t, ans)

	data, ok := filesystem.Read(path, "STAGING", "")
	assert.Assert(t, ok)
	expect_data := `1001 /tmp/colossusdb_test/table_1001/1001_1001
3001 /tmp/colossusdb/table_202/12345_6789
`
	assert.Equal(t, string(data), expect_data)

	logging.Info("append item to checkpoint list")
	execHistory.PushBack(&pb.TaskExecInfo{
		StartSecond:  4001,
		FinishSecond: 4002,
		SessionId:    4001,
		Args:         nil,
		FinishStatus: pb.TaskExecInfo_kFinished,
	})
	ans = handler.BeforeExecutionDurable(execHistory)
	assert.Assert(t, ans)

	data, ok = filesystem.Read(path, "STAGING", "")
	assert.Assert(t, ok)
	expect_data = `1001 /tmp/colossusdb_test/table_1001/1001_1001
3001 /tmp/colossusdb/table_202/12345_6789
4001 /tmp/colossusdb_test/table_1001/4001_4001
`
	assert.Equal(t, string(data), expect_data)

	logging.Info("add cancelled execInfo will have no effect")
	execHistory.PushBack(&pb.TaskExecInfo{
		StartSecond:  5001,
		FinishSecond: 5002,
		SessionId:    5001,
		Args:         nil,
		FinishStatus: pb.TaskExecInfo_kCancelled,
	})
	ans = handler.BeforeExecutionDurable(execHistory)
	assert.Assert(t, ans)

	data, ok = filesystem.Read(path, "STAGING", "")
	assert.Assert(t, ok)
	expect_data = `1001 /tmp/colossusdb_test/table_1001/1001_1001
3001 /tmp/colossusdb/table_202/12345_6789
4001 /tmp/colossusdb_test/table_1001/4001_4001
`
	assert.Equal(t, string(data), expect_data)
}

func TestCheckpointHandlerCheckMimicArgs(t *testing.T) {
	params := &cmd_base.HandlerParams{
		TableId:     1001,
		ServiceName: "test",
		TableName:   "test",
		TaskName:    "checkpoint",
		Regions:     []string{"HB1"},
		KconfPath:   "reco.rodisFea.notExistPath",
		Args:        map[string]string{},
	}
	handler := NewCheckpointHandler(params)

	path := "/tmp/local_mimicked_checkpoint_path"
	args := map[string]string{
		"restore_path": path,
	}
	localFs, _, _ := fs.GetFsContextByKconfPath("local://web_server@/dummy")
	succ := localFs.RecursiveDelete(path, "HB1", "")
	assert.Equal(t, succ, true)

	tablePb := &pb.Table{PartsCount: 4}
	logging.Info("can't get kconf so check failed")
	err := handler.CheckMimicArgs(args, tablePb)
	assert.Assert(t, err != nil)

	logging.Info("can't get restore path")
	params.KconfPath = "reco.rodisFea.partitionKeeperUnitTest"
	handler = NewCheckpointHandler(params)
	err = handler.CheckMimicArgs(args, tablePb)
	assert.Assert(t, err != nil)

	logging.Info("restore path is not dir")
	succ = localFs.Create(path, "HB1", "", []byte("hello"))
	assert.Equal(t, succ, true)
	err = handler.CheckMimicArgs(args, tablePb)
	assert.Assert(t, err != nil)

	logging.Info("restore path don't have done mark")
	succ = localFs.RecursiveDelete(path, "HB1", "")
	assert.Equal(t, succ, true)
	succ = localFs.RecursiveMkdir(path, "HB1", "")
	assert.Equal(t, succ, true)
	err = handler.CheckMimicArgs(args, tablePb)
	assert.Assert(t, err != nil)

	logging.Info("restore path's partitions smaller than expect")
	succ = localFs.Create(path+"/done", "HB1", "", []byte(""))
	assert.Equal(t, succ, true)
	for i := 0; i < 2; i++ {
		partPath := fmt.Sprintf("%s/part_%d", path, i)
		succ = localFs.RecursiveMkdir(partPath, "HB1", "")
		assert.Equal(t, succ, true)
	}
	err = handler.CheckMimicArgs(args, tablePb)
	assert.Assert(t, err != nil)

	logging.Info("restore path check succeed")
	for i := 2; i < int(tablePb.PartsCount); i++ {
		partPath := fmt.Sprintf("%s/part_%d", path, i)
		succ = localFs.RecursiveMkdir(partPath, "HB1", "")
		assert.Equal(t, succ, true)
	}
	err = handler.CheckMimicArgs(args, tablePb)
	assert.Assert(t, err == nil)
}

// func TestCheckpointHandlerCheckMimicArgsRealHdfs(t *testing.T) {
//	params := &TaskEffectHandlerParams{
//		TableId:     1001,
//		ServiceName: "test",
//		TableName:   "test",
//		TaskName:    "checkpoint",
//		Regions:     []string{"HB1"},
//		KconfPath:   "reco.rodisFea.partitionKeeperHDFSTest",
//		Args:        map[string]string{},
//	}
//	handler := NewCheckpointHandler(params)
//	logging.Info("please make sure this dir won't be deleted, as this is an online valid path")
//	path := "/home/reco_6/colossusdb_embd/reco_test/table_28/1641643201_1641643201"
//	args := map[string]string{
//		"restore_path": path,
//	}
//	tablePb := &pb.Table{
//		PartsCount: 128,
//	}
//	err := handler.CheckMimicArgs(args, tablePb)
//	assert.NilError(t, err)
//
//	tablePb.PartsCount = 256
//	err = handler.CheckMimicArgs(args, tablePb)
//	assert.NilError(t, err)
//
//	tablePb.PartsCount = 512
//	err = handler.CheckMimicArgs(args, tablePb)
//	assert.Assert(t, err != nil)
//}
