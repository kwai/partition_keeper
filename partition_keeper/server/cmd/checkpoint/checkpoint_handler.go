package checkpoint

import (
	"bytes"
	"container/list"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/fs"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	cmd_base "github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/dbg"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
)

const (
	kDirectory  = "DIRECTORY"
	kFile       = "FILE"
	kDoneMark   = "done"
	kPartPrefix = "part_"
)

type KConfValue struct {
	CheckpointPath string `json:"checkpoint_path"`
}

type taskFsWrapper struct {
	logName  string
	regions  []string
	user     string
	fsHandle fs.FileSystem
}

func (t *taskFsWrapper) create(path string, data []byte) bool {
	for _, region := range t.regions {
		if !t.fsHandle.Create(path, region, t.user, data) {
			logging.Warning("%s create %s for %s failed", t.logName, path, region)
			return false
		} else {
			logging.Info("%s create %s for %s succeed", t.logName, path, region)
		}
	}
	return true
}

func (t *taskFsWrapper) rename(src, dest string) bool {
	for _, region := range t.regions {
		if !t.fsHandle.Rename(src, dest, region, t.user) {
			logging.Warning("%s rename %s -> %s for %s failed", t.logName, src, dest, region)
			return false
		} else {
			logging.Info("%s rename %s -> %s for %s succeed", t.logName, src, dest, region)
		}
	}
	return true
}

func (t *taskFsWrapper) recursiveCreateDir(path string) bool {
	for _, region := range t.regions {
		if !t.fsHandle.RecursiveMkdir(path, region, t.user) {
			logging.Warning("%s create dir %s for %s failed", t.logName, path, region)
			return false
		} else {
			logging.Info("%s create dir %s for %s succeed", t.logName, path, region)
		}
	}
	return true
}

func (t *taskFsWrapper) recursiveDelete(path string) bool {
	for _, region := range t.regions {
		if !t.fsHandle.RecursiveDelete(path, region, t.user) {
			logging.Warning("%s delete %s for %s failed", t.logName, path, region)
			return false
		} else {
			logging.Info("%s delete %s for %s succeed", t.logName, path, region)
		}
	}
	return true
}

type fsLayoutValidator func(fs fs.FileSystem, region, user string) error

func (t *taskFsWrapper) validate(validator fsLayoutValidator) error {
	for _, region := range t.regions {
		err := validator(t.fsHandle, region, t.user)
		if err != nil {
			return err
		}
	}
	return nil
}

type CheckpointHandler struct {
	*cmd_base.HandlerParams
	logName string
}

func NewCheckpointHandler(params *cmd_base.HandlerParams) cmd_base.CmdHandler {
	return &CheckpointHandler{
		HandlerParams: params,
		logName: fmt.Sprintf(
			"%s-%s-%s",
			params.ServiceName,
			params.TableName,
			params.TaskName,
		),
	}
}

func (c *CheckpointHandler) getCheckpointFsContext() (fileSystem fs.FileSystem, prefix string, user string) {
	defer func() {
		if fileSystem == nil {
			third_party.PerfLog4(
				"reco",
				"reco.colossusdb.kconf_error",
				third_party.GetKsnNameByServiceName(c.ServiceName),
				c.ServiceName,
				c.TableName,
				c.TaskName,
				c.KconfPath,
				1,
			)
		}
	}()

	data, err := third_party.GetStringConfig(c.KconfPath)
	if err != nil {
		logging.Warning("%s get kconf:%s failed:%s", c.logName, c.KconfPath, err.Error())
		return nil, "", ""
	}

	value := &KConfValue{}
	err = json.Unmarshal([]byte(data), value)
	if err != nil {
		logging.Warning("%s unmarshal kconf:%s failed:%s", c.logName, c.KconfPath, err.Error())
		return nil, "", ""
	}
	if value.CheckpointPath == "" {
		logging.Warning("%s There is no checkpoint in the kconf:%s", c.logName, c.KconfPath)
		return nil, "", ""
	}

	fs, prefix, user := fs.GetFsContextByKconfPath(value.CheckpointPath)
	if user == "" {
		user = c.Args["hdfs_user"]
	}
	return fs, prefix, user
}

func (c *CheckpointHandler) getCheckpointPathFromExecInfo(
	prefix string,
	execInfo *pb.TaskExecInfo,
) (path string, restored, shared bool) {
	path = execInfo.Args["restore_path"]
	if len(path) > 0 {
		restored = true
		cm, err := parseFromCheckpointPath(path)
		if err != nil {
			shared = true
		} else {
			shared = (cm.prefix != prefix || cm.tableId != c.TableId)
		}
	} else {
		meta := &checkpointMeta{
			prefix:    prefix,
			tableId:   c.TableId,
			sessionId: execInfo.SessionId,
			timestamp: execInfo.StartSecond,
		}
		path = meta.concatSessionCheckpointPath()
		restored = false
		shared = false
	}
	return
}

func (c *CheckpointHandler) generateCheckpointList(prefix string, execHistory *list.List) []byte {
	// TODO(huyifan03): prefix may be changed as it is in kconf
	var buf bytes.Buffer
	for iter := execHistory.Front(); iter != nil; iter = iter.Next() {
		execInfo := iter.Value.(*pb.TaskExecInfo)
		if execInfo.FinishStatus != pb.TaskExecInfo_kFinished {
			logging.Info(
				"%s session %d is %s, skip it from checkpoint list",
				c.logName,
				execInfo.SessionId,
				execInfo.FinishStatus.String(),
			)
			continue
		}
		path, _, _ := c.getCheckpointPathFromExecInfo(prefix, execInfo)
		logging.Info("%s generate checkpoint path %d %s", c.logName, execInfo.SessionId, path)
		fmt.Fprintf(&buf, "%d %s\n", execInfo.SessionId, path)
	}
	return buf.Bytes()
}

func (c *CheckpointHandler) checkRestorePath(
	restorePath string,
	partsCount int,
	fsHandle fs.FileSystem,
	region, user string,
) error {
	lsStatus, succ := fsHandle.ListDirectory(restorePath, region, user)
	if !succ {
		return fmt.Errorf("ls %s failed, may not exist", restorePath)
	}
	if lsStatus == nil {
		return fmt.Errorf("ls %s failed, can't get status", restorePath)
	}

	hasDone := false
	partSlots := map[string]bool{}
	for _, fileStatus := range lsStatus.FileStatuses.FileStatus {
		if fileStatus.Type == kFile {
			if fileStatus.PathSuffix == kDoneMark {
				hasDone = true
			}
		} else if fileStatus.Type == kDirectory {
			if strings.HasPrefix(fileStatus.PathSuffix, kPartPrefix) {
				partId := fileStatus.PathSuffix[len(kPartPrefix):]
				partSlots[partId] = true
			}
		}
	}
	if !hasDone {
		return fmt.Errorf("can't detect done mark in %s", restorePath)
	}
	for i := 0; i < partsCount; i++ {
		partStr := fmt.Sprintf("%d", i)
		if _, ok := partSlots[partStr]; !ok {
			return fmt.Errorf("can't get part %s from %s", partStr, restorePath)
		}
	}
	return nil
}

func (c *CheckpointHandler) getRegions() []string {
	output := []string{}
	output = append(output, c.Regions...)
	return output
}

func (c *CheckpointHandler) CheckMimicArgs(args map[string]string, tablePb *pb.Table) error {
	fsHandle, _, user := c.getCheckpointFsContext()
	if fsHandle == nil {
		return fmt.Errorf(
			"can't get fs from kconf path: %s, maybe invalid kconf or no \"checkpoint_path\" in kconf",
			c.KconfPath,
		)
	}
	restorePath := args["restore_path"]
	fsWrapper := &taskFsWrapper{
		logName:  c.logName,
		regions:  c.getRegions(),
		user:     user,
		fsHandle: fsHandle,
	}

	checkPath := func(fsHandle fs.FileSystem, region, user string) error {
		return c.checkRestorePath(restorePath, int(tablePb.PartsCount), fsHandle, region, user)
	}
	return fsWrapper.validate(checkPath)
}

func (c *CheckpointHandler) BeforeExecutionDurable(execHistory *list.List) bool {
	logging.Assert(execHistory.Len() > 0, "%s: empty exec history", c.logName)
	recent := execHistory.Back().Value.(*pb.TaskExecInfo)
	if recent.FinishStatus == pb.TaskExecInfo_kCancelled {
		logging.Info(
			"%s: skip to update checkpoint path due to recent item is cancelled",
			c.logName,
		)
		return true
	}

	fs, path, user := c.getCheckpointFsContext()
	if fs == nil {
		logging.Warning("%s can't run before execution durable as get fs failed", c.logName)
		return false
	}
	logging.Assert(path != "", "")

	execInfo := execHistory.Back().Value.(*pb.TaskExecInfo)
	cm := &checkpointMeta{
		prefix:    path,
		tableId:   c.TableId,
		sessionId: execInfo.SessionId,
		timestamp: execInfo.StartSecond,
	}
	tablePath := cm.concatTableCheckpointPath()
	checkpointListPath := tablePath + "/checkpoint_list"
	stashPath := checkpointListPath + ".tmp"
	doneFilePath := cm.concatSessionCheckpointPath() + "/done"

	fsWrapper := &taskFsWrapper{
		logName:  c.logName,
		regions:  c.getRegions(),
		user:     user,
		fsHandle: fs,
	}
	if !fsWrapper.recursiveCreateDir(cm.concatSessionCheckpointPath()) {
		return false
	}
	if !fsWrapper.create(doneFilePath, []byte{}) {
		return false
	}
	if !fsWrapper.recursiveDelete(stashPath) {
		return false
	}
	checkpointListData := c.generateCheckpointList(path, execHistory)
	if !fsWrapper.create(stashPath, checkpointListData) {
		return false
	}
	if !fsWrapper.recursiveDelete(checkpointListPath) {
		return false
	}
	if !fsWrapper.rename(stashPath, checkpointListPath) {
		return false
	}
	return true
}

func (c *CheckpointHandler) BorrowFromHistory(
	execHistory *list.List,
	args map[string]string,
) int64 {
	if len(args) == 0 {
		return cmd_base.INVALID_SESSION_ID
	}
	path := args["restore_path"]
	if len(path) == 0 {
		return cmd_base.INVALID_SESSION_ID
	}
	meta, err := parseFromCheckpointPath(path)
	// TODO: check checkpoint path prefix
	if err != nil {
		return cmd_base.INVALID_SESSION_ID
	}
	if meta.tableId != c.TableId {
		return cmd_base.INVALID_SESSION_ID
	}
	for iter := execHistory.Front(); iter != nil; iter = iter.Next() {
		execInfo := iter.Value.(*pb.TaskExecInfo)
		if execInfo.SessionId == meta.sessionId &&
			execInfo.StartSecond == meta.timestamp {
			logging.Info(
				"%s borrow from history %v, remove it from execHistory",
				c.logName,
				execInfo,
			)
			ans := execInfo.SessionId
			execHistory.Remove(iter)
			return ans
		} else {
			logging.Verbose(1, "%s: skip %v as we want %v", c.logName, execInfo, meta)
		}
	}
	return cmd_base.INVALID_SESSION_ID
}

func (c *CheckpointHandler) CleanOneExecution(execInfo *pb.TaskExecInfo) bool {
	fileSystem, prefix, user := c.getCheckpointFsContext()
	if fileSystem == nil {
		return false
	}
	logging.Assert(prefix != "", "")
	checkpointPath, restored, shared := c.getCheckpointPathFromExecInfo(prefix, execInfo)
	if shared {
		logging.Info(
			"%s session %d's checkpoint path %s is shared restore path, skip to clean",
			c.logName,
			execInfo.SessionId,
			checkpointPath,
		)
		logging.Assert(restored, "")
		return true
	}

	for _, region := range c.getRegions() {
		hdfsUser := user
		if dbg.RunOnebox() {
			logging.Info(
				"[change_state] Mock onebox: successfully deleted checkpoint:%s from HDFS, region:%s",
				checkpointPath,
				region,
			)
			continue
		}
		if dbg.RunInSafeMode() {
			logging.Info(
				"[change_state] run in safe mode, skip to delete path %s region: %s user: %s",
				checkpointPath,
				region,
				hdfsUser,
			)
			return false
		}
		if !fileSystem.RecursiveDelete(checkpointPath, region, hdfsUser) {
			logging.Warning(
				"[change_state] Delete to HDFS fail, path : %s, region : %s",
				checkpointPath,
				region,
			)
			third_party.PerfLog3(
				"reco",
				"reco.colossusdb.hdfs_fail",
				third_party.GetKsnNameByServiceName(c.ServiceName),
				c.ServiceName,
				c.TableName,
				c.TaskName,
				1,
			)
			return false
		}
		logging.Info(
			"[change_state] Successfully deleted checkpoint:%s from HDFS, region:%s",
			checkpointPath,
			region,
		)
	}

	return true
}

func (c *CheckpointHandler) CleanAllExecutions() bool {
	fileSystem, prefix, user := c.getCheckpointFsContext()
	if fileSystem == nil {
		return false
	}
	meta := checkpointMeta{
		prefix:    prefix,
		tableId:   c.TableId,
		sessionId: cmd_base.INVALID_SESSION_ID,
		timestamp: cmd_base.INVALID_SESSION_ID,
	}
	tablePath := meta.concatTableCheckpointPath()

	logging.Info("start to clean path %s for region %v", tablePath, c.getRegions())
	for _, region := range c.getRegions() {
		hdfsUser := user
		if dbg.RunOnebox() {
			logging.Info(
				"[change_state] Mock onebox: successfully deleted table all checkpoint:%s from HDFS, region:%s",
				tablePath,
				region,
			)
			continue
		}
		if dbg.RunInSafeMode() {
			logging.Info(
				"[change_state] run in safe mode, skip to delete path %s region: %s user: %s",
				tablePath,
				region,
				hdfsUser,
			)
			return false
		}
		if !fileSystem.RecursiveDelete(tablePath, region, hdfsUser) {
			logging.Warning(
				"[change_state] Delete to HDFS fail, path : %s, region : %s",
				tablePath,
				region,
			)
			third_party.PerfLog3(
				"reco",
				"reco.colossusdb.hdfs_fail",
				third_party.GetKsnNameByServiceName(c.ServiceName),
				c.ServiceName,
				c.TableName,
				c.TaskName,
				1,
			)
			return false
		}
		logging.Info(
			"[change_state] Successfully deleted table all checkpoint:%s from HDFS, region:%s",
			tablePath,
			region,
		)
	}

	return true
}
