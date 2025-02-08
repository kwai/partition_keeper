package fs

import (
	"strings"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
)

type FileSystem interface {
	RecursiveMkdir(path, region, user string) bool
	Create(path, region, user string, data []byte) bool
	Append(path, region, user string, data []byte) bool
	Rename(oldPath, newPath, region, user string) bool
	RecursiveDelete(path, region, user string) bool

	Read(path, region, user string) ([]byte, bool)
	Exist(path, region, user string) (bool, bool)
	ListDirectory(path, region, user string) (*ListDirectoryStatus, bool)
}

type FsType int32

const (
	FsType_invalid FsType = 0
	FsType_hdfs    FsType = 1
	FsType_local   FsType = 2
)

var (
	FileSystemFactory = map[FsType]FileSystem{}
)

func init() {
	FileSystemFactory[FsType_hdfs] = &Hdfs{}
	FileSystemFactory[FsType_local] = &LocalFs{}
}

func GetFsContextByKconfPath(path string) (FileSystem, string, string) {
	path_start := strings.Index(path, "@")
	if path_start == -1 {
		// Compatible with older paths
		return FileSystemFactory[FsType_hdfs], path, ""
	}

	checkpoint_path := path[path_start+1:]
	local_prefix := "local://"
	hdfs_prefix := "hdfs://"
	if strings.HasPrefix(path, local_prefix) {
		return FileSystemFactory[FsType_local], checkpoint_path, path[len(local_prefix):path_start]
	} else if strings.HasPrefix(path, "hdfs://") {
		return FileSystemFactory[FsType_hdfs], checkpoint_path, path[len(hdfs_prefix):path_start]
	} else {
		logging.Warning("The checkpoint path in the kconf is wrong:%s", path)
		return nil, "", ""
	}
}
