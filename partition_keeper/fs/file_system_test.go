package fs

import (
	"testing"

	"gotest.tools/assert"
)

func TestGetFsContext(t *testing.T) {
	type CheckResult struct {
		path    string
		user    string
		fs_type FsType
	}
	paths := map[string]CheckResult{
		"hdfs://kwai@/home/test":  {path: "/home/test", user: "kwai", fs_type: FsType_hdfs},
		"hdfs:/kwai@/home/test":   {path: "", user: "", fs_type: FsType_invalid},
		"hdfs1:/kwai@/home/test":  {path: "", user: "", fs_type: FsType_invalid},
		"/home/test":              {path: "/home/test", user: "", fs_type: FsType_hdfs},
		"local:/home/test":        {path: "local:/home/test", user: "", fs_type: FsType_hdfs},
		"local://kwai@/home/test": {path: "/home/test", user: "kwai", fs_type: FsType_local},
	}
	for k, v := range paths {
		fs, path, user := GetFsContextByKconfPath(k)
		assert.Assert(t, path == v.path)
		assert.Assert(t, user == v.user)
		if path == "" {
			assert.Equal(t, fs, nil)
		} else {
			assert.Assert(t, fs == FileSystemFactory[v.fs_type])
		}
	}
}
