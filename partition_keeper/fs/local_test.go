package fs

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

func TestLocal(t *testing.T) {
	fileSystem, _, _ := GetFsContextByKconfPath("local://web_server@/tmp/partition_keeper_test")
	rootPath := "/tmp/partition_keeper_test" + utils.SpliceZkRootPath("/ut_test")
	assert.Equal(t, fileSystem.RecursiveDelete(rootPath, "", ""), true)

	// mkdir
	for i := 0; i < 2; i++ {
		assert.Equal(t, fileSystem.RecursiveMkdir(rootPath+"/mkdir1", "", ""), true)
		exist, ok := fileSystem.Exist(rootPath+"/mkdir1", "", "")
		assert.Equal(t, ok && exist, true)
	}

	assert.Equal(t, fileSystem.RecursiveMkdir(rootPath+"/mkdir2", "", ""), true)
	dir, ok := fileSystem.ListDirectory(rootPath, "", "")
	assert.Equal(t, ok, true)
	assert.Equal(t, len(dir.FileStatuses.FileStatus), 2)

	// create
	assert.Equal(t, fileSystem.Create(rootPath+"/mkdir1/test1.txt", "", "", []byte("hello")), true)
	data, ok := fileSystem.Read(rootPath+"/mkdir1/test1.txt", "", "")
	assert.Equal(t, ok, true)
	s := string(data)
	assert.Assert(t, s == "hello")

	assert.Equal(
		t,
		fileSystem.Create(rootPath+"/mkdir1/test1.txt", "", "", []byte("world")),
		true,
	)
	data, ok = fileSystem.Read(rootPath+"/mkdir1/test1.txt", "", "")
	assert.Equal(t, ok, true)
	s = string(data)
	assert.Assert(t, s == "world")

	// append
	assert.Equal(
		t,
		fileSystem.Append(rootPath+"/mkdir1/test1.txt", "", "", []byte("test")),
		true,
	)
	data, ok = fileSystem.Read(rootPath+"/mkdir1/test1.txt", "", "")
	assert.Equal(t, ok, true)
	s = string(data)
	assert.Assert(t, s == "worldtest")

	assert.Equal(
		t,
		fileSystem.Append(rootPath+"/mkdir1/test2.txt", "", "", []byte("test")),
		false,
	)

	// rename
	assert.Equal(
		t,
		fileSystem.Rename(rootPath+"/mkdir1/test1.txt", rootPath+"/mkdir2/test1.txt", "", ""),
		true,
	)
	exist, ok := fileSystem.Exist(rootPath+"/mkdir1/test1.txt", "", "")
	assert.Equal(t, ok, true)
	assert.Equal(t, exist, false)
	exist, ok = fileSystem.Exist(rootPath+"/mkdir2/test1.txt", "", "")
	assert.Equal(t, ok && exist, true)

	assert.Equal(t, fileSystem.Rename(rootPath+"/mkdir1", rootPath+"/mkdir3", "", ""), true)
	exist, ok = fileSystem.Exist(rootPath+"/mkdir1", "", "")
	assert.Equal(t, ok, true)
	assert.Equal(t, exist, false)
	exist, ok = fileSystem.Exist(rootPath+"/mkdir3", "", "")
	assert.Equal(t, ok && exist, true)

	// delete
	assert.Equal(t, fileSystem.RecursiveDelete(rootPath+"/mkdir2", "", ""), true)
	dir, ok = fileSystem.ListDirectory(rootPath, "", "")
	assert.Equal(t, ok, true)
	assert.Equal(t, len(dir.FileStatuses.FileStatus), 1)

	assert.Equal(t, fileSystem.RecursiveDelete(rootPath+"/mkdir3/test1.txt", "", ""), true)
	exist, ok = fileSystem.Exist(rootPath+"/mkdir3/test1.txt", "", "")
	assert.Equal(t, ok, true)
	assert.Equal(t, exist, false)
	dir, ok = fileSystem.ListDirectory(rootPath, "", "")
	assert.Equal(t, ok, true)
	assert.Equal(t, len(dir.FileStatuses.FileStatus), 1)

	assert.Equal(
		t,
		fileSystem.RecursiveDelete(rootPath+"/mkdir1/test_not_exist.txt", "", ""),
		true,
	)
	dir, ok = fileSystem.ListDirectory(rootPath, "", "")
	assert.Equal(t, ok, true)
	assert.Equal(t, len(dir.FileStatuses.FileStatus), 1)

	// clean
	assert.Equal(t, fileSystem.RecursiveDelete(rootPath, "", ""), true)
}
