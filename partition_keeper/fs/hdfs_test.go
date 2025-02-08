package fs

import (
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"gotest.tools/assert"
)

func TestGenerateURL(t *testing.T) {
	fileSystem := Hdfs{}
	url := fileSystem.generateURL(utils.SpliceZkRootPath("/tmp/keeper/ut_test/url"), "HD")
	assert.Assert(t, url == "")
	url = fileSystem.generateURL(utils.SpliceZkRootPath("/tmp/keeper/ut_test/url"), "HB1")
	assert.Assert(t, url != "")
}

func TestHdfs(t *testing.T) {
	fileSystem, _, _ := GetFsContextByKconfPath("/tmp/partition_keeper_test")
	regions := []string{"STAGING"}
	for _, region := range regions {

		rootPath := "/tmp/partition_keeper_test" + utils.SpliceZkRootPath("/ut_test")
		assert.Equal(t, fileSystem.RecursiveDelete(rootPath, region, ""), true)

		_, ok := fileSystem.ListDirectory(rootPath, region, "")
		assert.Equal(t, ok, false)
		// mkdir
		assert.Equal(t, fileSystem.RecursiveMkdir(rootPath+"/mkdir1", "HD", ""), false)
		exist, ok := fileSystem.Exist(rootPath+"/mkdir1", region, "")
		assert.Equal(t, ok, true)
		assert.Equal(t, exist, false)

		for i := 0; i < 2; i++ {
			assert.Equal(t, fileSystem.RecursiveMkdir(rootPath+"/mkdir1", region, ""), true)
			exist, ok = fileSystem.Exist(rootPath+"/mkdir1", region, "")
			assert.Equal(t, ok && exist, true)
		}

		assert.Equal(t, fileSystem.RecursiveMkdir(rootPath+"/mkdir2", region, ""), true)
		dir, ok := fileSystem.ListDirectory(rootPath, region, "")
		assert.Equal(t, ok, true)
		assert.Equal(t, len(dir.FileStatuses.FileStatus), 2)

		// create
		assert.Equal(
			t,
			fileSystem.Create(rootPath+"/mkdir1/test1.txt", region, "err_user", []byte("hello")),
			false,
		)

		assert.Equal(
			t,
			fileSystem.Create(rootPath+"/mkdir1/test1.txt", region, "", []byte("hello")),
			true,
		)
		_, ok = fileSystem.Read(rootPath+"/mkdir1/test_not_exist.txt", region, "")
		assert.Equal(t, ok, false)

		data, ok := fileSystem.Read(rootPath+"/mkdir1/test1.txt", region, "")
		assert.Equal(t, ok, true)
		s := string(data)
		assert.Assert(t, s == "hello")

		assert.Equal(
			t,
			fileSystem.Create(rootPath+"/mkdir3/test1.txt", region, "", []byte("hello")),
			true,
		)
		dir, ok = fileSystem.ListDirectory(rootPath, region, "")
		assert.Equal(t, ok, true)
		assert.Equal(t, len(dir.FileStatuses.FileStatus), 3)

		assert.Equal(
			t,
			fileSystem.Create(rootPath+"/mkdir1/test1.txt", region, "", []byte("world")),
			true,
		)
		data, ok = fileSystem.Read(rootPath+"/mkdir1/test1.txt", region, "")
		assert.Equal(t, ok, true)
		s = string(data)
		assert.Assert(t, s == "world")

		// append
		assert.Equal(
			t,
			fileSystem.Append(rootPath+"/mkdir1/test1.txt", region, "", []byte("test")),
			true,
		)
		data, ok = fileSystem.Read(rootPath+"/mkdir1/test1.txt", region, "")
		assert.Equal(t, ok, true)
		s = string(data)
		assert.Assert(t, s == "worldtest")

		assert.Equal(
			t,
			fileSystem.Append(rootPath+"/mkdir1/test2.txt", region, "", []byte("test")),
			false,
		)

		// rename
		assert.Equal(
			t,
			fileSystem.Rename(
				rootPath+"/mkdir3/test1.txt",
				rootPath+"/mkdir3/test2.txt",
				region,
				"",
			),
			true,
		)
		exist, ok = fileSystem.Exist(rootPath+"/mkdir3/test1.txt", region, "")
		assert.Equal(t, ok, true)
		assert.Equal(t, exist, false)
		exist, ok = fileSystem.Exist(rootPath+"/mkdir3/test2.txt", region, "")
		assert.Equal(t, ok && exist, true)

		assert.Equal(
			t,
			fileSystem.Rename(
				rootPath+"/mkdir3/test2.txt",
				rootPath+"/mkdir4/test3.txt",
				region,
				"",
			),
			false,
		)
		exist, ok = fileSystem.Exist(rootPath+"/mkdir3/test2.txt", region, "")
		assert.Equal(t, ok && exist, true)

		assert.Equal(t, fileSystem.Rename(rootPath+"/mkdir2", rootPath+"/mkdir4", region, ""), true)
		exist, ok = fileSystem.Exist(rootPath+"/mkdir2", region, "")
		assert.Equal(t, ok, true)
		assert.Equal(t, exist, false)
		exist, ok = fileSystem.Exist(rootPath+"/mkdir4", region, "")
		assert.Equal(t, ok && exist, true)

		// delete
		assert.Equal(t, fileSystem.RecursiveDelete(rootPath+"/mkdir3", region, "err_user"), false)
		exist, ok = fileSystem.Exist(rootPath+"/mkdir3", region, "")
		assert.Equal(t, ok && exist, true)

		assert.Equal(t, fileSystem.RecursiveDelete(rootPath+"/mkdir3", region, ""), true)
		dir, ok = fileSystem.ListDirectory(rootPath, region, "")
		assert.Equal(t, ok, true)
		assert.Equal(t, len(dir.FileStatuses.FileStatus), 2)

		assert.Equal(t, fileSystem.RecursiveDelete(rootPath+"/mkdir1/test1.txt", region, ""), true)
		exist, ok = fileSystem.Exist(rootPath+"/mkdir1/test1.txt", region, "")
		assert.Equal(t, ok, true)
		assert.Equal(t, exist, false)
		dir, ok = fileSystem.ListDirectory(rootPath, region, "")
		assert.Equal(t, ok, true)
		assert.Equal(t, len(dir.FileStatuses.FileStatus), 2)

		assert.Equal(
			t,
			fileSystem.RecursiveDelete(rootPath+"/mkdir1/test_not_exist.txt", region, ""),
			true,
		)
		dir, ok = fileSystem.ListDirectory(rootPath, region, "")
		assert.Equal(t, ok, true)
		assert.Equal(t, len(dir.FileStatuses.FileStatus), 2)

		// clean
		assert.Equal(t, fileSystem.RecursiveDelete(rootPath, region, ""), true)
	}
}
