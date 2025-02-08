package fs

import (
	"io/ioutil"
	"os"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
)

type LocalFs struct{}

func (l *LocalFs) RecursiveMkdir(path, region, user string) bool {
	err := os.MkdirAll(path, 0755)
	logging.Info("MkdirAll local path:%s got result:%v", path, err)
	if err == nil {
		return true
	} else {
		return false
	}
}

func (l *LocalFs) Create(path, region, user string, data []byte) bool {
	file, err := os.Create(path)
	if err != nil {
		logging.Info("Create local file:%s got result:%v", path, err)
		return false
	}
	defer file.Close()

	if data != nil {
		n, err := file.Write(data)
		logging.Info("Create local file:%s got result:%v", path, err)
		if err != nil || n != len(data) {
			logging.Info(
				"Fail to create and append local file:%s, result:%v, write len:%d, data len:%d",
				path,
				err,
				n,
				len(data),
			)
			return false
		} else {
			return true
		}
	}

	return true
}

func (l *LocalFs) Append(path, region, user string, data []byte) bool {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0755)
	if err != nil {
		logging.Info("Append local file:%s got result:%v", path, err)
		return false
	}
	defer file.Close()

	n, err := file.Write(data)
	logging.Info("Append local file:%s got result:%v", path, err)
	if err != nil || n != len(data) {
		logging.Info(
			"Fail to append local file:%s, result:%v, write len:%d, data len:%d",
			path,
			err,
			n,
			len(data),
		)
		return false
	} else {
		return true
	}
}

func (l *LocalFs) RecursiveDelete(path, region, user string) bool {
	err := os.RemoveAll(path)
	logging.Info("RemoveAll local path:%s got result:%v", path, err)
	if err == nil {
		return true
	} else {
		return false
	}
}

func (l *LocalFs) Rename(oldPath, newPath, region, user string) bool {
	err := os.Rename(oldPath, newPath)
	logging.Info("Rename local path:%s to %s got result:%v", oldPath, newPath, err)
	if err == nil {
		return true
	} else {
		return false
	}
}

func (l *LocalFs) Read(path, region, user string) ([]byte, bool) {
	file, err := os.Open(path)
	if err != nil {
		logging.Info("Read local file:%s got result:%v", path, err)
		return []byte{}, false
	}
	defer file.Close()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		logging.Info("Read local file:%s got result:%v", path, err)
		return []byte{}, false
	}

	return data, true
}

func (l *LocalFs) Exist(path, region, user string) (bool, bool) {
	exist, succ := false, false
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			exist = false
			succ = true
		}
	} else {
		exist = true
		succ = true
	}
	return exist, succ
}

func (l *LocalFs) ListDirectory(path, region, user string) (*ListDirectoryStatus, bool) {
	var result ListDirectoryStatus
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, false
	}

	for _, file := range files {
		var fileStatus FileStatus
		fileStatus.PathSuffix = file.Name()
		if file.IsDir() {
			fileStatus.Type = "DIRECTORY"
		} else {
			fileStatus.Type = "FILE"
		}
		fileStatus.Length = int(file.Size())
		fileStatus.ModificationTime = file.ModTime().Local().Unix()

		result.FileStatuses.FileStatus = append(result.FileStatuses.FileStatus, fileStatus)
	}
	return &result, true
}
