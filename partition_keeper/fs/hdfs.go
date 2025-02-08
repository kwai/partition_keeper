package fs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

const (
	operateHdfsMaxTimes            = 5
	HdfsError_kOk           string = "ok"
	HdfsError_kFileNotExist string = "FileNotFoundException"
)

type RemoteException struct {
	Message       string `json:"message"`
	Exception     string `json:"exception"`
	JavaClassName string `json:"javaClassName"`
}

type ErrorStatus struct {
	Boolean         bool            `json:"boolean"`
	RemoteException RemoteException `json:"RemoteException"`
}

type FileStatus struct {
	PathSuffix       string `json:"pathSuffix"`
	Type             string `json:"type"`
	Length           int    `json:"length"`
	Owner            string `json:"owner"`
	Group            string `json:"group"`
	Permission       string `json:"permission"`
	AccessTime       int    `json:"accessTime"`
	ModificationTime int64  `json:"modificationTime"`
	BlockSize        int    `json:"blockSize"`
	Replication      int    `json:"replication"`
}

type ListDirectoryStatus struct {
	FileStatuses struct {
		FileStatus []FileStatus `json:"FileStatus"`
	} `json:"FileStatuses"`
	RemoteException RemoteException `json:"RemoteException"`
}

type GetFileStatus struct {
	FileStatus      FileStatus      `json:"FileStatus"`
	RemoteException RemoteException `json:"RemoteException"`
}

type Hdfs struct{}

func (h *Hdfs) generateURL(path, region string) string {
	url := "http://"
	hdfs := third_party.GetHdfsUrlFromRegion(region)
	if hdfs == "" {
		logging.Warning("Fail to get url by region : %s", region)
		return ""
	}
	url += hdfs + "/webhdfs/v1" + path
	return url
}

func (h *Hdfs) fillBasicParameters(op, user, region string) map[string]string {
	var args = make(map[string]string)
	args["op"] = op
	args["user.name"] = user

	return args
}

func (h *Hdfs) RecursiveMkdir(path, region, user string) bool {
	var result ErrorStatus
	retry := true
	args := h.fillBasicParameters("MKDIRS", user, region)

	url := h.generateURL(path, region)
	if url == "" {
		return false
	}

	checkError := func(httpResp *http.Response, response interface{}) error {
		err := json.NewDecoder(httpResp.Body).Decode(response)
		if err != nil {
			return err
		}

		result, ok := response.(*ErrorStatus)
		logging.Assert(ok, "")

		if httpResp.StatusCode == 200 && result.Boolean {
			return nil
		}

		return fmt.Errorf(
			"http status code:%d, message:%s",
			httpResp.StatusCode,
			result.RemoteException.Message,
		)
	}

	op := func() (bool, error) {
		startTime := time.Now().UnixNano() / 1e6
		err := utils.HttpPutStream(
			url,
			args,
			[]byte{},
			&result,
			checkError,
		)
		costTime := time.Now().UnixNano()/1e6 - startTime
		logging.Info("Mkdir hdfs path %s got result:%v, cost time:%d ms", path, err, costTime)
		return retry, err
	}

	return h.retry(op)
}

func (h *Hdfs) Create(path, region, user string, data []byte) bool {
	var result ErrorStatus
	retry := true
	args := h.fillBasicParameters("CREATE", user, region)
	args["data"] = "true"
	args["overwrite"] = "true"

	url := h.generateURL(path, region)
	if url == "" {
		return false
	}

	checkError := func(httpResp *http.Response, response interface{}) error {
		if httpResp.StatusCode == 201 {
			return nil
		}

		err := json.NewDecoder(httpResp.Body).Decode(response)
		if err != nil {
			return err
		}

		result, ok := response.(*ErrorStatus)
		logging.Assert(ok, "")
		return fmt.Errorf(
			"http status code:%d, message:%s",
			httpResp.StatusCode,
			result.RemoteException.Message,
		)
	}

	op := func() (bool, error) {
		startTime := time.Now().UnixNano() / 1e6
		err := utils.HttpPutStream(url, args, data, &result, checkError)
		costTime := time.Now().UnixNano()/1e6 - startTime
		logging.Info("Create hdfs file %s got result:%v, cost time:%d ms", path, err, costTime)
		return retry, err
	}

	return h.retry(op)
}

func (h *Hdfs) Append(path, region, user string, data []byte) bool {
	var result ErrorStatus
	retry := true
	args := h.fillBasicParameters("APPEND", user, region)
	args["data"] = "true"

	url := h.generateURL(path, region)
	if url == "" {
		return false
	}

	checkError := func(httpResp *http.Response, response interface{}) error {
		if httpResp.StatusCode == 200 {
			return nil
		}

		err := json.NewDecoder(httpResp.Body).Decode(response)
		if err != nil {
			return err
		}

		result, ok := response.(*ErrorStatus)
		logging.Assert(ok, "")

		if httpResp.StatusCode == 404 {
			// File does not exist
			retry = false
		}

		return fmt.Errorf(
			"http status code:%d, message:%s",
			httpResp.StatusCode,
			result.RemoteException.Message,
		)
	}

	op := func() (bool, error) {
		startTime := time.Now().UnixNano() / 1e6
		err := utils.HttpPostStream(url, args, data, &result, checkError)
		costTime := time.Now().UnixNano()/1e6 - startTime
		logging.Info("Append hdfs file %s got result:%v, cost time:%d ms", path, err, costTime)
		return retry, err
	}

	return h.retry(op)
}

func (h *Hdfs) RecursiveDelete(path, region, user string) bool {
	var result ErrorStatus
	retry := true
	args := h.fillBasicParameters("DELETE", user, region)
	args["recursive"] = "true"

	url := h.generateURL(path, region)
	if url == "" {
		return false
	}

	checkError := func(httpResp *http.Response, response interface{}) error {
		err := json.NewDecoder(httpResp.Body).Decode(response)
		if err != nil {
			return err
		}

		result, ok := response.(*ErrorStatus)
		logging.Assert(ok, "")

		if httpResp.StatusCode == 200 && result.Boolean {
			return nil
		}

		exist, ok := h.Exist(path, region, user)
		if ok && !exist {
			return nil
		}
		return fmt.Errorf(
			"http status code:%d, message:%s",
			httpResp.StatusCode,
			result.RemoteException.Message,
		)
	}

	op := func() (bool, error) {
		startTime := time.Now().UnixNano() / 1e6
		err := utils.HttpDelete(
			url,
			args,
			&result,
			checkError,
		)
		costTime := time.Now().UnixNano()/1e6 - startTime
		logging.Info("Delete hdfs path %s got result:%v, cost time:%d ms", path, err, costTime)
		return retry, err
	}

	return h.retry(op)
}

func (h *Hdfs) Rename(oldPath, newPath, region, user string) bool {
	var result ErrorStatus
	retry := true
	args := h.fillBasicParameters("RENAME", user, region)
	args["destination"] = newPath

	url := h.generateURL(oldPath, region)
	if url == "" {
		return false
	}

	checkError := func(httpResp *http.Response, response interface{}) error {
		err := json.NewDecoder(httpResp.Body).Decode(response)
		if err != nil {
			return err
		}
		result, ok := response.(*ErrorStatus)
		logging.Assert(ok, "")

		if httpResp.StatusCode == 200 && result.Boolean {
			return nil
		}

		if httpResp.StatusCode == 404 &&
			result.RemoteException.Exception == HdfsError_kFileNotExist {
			retry = false
		}

		return fmt.Errorf(
			"http status code:%d, message:%s, boolean:%t",
			httpResp.StatusCode,
			result.RemoteException.Message,
			result.Boolean,
		)
	}

	op := func() (bool, error) {
		startTime := time.Now().UnixNano() / 1e6
		err := utils.HttpPutStream(
			url,
			args,
			[]byte{},
			&result,
			checkError,
		)
		costTime := time.Now().UnixNano()/1e6 - startTime
		logging.Info(
			"Rename hdfs path %s to %s got result:%v, cost time:%d ms",
			oldPath,
			newPath,
			err,
			costTime,
		)
		return retry, err
	}

	return h.retry(op)
}

func (h *Hdfs) Read(path, region, user string) ([]byte, bool) {
	var httpResult ErrorStatus
	var result []byte
	retry := true
	args := h.fillBasicParameters("OPEN", user, region)

	url := h.generateURL(path, region)
	if url == "" {
		return []byte{}, false
	}

	checkError := func(httpResp *http.Response, response interface{}) error {
		if httpResp.StatusCode == 200 {
			result, _ = ioutil.ReadAll(httpResp.Body)
			return nil
		}

		err := json.NewDecoder(httpResp.Body).Decode(response)
		if err != nil {
			return err
		}
		errStatus, ok := response.(*ErrorStatus)
		logging.Assert(ok, "")

		if httpResp.StatusCode == 404 {
			retry = false
		}

		return fmt.Errorf(
			"http status code:%d, message:%s",
			httpResp.StatusCode,
			errStatus.RemoteException.Message,
		)
	}

	op := func() (bool, error) {
		startTime := time.Now().UnixNano() / 1e6
		err := utils.HttpGet(
			url,
			nil,
			args,
			&httpResult,
			checkError,
		)
		costTime := time.Now().UnixNano()/1e6 - startTime
		logging.Info("Read file %s got result:%v, cost time:%d ms", path, err, costTime)
		return retry, err
	}

	ans := h.retry(op)
	return result, ans
}

func (h *Hdfs) Exist(path, region, user string) (bool, bool) {
	var result GetFileStatus
	exist := false
	retry := true
	args := h.fillBasicParameters("GETFILESTATUS", user, region)

	url := h.generateURL(path, region)
	if url == "" {
		return false, false
	}

	checkError := func(httpResp *http.Response, response interface{}) error {
		if httpResp.StatusCode == 200 {
			exist = true
			return nil
		}

		err := json.NewDecoder(httpResp.Body).Decode(response)
		if err != nil {
			return err
		}
		result, ok := response.(*GetFileStatus)
		logging.Assert(ok, "")

		if httpResp.StatusCode == 404 &&
			result.RemoteException.Exception == HdfsError_kFileNotExist {
			return nil
		}

		return fmt.Errorf(
			"http status code:%d, message:%s",
			httpResp.StatusCode,
			result.RemoteException.Message,
		)
	}

	op := func() (bool, error) {
		startTime := time.Now().UnixNano() / 1e6
		err := utils.HttpGet(
			url,
			nil,
			args,
			&result,
			checkError,
		)
		costTime := time.Now().UnixNano()/1e6 - startTime
		logging.Info("Get file status %s got result:%v, cost time:%d ms", path, err, costTime)
		return retry, err
	}

	ans := h.retry(op)
	return exist, ans
}

func (h *Hdfs) ListDirectory(path, region, user string) (*ListDirectoryStatus, bool) {
	var result ListDirectoryStatus
	retry := true
	args := h.fillBasicParameters("LISTSTATUS", user, region)

	url := h.generateURL(path, region)
	if url == "" {
		return nil, false
	}

	checkError := func(httpResp *http.Response, response interface{}) error {
		err := json.NewDecoder(httpResp.Body).Decode(response)
		if err != nil {
			return err
		}
		result, ok := response.(*ListDirectoryStatus)
		logging.Assert(ok, "")

		if httpResp.StatusCode == 200 {
			return nil
		}

		if httpResp.StatusCode == 404 &&
			result.RemoteException.Exception == HdfsError_kFileNotExist {
			retry = false
		}

		return fmt.Errorf(
			"http status code:%d, message:%s",
			httpResp.StatusCode,
			result.RemoteException.Message,
		)
	}

	op := func() (bool, error) {
		startTime := time.Now().UnixNano() / 1e6
		err := utils.HttpGet(
			url,
			nil,
			args,
			&result,
			checkError,
		)
		costTime := time.Now().UnixNano()/1e6 - startTime
		logging.Info("list directory %s got result:%v, cost time:%d ms", path, err, costTime)
		return retry, err
	}

	ans := h.retry(op)
	return &result, ans
}

type RetryableFunc func() (bool, error)

func (h *Hdfs) retry(retryableFunc RetryableFunc) bool {
	tryCount := 1
	for tryCount < operateHdfsMaxTimes {
		retry, err := retryableFunc()
		if err == nil {
			return true
		}
		if !retry {
			return false
		}
		logging.Warning("operate hdfs got error %s with %d times(s)", err.Error(), tryCount)
		<-time.After(2 * time.Second)
		tryCount++
	}
	return false
}
