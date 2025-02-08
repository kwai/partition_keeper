package utils

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
)

func GetReqFromHttpBody(r *http.Request, ptr interface{}) *pb.ErrorStatus {
	if r.Body == nil {
		return pb.AdminErrorMsg(pb.AdminError_kEmptyHttpBody, "can't find http body")
	}
	data, _ := ioutil.ReadAll(r.Body)
	err := json.Unmarshal(data, ptr)
	if err != nil {
		return pb.AdminErrorMsg(pb.AdminError_kHttpDecodeFailed, "decode err: %v", err.Error())
	}
	return nil
}

func GetStringFromUrl(r *http.Request, name string) (string, *pb.ErrorStatus) {
	ans := r.URL.Query().Get(name)
	if ans == "" {
		return "", pb.AdminErrorMsg(
			pb.AdminError_kHttpDecodeFailed,
			"can't get %s from url %v",
			name,
			r.URL.Query(),
		)
	} else {
		return ans, nil
	}
}

func GetIntFromUrl(r *http.Request, name string) (int64, *pb.ErrorStatus) {
	ans, err := GetStringFromUrl(r, name)
	if err != nil {
		return 0, err
	}
	val, e := strconv.ParseInt(ans, 10, 64)
	if e != nil {
		return 0, pb.AdminErrorMsg(pb.AdminError_kHttpDecodeFailed,
			"can't parse %s as integer for %s in %v: %s",
			ans, name, r.URL.Query(), e.Error())
	}
	return val, nil
}

func GetBoolFromUrl(r *http.Request, name string) bool {
	ans, _ := GetStringFromUrl(r, name)
	if ans == "" {
		return false
	}
	return ans == "true"
}
