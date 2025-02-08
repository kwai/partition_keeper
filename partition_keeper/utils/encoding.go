package utils

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"reflect"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
)

func MarshalJsonOrDie(ptr interface{}) []byte {
	data, err := json.Marshal(ptr)
	if err != nil {
		logging.Fatal("marshal %v to json failed: %v", ptr, err.Error())
	}
	return data
}

func UnmarshalJsonOrDie(data []byte, ptr interface{}) {
	err := json.Unmarshal(data, ptr)
	if err != nil {
		logging.Fatal("unmarshal json %v to %s failed", string(data), reflect.TypeOf(ptr).String())
	}
}

func GobClone(out, in interface{}) {
	var buf bytes.Buffer

	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(in); err != nil {
		logging.Fatal("encode %v failed: %s", reflect.TypeOf(in).String(), err.Error())
	}

	decoder := gob.NewDecoder(&buf)
	if err := decoder.Decode(out); err != nil {
		logging.Fatal("decode %v failed: %s", reflect.TypeOf(out).String(), err.Error())
	}
}
