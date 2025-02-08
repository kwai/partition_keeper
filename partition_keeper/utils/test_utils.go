package utils

import (
	"bytes"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"

	"github.com/emirpasic/gods/maps/treemap"
	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
)

func HubsEqual(l []*pb.ReplicaHub, r []*pb.ReplicaHub) bool {
	if len(l) != len(r) {
		logging.Info("left count: %d, right count: %d", len(l), len(r))
		return false
	}
	for i, lh := range l {
		if !proto.Equal(lh, r[i]) {
			logging.Info("left: %v, right: %v", lh, r[i])
			return false
		}
	}
	return true
}

func TreemapEqual(m1, m2 *treemap.Map) bool {
	if m1.Size() != m2.Size() {
		return false
	}
	iter1 := m1.Iterator()
	iter2 := m2.Iterator()

	iter1.Begin()
	iter2.Begin()

	for {
		has1 := iter1.Next()
		has2 := iter2.Next()
		if has1 != has2 {
			return false
		}
		if !has1 {
			return true
		}

		k1, v1 := iter1.Key(), iter1.Value()
		k2, v2 := iter2.Key(), iter2.Value()

		if !reflect.DeepEqual(k1, k2) {
			logging.Info("%v and %v not equal", k1, k2)
			return false
		}

		valueEqual := true
		if reflect.TypeOf(v1) != reflect.TypeOf(v2) {
			valueEqual = false
		}
		if valueEqual {
			switch v1.(type) {
			case proto.Message:
				valueEqual = proto.Equal(v1.(proto.Message), v2.(proto.Message))
			default:
				valueEqual = reflect.DeepEqual(v1, v2)
			}
		}
		if !valueEqual {
			logging.Info("%v and %v not equal", v1, v2)
			return false
		}
	}
}

func WaitCondition(t *testing.T, f func(log bool) bool, count int) {
	for i := 0; i < count; i++ {
		if f(false) {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
	assert.Assert(t, f(true))
}

func SpliceZkRootPath(path string) string {
	cmd := exec.Command("pwd")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	logging.Assert(err == nil, "")
	cur_path := strings.Replace(out.String(), "\n", "", -1)
	cur_path = cur_path + path
	return cur_path
}
