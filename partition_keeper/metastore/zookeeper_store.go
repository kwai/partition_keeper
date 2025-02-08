package metastore

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"

	"github.com/go-zookeeper/zk"
)

var (
	flagMaxAccessZkQps = flag.Int64("max_access_zk_qps", 20000, "max_access_zk_qps")
)

type zooLogAdapter struct{}

func (z *zooLogAdapter) Printf(format string, args ...interface{}) {
	logging.Info(format, args...)
}

func CreateZookeeperStore(
	zkHosts []string,
	timeout time.Duration,
	acl []zk.ACL,
	scheme string,
	auth []byte,
) *ZookeeperStore {
	result := &ZookeeperStore{
		rateLimiter: utils.NewTokenBucket(*flagMaxAccessZkQps, 100),
	}
	var err error
	result.acl = acl
	result.conn, result.eventWatcher, err = zk.Connect(
		zkHosts,
		timeout,
		zk.WithLogger(&zooLogAdapter{}),
	)
	if err != nil {
		logging.Fatal("can't connect to hosts: %v", zkHosts)
	}
	go result.watchSessionEvent()
	// wait a while for the session to connected
	time.Sleep(time.Millisecond * 50)

	if len(auth) > 0 && scheme != "" {
		succ := result.addAuth(context.Background(), scheme, auth)
		logging.Assert(succ, "")
	}
	return result
}

type ZookeeperStore struct {
	conn         *zk.Conn
	eventWatcher <-chan zk.Event
	manualClosed bool
	acl          []zk.ACL
	rateLimiter  *utils.TokenBucket
}

type accessZk func(conn *zk.Conn) error

var zkShouldRetryErrors = []error{
	zk.ErrUnknown,
	zk.ErrSessionMoved,
}

var zkLogicErrors = []error{
	zk.ErrNoNode,
	zk.ErrNodeExists,
}

func errorContains(expect []error, given error) bool {
	for _, e := range expect {
		if e == given {
			return true
		}
	}
	return false
}

func (z *ZookeeperStore) watchSessionEvent() {
	for {
		event := <-z.eventWatcher
		if event.Type == zk.EventSession {
			if event.State == zk.StateConnecting || event.State == zk.StateConnected ||
				event.State == zk.StateHasSession {
				logging.Info("got zk event %s", event.State.String())
			} else {
				if event.State == zk.StateDisconnected {
					if z.manualClosed {
						return
					} else {
						logging.Fatal("got unexpected zk event %v", &event)
					}
				} else {
					logging.Fatal("got unexpected zk event %v", &event)
				}
			}
		} else {
			logging.Warning("got zk event %s", &event)
		}
	}
}

func (z *ZookeeperStore) retryAccessZk(ctx context.Context, f accessZk, expect []error) bool {
	z.rateLimiter.AcquireToken()
	tryCount := 1
	for {
		state := z.conn.State()
		switch state {
		case zk.StateExpired:
			logging.Fatal("quit due to zk session has gone: %s", state.String())
		case zk.StateConnected, zk.StateHasSession:
			err := f(z.conn)
			if err == nil {
				logging.Verbose(1, "operate zk succeed with no error with %d times(s)", tryCount)
				return true
			}
			if errorContains(expect, err) {
				logging.Info(
					"operate zk got %s, treat succeed, with %d time(s)",
					err.Error(),
					tryCount,
				)
				return true
			}
			if errorContains(zkShouldRetryErrors, err) {
				logging.Warning(
					"operate zk got %s with %d times(s), should retry",
					err.Error(),
					tryCount,
				)
			} else {
				if errorContains(zkLogicErrors, err) {
					return false
				} else {
					logging.Fatal("operate zk got error %s with %d times(s), can't recover", err.Error(), tryCount)
				}
			}
		default:
			logging.Warning("wait zk %s to recover, has try %d times", state.String(), tryCount)
		}
		tryCount++

		sleepFor := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			logging.Info("don't continue to try as context is not allowed")
			sleepFor.Stop()
			return false
		case <-sleepFor.C:
			logging.Info("retry as 1 seconds has passed, old state: %s", state.String())
		}
	}
}

func (z *ZookeeperStore) GetRawSession() *zk.Conn {
	return z.conn
}

func (z *ZookeeperStore) Close() {
	z.manualClosed = true
	z.conn.Close()
}

func (z *ZookeeperStore) Create(ctx context.Context, path string, data []byte) bool {
	op := func(conn *zk.Conn) error {
		_, err := conn.Create(path, data, 0, z.acl)
		logging.Info("create zk node %s got result: %v", path, err)
		return err
	}
	return z.retryAccessZk(ctx, op, []error{zk.ErrNodeExists})
}

func (z *ZookeeperStore) Delete(ctx context.Context, path string) bool {
	op := func(conn *zk.Conn) error {
		err := conn.Delete(path, -1)
		logging.Info("delete zk node %s got result: %v", path, err)
		return err
	}
	return z.retryAccessZk(ctx, op, []error{zk.ErrNoNode})
}

func (z *ZookeeperStore) Set(ctx context.Context, path string, data []byte) bool {
	op := func(conn *zk.Conn) error {
		_, err := conn.Set(path, data, -1)
		logging.Info("set zk node %s got result: %v", path, err)
		return err
	}
	return z.retryAccessZk(ctx, op, nil)
}

func (z *ZookeeperStore) Get(ctx context.Context, path string) ([]byte, bool, bool) {
	var data []byte
	var err error
	op := func(conn *zk.Conn) error {
		data, _, err = conn.Get(path)
		if err == nil {
			logging.Verbose(1, "get zk node %s got result: %v", path, err)
		} else {
			logging.Info("get zk node %s got result: %v", path, err)
		}
		return err
	}
	ans := z.retryAccessZk(ctx, op, []error{zk.ErrNoNode})
	return data, err != zk.ErrNoNode, ans
}

func (z *ZookeeperStore) Children(ctx context.Context, path string) ([]string, bool, bool) {
	var children []string
	var err error
	op := func(conn *zk.Conn) error {
		children, _, err = conn.Children(path)
		if err == nil {
			logging.Verbose(1, "list zk node %s get result: %v", path, err)
		} else {
			logging.Info("list zk node %s get result: %v", path, err)
		}
		return err
	}
	ans := z.retryAccessZk(ctx, op, []error{zk.ErrNoNode})
	return children, err != zk.ErrNoNode, ans
}

func (z *ZookeeperStore) MultiCreate(ctx context.Context, path []string, data [][]byte) bool {
	pl, dl := len(path), len(data)
	logging.Assert(pl == dl, "path size %d vs data size %d", pl, dl)

	requests := []interface{}{}
	for i := range path {
		requests = append(requests, &zk.CreateRequest{
			Path:  path[i],
			Data:  data[i],
			Flags: 0,
			Acl:   z.acl,
		})
	}
	op := func(conn *zk.Conn) error {
		sub, err := conn.Multi(requests...)
		logging.Info("create zk %v got error: %v", path, err)
		if err != nil {
			for i := range sub {
				logging.Info("the %d(th) item %s error is %v", i, path[i], sub[i].Error)
			}
		}
		return err
	}

	return z.retryAccessZk(ctx, op, []error{zk.ErrNodeExists})
}

func (z *ZookeeperStore) MultiDelete(ctx context.Context, path []string) bool {
	requests := []interface{}{}
	for _, p := range path {
		requests = append(requests, &zk.DeleteRequest{
			Path:    p,
			Version: -1,
		})
	}
	op := func(conn *zk.Conn) error {
		sub, err := conn.Multi(requests...)
		logging.Info("delete zk %v got error: %v", path, err)
		if err != nil {
			for i := range sub {
				logging.Info("the %d(th) item %s error is %v", i, path[i], sub[i].Error)
			}
		}
		return err
	}

	return z.retryAccessZk(ctx, op, []error{zk.ErrNoNode})
}

func (z *ZookeeperStore) MultiSet(ctx context.Context, path []string, data [][]byte) bool {
	pl, dl := len(path), len(data)
	logging.Assert(pl == dl, "path size %d vs data size %d", pl, dl)

	requests := []interface{}{}
	for i := range path {
		requests = append(requests, &zk.SetDataRequest{
			Path:    path[i],
			Data:    data[i],
			Version: -1,
		})
	}
	op := func(conn *zk.Conn) error {
		sub, err := conn.Multi(requests...)
		logging.Info("set zk %v got error: %v", path, err)
		if err != nil {
			for i := range sub {
				logging.Info("the %d(th) item %s error is %v", i, path[i], sub[i].Error)
			}
		}
		return err
	}

	return z.retryAccessZk(ctx, op, nil)
}

func (z *ZookeeperStore) multiOpSubFail(err error) bool {
	return strings.Contains(err.Error(), "unknown error: -2")
}

func (z *ZookeeperStore) WriteBatch(ctx context.Context, ops ...WriteOp) bool {
	requests := []interface{}{}

	for _, op := range ops {
		switch op := op.(type) {
		case *SetOp:
			requests = append(requests, &zk.SetDataRequest{
				Path:    op.OpPath(),
				Data:    op.Data,
				Version: -1,
			})
		case *DeleteOp:
			requests = append(requests, &zk.DeleteRequest{
				Path:    op.OpPath(),
				Version: -1,
			})
		case *CreateOp:
			requests = append(requests, &zk.CreateRequest{
				Path:  op.OpPath(),
				Data:  op.Data,
				Flags: 0,
				Acl:   z.acl,
			})
		default:
			logging.Fatal("unreachable")
		}
	}

	writeAction := func(conn *zk.Conn) error {
		sub, err := conn.Multi(requests...)
		logging.Info("write batch to zk got %v", err)
		for i := range sub {
			logging.Info(
				"the %dth item %s %s error is %v",
				i,
				ops[i].Name(),
				ops[i].OpPath(),
				sub[i].Error,
			)
			suberr := sub[i].Error
			switch ops[i].(type) {
			case *SetOp:
				if suberr != nil && !z.multiOpSubFail(suberr) {
					return suberr
				}
			case *DeleteOp:
				if suberr != nil && suberr != zk.ErrNoNode && !z.multiOpSubFail(suberr) {
					return suberr
				}
			case *CreateOp:
				if suberr != nil && suberr != zk.ErrNodeExists && !z.multiOpSubFail(suberr) {
					return suberr
				}
			}
		}
		return err
	}

	return z.retryAccessZk(ctx, writeAction, nil)
}

func (z *ZookeeperStore) RecursiveCreate(ctx context.Context, path string) bool {
	elements := strings.Split(path, "/")
	prefix := ""
	for _, element := range elements {
		if element != "" {
			prefix = prefix + "/" + element
			_, exists, succ := z.Get(ctx, prefix)
			if !succ {
				return false
			}
			if !exists && !z.Create(ctx, prefix, []byte{}) {
				return false
			}
		}
	}
	return true
}

func (z *ZookeeperStore) RecursiveDelete(ctx context.Context, path string) bool {
	children, exists, succ := z.Children(ctx, path)
	if !succ {
		return false
	}
	if !exists {
		return true
	}
	for _, child := range children {
		succ := z.RecursiveDelete(ctx, fmt.Sprintf("%s/%s", path, child))
		if !succ {
			return false
		}
	}
	return z.Delete(ctx, path)
}

func (z *ZookeeperStore) addAuth(ctx context.Context, scheme string, auth []byte) bool {
	op := func(conn *zk.Conn) error {
		err := conn.AddAuth(scheme, auth)
		logging.Info("addAuth zk scheme : %s got result: %v", scheme, err)
		return err
	}
	return z.retryAccessZk(ctx, op, nil)
}
