package route

import (
	"errors"
	"sync"
	"testing"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route/base"

	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"
)

var (
	errorIll = errors.New("mocked ill")
)

type mockRouteStore struct {
	mu    sync.Mutex
	ill   bool
	entry *pb.RouteEntries
}

func (m *mockRouteStore) MakeIll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ill = true
}

func (m *mockRouteStore) Put(data *pb.RouteEntries) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entry = proto.Clone(data).(*pb.RouteEntries)
	if m.ill {
		return errorIll
	} else {
		return nil
	}
}

func (m *mockRouteStore) Get() (*pb.RouteEntries, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return proto.Clone(m.entry).(*pb.RouteEntries), nil
}

func (m *mockRouteStore) Del() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entry = nil

	if m.ill {
		return errorIll
	} else {
		return nil
	}
}

func init() {
	base.RegisterStoreCreator("mock_route_store", func(args *base.StoreBaseOption) base.StoreBase {
		return &mockRouteStore{}
	})
}

func TestCascadeStore(t *testing.T) {
	opts := []*StoreOption{
		{
			Media: "mock_route_store",
			StoreBaseOption: base.StoreBaseOption{
				Url:     "test1",
				Service: "test",
				Table:   "test",
				Args:    "",
			},
		},
		{
			Media: "mock_route_store",
			StoreBaseOption: base.StoreBaseOption{
				Url:     "test1",
				Service: "test",
				Table:   "test",
				Args:    "",
			},
		},
	}

	store := NewCascadeStore(opts)
	assert.Equal(t, len(store.stores), 1)

	opts[1].Url = "test2"
	store2 := NewCascadeStore(opts)
	assert.Equal(t, len(store2.stores), 2)

	mock1 := store2.GetStore(opts[0]).(*mockRouteStore)

	mock2 := store2.GetStore(opts[1]).(*mockRouteStore)
	mock2.MakeIll()

	entries := &pb.RouteEntries{}
	err := store2.Put(entries)
	assert.Assert(t, err != nil)

	assert.Assert(t, mock1.entry != nil)
	assert.Assert(t, mock2.entry != nil)

	opts[1] = &StoreOption{
		Media: "mock_route_store",
		StoreBaseOption: base.StoreBaseOption{
			Url:     "test3",
			Service: "test",
			Table:   "test",
			Args:    "",
		},
	}

	store2.UpdateStores(opts)
	newMock1 := store2.GetStore(opts[0]).(*mockRouteStore)
	newMock3 := store2.GetStore(opts[1]).(*mockRouteStore)
	assert.Equal(t, newMock1, mock1)
	assert.Assert(t, newMock3 != mock2)
	assert.Assert(t, newMock1.entry != nil)
	assert.Assert(t, mock2.entry == nil)
	assert.Assert(t, newMock3.entry == nil)

	err = store2.Put(entries)
	assert.Assert(t, err == nil)
	assert.Assert(t, newMock3.entry != nil)

	err = store2.Del()
	assert.Assert(t, err == nil)
	assert.Assert(t, newMock1.entry == nil)
	assert.Assert(t, newMock3.entry == nil)
}
