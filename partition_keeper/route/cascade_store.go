package route

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
)

var (
	ErrorEmptyStore = errors.New("empty store")
)

type CascadeRouteStore struct {
	mu     sync.Mutex
	stores map[string]RouteStore
	keys   []string
}

func makeStoreKey(opt *StoreOption) string {
	return fmt.Sprintf("%s://%s", opt.Media, opt.Url)
}

func NewCascadeStore(multiOpts []*StoreOption) *CascadeRouteStore {
	ans := &CascadeRouteStore{
		stores: make(map[string]RouteStore),
	}

	for _, opt := range multiOpts {
		key := makeStoreKey(opt)
		ans.keys = append(ans.keys, key)
		ans.stores[key] = CreateStore(opt)
	}
	return ans
}

func (c *CascadeRouteStore) UpdateStores(multiOpts []*StoreOption) {
	c.mu.Lock()
	defer c.mu.Unlock()

	oldStores := c.stores
	c.stores = make(map[string]RouteStore)
	c.keys = []string{}

	for _, opt := range multiOpts {
		key := makeStoreKey(opt)
		if store, ok := oldStores[key]; ok {
			c.stores[key] = store
			delete(oldStores, key)
		} else {
			c.stores[key] = CreateStore(opt)
		}
		c.keys = append(c.keys, key)
	}

	for key, store := range oldStores {
		if err := store.Del(); err != nil {
			logging.Error("delete store %s failed: %s", key, err.Error())
		}
	}
}

func (c *CascadeRouteStore) GetStore(opt *StoreOption) RouteStore {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := makeStoreKey(opt)
	return c.stores[key]
}

func (c *CascadeRouteStore) Put(data *pb.RouteEntries) error {
	var finalError error = nil
	c.mu.Lock()
	defer c.mu.Unlock()

	for name, store := range c.stores {
		if err := store.Put(data); err != nil {
			logging.Warning("put to store %s failed: %s", name, err.Error())
			finalError = err
		}
	}
	return finalError
}

func (c *CascadeRouteStore) Get() (*pb.RouteEntries, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.keys) == 0 {
		return nil, ErrorEmptyStore
	}
	i := rand.Intn(len(c.keys))
	return c.stores[c.keys[i]].Get()
}

func (c *CascadeRouteStore) Del() error {
	var finalError error = nil
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, store := range c.stores {
		if err := store.Del(); err != nil {
			logging.Warning("delete store %s failed: %s", name, err.Error())
			finalError = err
		}
	}
	return finalError
}
