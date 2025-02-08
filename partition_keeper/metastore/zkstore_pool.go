package metastore

import (
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

var (
	poolOnce   sync.Once
	globalPool *ZKStorePool
)

func GetGlobalPool() *ZKStorePool {
	poolOnce.Do(func() {
		globalPool = NewZKStorePool()
	})
	return globalPool
}

func NewZKStorePool() *ZKStorePool {
	return &ZKStorePool{
		pool: make(map[string]*ZookeeperStore),
	}
}

type ZKStorePool struct {
	mu   sync.RWMutex
	pool map[string]*ZookeeperStore
}

func (p *ZKStorePool) getExists(key string) *ZookeeperStore {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pool[key]
}

func (p *ZKStorePool) getOrNew(
	key string,
	addr []string,
	acl []zk.ACL,
	scheme string,
	auth []byte,
) *ZookeeperStore {
	p.mu.Lock()
	defer p.mu.Unlock()

	if ans, ok := p.pool[key]; ok {
		return ans
	}
	store := CreateZookeeperStore(addr, time.Second*60, acl, scheme, auth)
	p.pool[key] = store
	return store
}

func (p *ZKStorePool) Get(
	addrs []string,
	acl []zk.ACL,
	scheme string,
	auth []byte,
) *ZookeeperStore {
	key := utils.StringsKey(addrs)
	key += ";"
	key += utils.StringsAcl(acl)
	if ans := p.getExists(key); ans != nil {
		return ans
	}
	return p.getOrNew(key, addrs, acl, scheme, auth)
}
