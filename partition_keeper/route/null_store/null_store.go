package null_store

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route/base"
)

const (
	MediaType = "nullstore"
)

type nullStore struct {
}

func (n *nullStore) Put(data *pb.RouteEntries) error {
	return nil
}

func (n *nullStore) Get() (*pb.RouteEntries, error) {
	return nil, nil
}

func (n *nullStore) Del() error {
	return nil
}

func NewNullStore(args *base.StoreBaseOption) base.StoreBase {
	return &nullStore{}
}

func init() {
	base.RegisterStoreCreator(MediaType, func(args *base.StoreBaseOption) base.StoreBase {
		return NewNullStore(args)
	})
}
