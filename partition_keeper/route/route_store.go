package route

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/route/base"
)

type RouteStore = base.StoreBase

type StoreOption struct {
	Media string
	base.StoreBaseOption
}

func CreateStore(args *StoreOption) RouteStore {
	creator := base.GetStoreCreator(args.Media)
	return creator(&(args.StoreBaseOption))
}
