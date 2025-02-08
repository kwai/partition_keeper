package base

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
)

type StoreBaseOption struct {
	Url     string
	Service string
	Table   string
	Args    string
}

type StoreBase interface {
	Put(data *pb.RouteEntries) error
	Del() error
	Get() (*pb.RouteEntries, error)
}

type StoreCreator func(args *StoreBaseOption) StoreBase

var storeFactory = map[string]StoreCreator{}

func RegisterStoreCreator(storeMedia string, f StoreCreator) {
	if _, ok := storeFactory[storeMedia]; ok {
		logging.Fatal("duplicate register media %s", storeMedia)
	}
	storeFactory[storeMedia] = f
}

func UnregisterStoreCreator(storeMedia string) {
	delete(storeFactory, storeMedia)
}

func GetStoreCreator(storeMedia string) StoreCreator {
	return storeFactory[storeMedia]
}
