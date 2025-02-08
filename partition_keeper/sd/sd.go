package sd

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
)

type SdType string

type ServiceDiscovery interface {
	Stop()
	UpdateAzs(az map[string]bool) error
	GetNodes() (map[string]*pb.ServiceNode, error)
}

type sdCreator func(serviceName string, opts map[string]string) ServiceDiscovery

var (
	sdFactory = map[SdType]sdCreator{}
)

type SDFactoryOpts func(opts map[string]string) map[string]string

func NewServiceDiscovery(
	sdType SdType,
	serviceName string,
	opts ...SDFactoryOpts,
) ServiceDiscovery {
	creator, ok := sdFactory[sdType]
	if !ok {
		logging.Fatal("can't find sd creator for %s", sdType)
	}
	optMap := map[string]string{}
	for _, opt := range opts {
		optMap = opt(optMap)
	}

	return creator(serviceName, optMap)
}
