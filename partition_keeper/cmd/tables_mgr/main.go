package main

import (
	"flag"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/tables_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/version"
)

var (
	flagServiceName = flag.String("service_name", "", "service_name")
	flagGrpcPort    = flag.Int("grpc_port", 10200, "grpc port")
	flagHttpPort    = flag.Int(
		"http_port",
		0,
		"http port, should different with grpc port. default grpc_port+10",
	)
	flagZkHosts utils.StrlistFlag
	flagZkPath  = flag.String("zk_path", "/ks/reco/keeper/tables_mgr", "zk_path")
)

func init() {
	flag.Var(&flagZkHosts, "zk_hosts", "zk hosts with format <ip:port>,<ip:port>...")
}

func main() {
	flag.Parse()
	version.MayPrintVersionAndExit()

	httpPort := *flagHttpPort
	if httpPort == 0 {
		httpPort = *flagGrpcPort + 10
	}
	sv := tables_mgr.NewServer(
		*flagServiceName,
		int32(*flagGrpcPort),
		int32(httpPort),
		flagZkHosts,
		*flagZkPath,
	)
	sv.Start()
}
