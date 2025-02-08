package main

import (
	"flag"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/proxy"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/version"
)

func main() {
	flag.Parse()
	version.MayPrintVersionAndExit()
	p := proxy.CreateProxy()
	p.Start()
}
