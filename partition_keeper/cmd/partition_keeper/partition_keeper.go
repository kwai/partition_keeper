package main

import (
	"flag"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/server"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/version"
)

func main() {
	flag.Parse()
	version.MayPrintVersionAndExit()
	server.KconfInitialize()
	sv := server.CreateServer()
	sv.Start()
}
