package main

import (
	"fmt"
	"os"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type HostFileIterator struct {
	f *os.File
}

func (h *HostFileIterator) Next() *utils.RpcNode {
	output := &utils.RpcNode{}
	n, err := fmt.Fscanf(h.f, "%s:%d\n", &(output.NodeName), &(output.Port))
	if err == nil && n == 2 {
		return output
	}
	logging.Info("scan file got error: %v, count: %d", err, n)
	return nil
}
