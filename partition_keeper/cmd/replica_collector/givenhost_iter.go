package main

import (
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

type GivenHostIter struct {
	host   string
	parsed bool
}

func (g *GivenHostIter) Next() *utils.RpcNode {
	if !g.parsed {
		ans := utils.FromHostPort(g.host)
		g.parsed = true
		return ans
	}
	return nil
}
