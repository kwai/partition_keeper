package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/rpc"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/version"
)

var (
	flagHostList = flag.String(
		"host_list",
		"stdin",
		"host list, should be stdin, host://<hostname>:port or zk://ZKHost/<path_to_keeper_ns>",
	)
	flagPrint      = flag.Bool("p", false, "whether to print result to stdout")
	flagOutputFile = flag.String("output_file", "host_replicas", "output file of host replicas")
)

type HostIterator interface {
	Next() *utils.RpcNode
}

func getHostIterator() HostIterator {
	if *flagHostList == "stdin" {
		return &HostFileIterator{
			f: os.Stdin,
		}
	}
	if strings.HasPrefix(*flagHostList, "host://") {
		return &GivenHostIter{
			host:   strings.TrimPrefix(*flagHostList, "host://"),
			parsed: false,
		}
	}
	if strings.HasPrefix(*flagHostList, "zk://") {
		return NewZkIter(strings.TrimPrefix(*flagHostList, "zk://"))
	}
	logging.Assert(false, "can't recognize host list: %s", *flagHostList)
	return nil
}

func getReplicaOutputFile() *os.File {
	file, err := os.OpenFile(*flagOutputFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	logging.Assert(err == nil, "open %s got err: %v", *flagOutputFile, err)
	return file
}

func collectReplicas(node *utils.RpcNode, file *os.File, pool *rpc.ConnPool) {
	pool.Run(node, func(client interface{}) error {
		psclient := client.(pb.PartitionServiceClient)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		req := &pb.GetReplicasRequest{}
		resp, err := psclient.GetReplicas(ctx, req)
		if err != nil {
			logging.Warning("get replicas of %s failed: %s", node.String(), err.Error())
			return err
		}

		if *flagPrint {
			fmt.Printf("%s\n", node.String())
			fmt.Printf("%s\n", string(utils.MarshalJsonOrDie(resp)))
		}
		if file != nil {
			_, err := fmt.Fprintf(file, "%s\n", node.String())
			logging.Assert(err == nil, "write %s got error: %v", *flagOutputFile, err)

			var buf bytes.Buffer
			encoder := gob.NewEncoder(&buf)
			encoder.Encode(resp)
			_, err = fmt.Fprintf(file, "%X\n", buf.Bytes())
			logging.Assert(err == nil, "write %s got error: %v", *flagOutputFile, err)
		}
		return nil
	})
}

func main() {
	flag.Parse()
	version.MayPrintVersionAndExit()
	hostIterator := getHostIterator()
	file := getReplicaOutputFile()
	builder := &rpc.GrpcPSClientPoolBuilder{}
	pool := builder.Build()

	for {
		host := hostIterator.Next()
		if host == nil {
			break
		}
		collectReplicas(host, file, pool)
	}
}
