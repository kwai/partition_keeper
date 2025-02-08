package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/version"

	"github.com/go-zookeeper/zk"
)

const (
	kBackup  = "backup"
	kRestore = "restore"
)

var (
	flagZkAddr       utils.StrlistFlag
	flagOp           = flag.String("op", "", "the op to zk, should be backup or restore")
	flagBackupFile   = flag.String("backup_file", "backup_file", "the backup file")
	flagZkPath       = flag.String("zk_path", "", "the zk_path for backup or restore")
	flagRestoreBatch = flag.Int("restore_batch", 10, "restore_batch")
	flagZkUser       = flag.String("zk_user", "", "the user for zk ACL")
	flagZkPassword   = flag.String("zk_password", "", "the password for zk ACL")
)

func init() {
	flag.Var(&flagZkAddr, "zk_addr", "zookeeper addr to backup/retore")
}

func check(flag bool, format string, values ...interface{}) {
	if !flag {
		fmt.Fprintf(os.Stderr, format+"\n", values...)
		os.Exit(1)
	}
}

func getACLandAuth() ([]zk.ACL, string, []byte) {
	if *flagZkUser == "" && *flagZkPassword == "" {
		return zk.WorldACL(zk.PermAll), "", []byte("")
	} else {
		acls := zk.WorldACL(zk.PermRead)
		digest := zk.DigestACL(zk.PermAll, *flagZkUser, *flagZkPassword)
		acls = append(acls, digest...)
		return acls, "digest", []byte(*flagZkUser + ":" + *flagZkPassword)
	}
}

func checkZkArgs() {
	check(len(flagZkAddr) > 0, "please specify zk addr with -zk_addr")
	check(len(*flagZkPath) > 0, "please specify zk path with -zk_path")
}

func backupPath(lines *int, zkStore *metastore.ZookeeperStore, path string, writer io.Writer) {
	data, exists, succ := zkStore.Get(context.Background(), path)
	check(succ, "get from %s failed", path)
	if !exists {
		fmt.Fprintf(os.Stderr, "%s not exists\n", path)
		return
	}

	_, err := fmt.Fprintf(writer, "%s %X\n", path, data)
	check(err == nil, "write to output file failed: %v", err)
	*lines++
	if *lines%1000 == 0 {
		fmt.Printf("has handle %d lines\n", *lines)
	}

	subs, _, succ := zkStore.Children(context.Background(), path)
	check(succ, "get children failed for %s", path)

	for _, sub := range subs {
		backupPath(lines, zkStore, fmt.Sprintf("%s/%s", path, sub), writer)
	}
}

func runBackup() {
	checkZkArgs()

	outputFile, err := os.Create(*flagBackupFile)
	check(err == nil, "open %s failed: %v", *flagBackupFile, err)
	defer outputFile.Close()

	w := bufio.NewWriter(outputFile)
	defer w.Flush()

	acl, scheme, auth := getACLandAuth()
	zkStore := metastore.CreateZookeeperStore(flagZkAddr, time.Second*10, acl, scheme, auth)
	lines := 0
	backupPath(&lines, zkStore, *flagZkPath, w)
	fmt.Printf("finish backup, total handle %d lines\n", lines)
}

type batchCreator struct {
	zkConn *zk.Conn
	ops    []interface{}
}

func (b *batchCreator) add(path string, data []byte) {
	b.ops = append(b.ops, &zk.CreateRequest{
		Path:  path,
		Data:  data,
		Flags: 0,
		Acl:   zk.WorldACL(zk.PermAll),
	})
	if len(b.ops) >= *flagRestoreBatch {
		b.flush()
	}
}

func (b *batchCreator) flush() {
	if len(b.ops) <= 0 {
		return
	}
	res, err := b.zkConn.Multi(b.ops...)
	if err != nil {
		check(false, "create multiple path failed, err: %s, resps: %+q", err, res)
	}
	b.ops = nil
}

func restorePath(zkConn *zk.Conn, path string, reader io.Reader) {
	b := &batchCreator{
		zkConn: zkConn,
	}
	line := 1
	prefixLen := 0
	for {
		var p, data string
		n, err := fmt.Fscanf(reader, "%s %X\n", &p, &data)
		check(n <= 2, "read file can't parse valid item from line %d", line)
		if n <= 0 {
			b.flush()
			fmt.Printf("total handled lines: %d, current error: %v\n", line-1, err)
			break
		}
		if line == 1 {
			prefixLen = len(p)
		}

		newPath := path + p[prefixLen:]
		b.add(newPath, []byte(data))
		if line%1000 == 0 {
			fmt.Printf("has put %d lines\n", line)
		}
		line++
	}
}

func runRestore() {
	checkZkArgs()

	inputFile, err := os.Open(*flagBackupFile)
	check(err == nil, "open %s failed: %v", *flagBackupFile, err)
	defer inputFile.Close()

	rd := bufio.NewReader(inputFile)
	acl, scheme, auth := getACLandAuth()
	zkStore := metastore.CreateZookeeperStore(flagZkAddr, time.Second*10, acl, scheme, auth)

	succ := zkStore.RecursiveCreate(context.Background(), path.Dir(*flagZkPath))
	check(succ, "recursive create %s failed", path.Dir(*flagZkPath))
	restorePath(zkStore.GetRawSession(), *flagZkPath, rd)
}

func main() {
	flag.Parse()
	version.MayPrintVersionAndExit()
	if *flagOp == kBackup {
		runBackup()
	} else if *flagOp == kRestore {
		runRestore()
	} else {
		check(false, "please specify op with -op, allowed ops are backup or restore")
	}
}
