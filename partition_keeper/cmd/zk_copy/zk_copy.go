package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	cmd_base "github.com/kuaishou/open_partition_keeper/partition_keeper/server/cmd/base"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/dbg"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/node_mgr"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/version"
)

const (
	kDestZkUser     = "keeper"
	kDestZkPassword = "admin"

	kMockIdentify     = "-local-test"
	kDefaultKconfPath = "reco.rodisFea.colossusdbTest"

	kZkRootPath = "/ks/reco/keeper"
)

var (
	flagSrcZkHosts = flag.String(
		"src_zk_hosts",
		"1.2.3.4:1234",
		"zk hosts with format <ip:port>",
	)
	flagDestZkHosts = flag.String(
		"dest_zk_hosts",
		"1.2.3.4:1234",
		"zk hosts with format <ip:port>",
	)
	flagCopyNamespace = flag.String(
		"copy_namespace",
		"",
		"namespace of the keeper",
	)
	flagReplicasFile = flag.String(
		"replicas_file",
		"",
		"all replicas of the namespace",
	)
)

var (
	kHostName       = ""
	kNodePort int32 = 10000
)

func init() {
	var err error
	kHostName, err = os.Hostname()
	logging.Assert(err == nil, "")
}

type copyServer struct {
	srcZkConn         *metastore.ZookeeperStore
	destZkConn        *metastore.ZookeeperStore
	file              *os.File
	srcHost2LocalHost map[string]string
}

type copyCheckpointExecuteInfo struct {
	ExecuteType    string                 `json:"execute_type"`
	ExecuteName    string                 `json:"execute_name"`
	ExecuteTime    int64                  `json:"execute_time"`
	ExecuteContext cmd_base.HandlerParams `json:"execute_context"`
}

func getDestACLandAuthForZK() ([]zk.ACL, string, []byte) {
	acls := zk.WorldACL(zk.PermRead)
	digest := zk.DigestACL(zk.PermAll, kDestZkUser, kDestZkPassword)
	acls = append(acls, digest...)
	return acls, "digest", []byte(kDestZkUser + ":" + kDestZkPassword)
}

func (c *copyServer) prepare() bool {
	fmt.Printf("Start preparing to connect ZK \n")
	if *flagSrcZkHosts == *flagDestZkHosts {
		fmt.Printf("The src and dest hosts must be different\n")
		return false
	}
	var srcZkHosts, destZkHosts []string
	srcZkHosts = append(srcZkHosts, *flagSrcZkHosts)
	c.srcZkConn = metastore.CreateZookeeperStore(
		srcZkHosts,
		time.Second*10,
		zk.WorldACL(zk.PermRead),
		"",
		[]byte(""),
	)

	acl, scheme, auth := getDestACLandAuthForZK()
	destZkHosts = append(destZkHosts, *flagDestZkHosts)
	c.destZkConn = metastore.CreateZookeeperStore(destZkHosts, time.Second*10, acl, scheme, auth)

	filename := dbg.MOCK_NODES_FILE
	os.Remove(filename)

	var err error
	c.file, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("Fail to open node info file: %s\n", err.Error())
		return false
	}
	return true
}

func (c *copyServer) destZkCreate(path string, data []byte) {
	succ := c.destZkConn.Create(context.Background(), path, data)
	logging.Assert(succ, "")
}

func (c *copyServer) copyFromSrcToDest(srcPath, destPath string) bool {
	data, exists, succ := c.srcZkConn.Get(context.Background(), srcPath)
	logging.Assert(succ, "")
	if !exists {
		return false
	}
	c.destZkCreate(destPath, data)
	return true
}

func (c *copyServer) copyDelayedExecutor(executorPath string) bool {
	fmt.Printf("Start copying delayed executor:%s\n", executorPath)
	succ := c.copyFromSrcToDest(executorPath, executorPath)
	if !succ {
		fmt.Printf(
			"delayed_executor does not exist in this namespace, continue to copy other directories\n",
		)
		return true
	}
	children, exists, succ := c.srcZkConn.Children(context.Background(), executorPath)
	logging.Assert(succ && exists, "")
	for _, child := range children {
		executorPath := fmt.Sprintf("%s/%s", executorPath, child)
		data, exists, succ := c.srcZkConn.Get(context.Background(), executorPath)
		logging.Assert(succ && exists, "")

		if strings.Contains(child, "checkpoint") {
			var checkpointInfo copyCheckpointExecuteInfo
			utils.UnmarshalJsonOrDie(data, &checkpointInfo)

			checkpointInfo.ExecuteContext.ServiceName += kMockIdentify
			checkpointInfo.ExecuteContext.KconfPath = kDefaultKconfPath

			destData := utils.MarshalJsonOrDie(checkpointInfo)
			c.destZkCreate(executorPath, destData)
		} else {
			// ignore
			fmt.Printf("An unknown task type exists:%s. Ignore it", child)
		}
	}
	return true
}

func modifyServiceAz(input []byte) []byte {
	inputJson := map[string]interface{}{}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	err := decoder.Decode(&inputJson)
	logging.Assert(err == nil, "%v", err)

	modifyAz := func(hubs interface{}) (interface{}, error) {
		switch hubs := hubs.(type) {
		case []interface{}:
			for _, hub := range hubs {
				switch hub := hub.(type) {
				case map[string]interface{}:
					hub["az"] = "STAGING"
				default:
					return nil, errors.New("hub not map[string]interface{}")
				}
			}
			return hubs, nil
		default:
			return nil, errors.New("hubs not []interface{}")
		}
	}

	value, ok := inputJson["hubs"]
	if !ok || value == nil {
		fmt.Printf("hubs is empty\n")
		return input
	}
	err = utils.EditJson(inputJson, []string{"hubs"}, modifyAz)
	logging.Assert(err == nil, "%v", err)
	return utils.MarshalJsonOrDie(inputJson)
}

func modifyNodeInfo(input []byte, newAddr *utils.RpcNode, newBizPort int32) []byte {
	inputJson := map[string]interface{}{}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	err := decoder.Decode(&inputJson)
	logging.Assert(err == nil, "%v", err)

	concatId := func(id interface{}) (interface{}, error) {
		switch id := id.(type) {
		case string:
			return id + kMockIdentify, nil
		default:
			return nil, errors.New("not string")
		}
	}
	err = utils.EditJson(inputJson, []string{"id"}, concatId)
	logging.Assert(err == nil, "%v", err)

	replaceNodeName := func(name interface{}) (interface{}, error) {
		switch name.(type) {
		case string:
			return newAddr.NodeName, nil
		default:
			return nil, errors.New("not string")
		}
	}
	err = utils.EditJson(inputJson, []string{"address", "node_name"}, replaceNodeName)
	logging.Assert(err == nil, "%v", err)

	replacePort := func(port interface{}) (interface{}, error) {
		switch port.(type) {
		case json.Number:
			return json.Number(fmt.Sprintf("%d", newAddr.Port)), nil
		default:
			return nil, fmt.Errorf("not integer: %v, type: %T", port, port)
		}
	}
	logging.Info("%s", string(input))
	err = utils.EditJson(inputJson, []string{"address", "port"}, replacePort)
	logging.Assert(err == nil, "%v", err)

	replaceBizPort := func(bizPort interface{}) (interface{}, error) {
		switch bizPort.(type) {
		case json.Number:
			return json.Number(fmt.Sprintf("%d", newBizPort)), nil
		default:
			return nil, errors.New("not integer")
		}
	}
	err = utils.EditJson(inputJson, []string{"biz_port"}, replaceBizPort)
	logging.Assert(err == nil, "%v", err)

	replaceAz := func(az interface{}) (interface{}, error) {
		switch az.(type) {
		case string:
			return "STAGING", nil
		default:
			return nil, fmt.Errorf("not integer: %v, type: %T", az, az)
		}
	}
	err = utils.EditJson(inputJson, []string{"az"}, replaceAz)
	logging.Assert(err == nil, "%v", err)

	return utils.MarshalJsonOrDie(inputJson)
}

func (c *copyServer) copyService(serviceRootPath string) bool {
	// /ks/reco/keeper/default/service
	succ := c.copyFromSrcToDest(serviceRootPath, serviceRootPath)
	logging.Assert(succ, "")
	children, exists, succ := c.srcZkConn.Children(context.Background(), serviceRootPath)
	logging.Assert(succ && exists, "")
	for _, child := range children {
		nodesInfo := make(map[string]*node_mgr.NodeInfo)
		// /ks/reco/keeper/default/service/service_name
		fmt.Printf("Start copying service:%s\n", child)
		srcServicePath := fmt.Sprintf("%s/%s", serviceRootPath, child)
		srcNodesPath := srcServicePath + "/nodes"
		srcHintsPath := srcServicePath + "/hints"
		srcSchedOptionsPath := srcServicePath + "/sched_opts"

		destServicePath := srcServicePath + kMockIdentify
		destNodesPath := destServicePath + "/nodes"
		destHintsPath := destServicePath + "/hints"
		destSchedOptionsPath := destServicePath + "/sched_opts"

		serviceProps, exists, succ := c.srcZkConn.Get(context.Background(), srcServicePath)
		logging.Assert(succ && exists, "")
		destData := modifyServiceAz(serviceProps)
		c.destZkCreate(destServicePath, destData)

		c.copyFromSrcToDest(srcNodesPath, destNodesPath)
		c.copyFromSrcToDest(srcHintsPath, destHintsPath)
		c.copyFromSrcToDest(srcSchedOptionsPath, destSchedOptionsPath)

		// copy node
		nodes, exists, succ := c.srcZkConn.Children(context.Background(), srcNodesPath)
		logging.Assert(succ && exists, "")
		for _, nd := range nodes {
			// /ks/reco/keeper/default/service/service_name/nodes/node_id
			srcNodePath := fmt.Sprintf("%s/%s", srcNodesPath, nd)
			nodeProps, exists, succ := c.srcZkConn.Get(context.Background(), srcNodePath)
			logging.Assert(succ && exists, "")
			info := &node_mgr.NodeInfo{}
			utils.UnmarshalJsonOrDie(nodeProps, info)

			key := info.Address.NodeName + ":" + strconv.FormatInt(int64(info.Address.Port), 10)
			nodesInfo[key] = info
			c.srcHost2LocalHost[key] = kHostName + ":" + strconv.FormatInt(int64(kNodePort), 10)

			info.Id += kMockIdentify
			info.Address.NodeName = kHostName
			info.Address.Port = kNodePort
			info.BizPort = info.Address.Port + 1
			info.Az = "STAGING"
			kNodePort += 20

			destNodePath := fmt.Sprintf("%s/%s", destNodesPath, nd)
			destPath := destNodePath + kMockIdentify
			destData := modifyNodeInfo(nodeProps, info.Address, info.BizPort)
			c.destZkCreate(destPath, destData)
		}

		// copy hints
		hints, exists, succ := c.srcZkConn.Children(context.Background(), srcHintsPath)
		logging.Assert(succ && exists, "")
		for _, hint := range hints {
			// /ks/reco/keeper/default/service/service_name/hints/host:port
			srcHintPath := fmt.Sprintf("%s/%s", srcHintsPath, hint)
			data, exists, succ := c.srcZkConn.Get(context.Background(), srcHintPath)
			logging.Assert(succ && exists, "")

			destNodeInfo, succ := nodesInfo[hint]
			if !succ {
				fmt.Printf("This node:%s doesn't exist anymore, but it still has hints\n", hint)
				continue
			}

			destHintPath := kHostName + ":" + strconv.FormatInt(
				int64(destNodeInfo.Address.Port),
				10,
			)
			c.destZkCreate(fmt.Sprintf("%s/%s", destHintsPath, destHintPath), data)
		}

		if !c.writeNodeInfoFile(child+kMockIdentify, nodesInfo) {
			return false
		}
	}
	return true
}

func modifyTableProto(input []byte) []byte {
	inputJson := map[string]interface{}{}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	err := decoder.Decode(&inputJson)
	logging.Assert(err == nil, "%v", err)

	concatBelongToService := func(serviceName interface{}) (interface{}, error) {
		switch serviceName := serviceName.(type) {
		case string:
			return serviceName + kMockIdentify, nil
		default:
			return nil, errors.New("not string")
		}
	}
	err = utils.EditJson(inputJson, []string{"belong_to_service"}, concatBelongToService)
	logging.Assert(err == nil, "%v", err)

	replaceKconfPath := func(kconfPath interface{}) (interface{}, error) {
		switch kconfPath.(type) {
		case string:
			return kDefaultKconfPath, nil
		default:
			return nil, errors.New("not string")
		}
	}
	err = utils.EditJson(inputJson, []string{"kconf_path"}, replaceKconfPath)
	logging.Assert(err == nil, "%v", err)

	replaceZkHosts := func(zkHosts interface{}) (interface{}, error) {
		switch zkHosts.(type) {
		case []interface{}:
			return []string{*flagDestZkHosts}, nil
		default:
			return nil, errors.New("not array")
		}
	}

	concatClusterPrefix := func(clusterPrefix interface{}) (interface{}, error) {
		switch clusterPrefix := clusterPrefix.(type) {
		case string:
			return clusterPrefix + kMockIdentify, nil
		default:
			return nil, errors.New("not string")
		}
	}

	modifyJsonArgs := func(jsonArgs interface{}) (interface{}, error) {
		switch jsonArgs := jsonArgs.(type) {
		case string:
			if jsonArgs == "" {
				jsonArgs = "{}"
			}
			rodisArgs := map[string]interface{}{}
			decorder := json.NewDecoder(strings.NewReader(jsonArgs))
			decorder.UseNumber()
			err := decorder.Decode(&rodisArgs)
			logging.Assert(err == nil, "%v", err)
			err = utils.EditJson(rodisArgs, []string{"zk_hosts"}, replaceZkHosts)
			if err != nil {
				logging.Assert(strings.Contains(err.Error(), "can't find"), "%v", err)
			}
			err = utils.EditJson(rodisArgs, []string{"cluster_prefix"}, concatClusterPrefix)
			if err != nil {
				logging.Assert(strings.Contains(err.Error(), "can't find"), "%v", err)
			}
			output, err := json.MarshalIndent(rodisArgs, "", "  ")
			logging.Assert(err == nil, "")
			return string(output), nil
		default:
			return nil, errors.New("not string")
		}
	}
	err = utils.EditJson(inputJson, []string{"json_args"}, modifyJsonArgs)
	logging.Assert(err == nil, "%v", err)

	return utils.MarshalJsonOrDie(inputJson)
}

func modifyMembership(input []byte) []byte {
	inputJson := map[string]interface{}{}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	err := decoder.Decode(&inputJson)
	logging.Assert(err == nil, "%v", err)

	concatPeers := func(peers interface{}) (interface{}, error) {
		switch peers := peers.(type) {
		case map[string]interface{}:
			output := map[string]interface{}{}
			for id, role := range peers {
				output[id+kMockIdentify] = role
			}
			return output, nil
		default:
			return nil, errors.New("not map")
		}
	}

	err = utils.EditJson(inputJson, []string{"peers"}, concatPeers)
	logging.Assert(err == nil, "%v", err)
	return utils.MarshalJsonOrDie(inputJson)
}

func (c *copyServer) copyTables(tableRootPath string) bool {
	fmt.Printf("Start copying table:%s\n", tableRootPath)
	// /ks/reco/keeper/default/tables
	c.copyFromSrcToDest(tableRootPath, tableRootPath)

	{
		// /ks/reco/keeper/default/tables/__RECYCLE_IDS__
		recyclePath := tableRootPath + "/__RECYCLE_IDS__"
		c.copyFromSrcToDest(recyclePath, recyclePath)
		children, exists, succ := c.srcZkConn.Children(context.Background(), recyclePath)
		logging.Assert(succ && exists, "")

		for _, child := range children {
			// /ks/reco/keeper/default/tables/__RECYCLE_IDS__/child
			path := fmt.Sprintf("%s/%s", recyclePath, child)
			c.copyFromSrcToDest(path, path)
		}
	}

	children, exists, succ := c.srcZkConn.Children(context.Background(), tableRootPath)
	logging.Assert(succ && exists, "")

	for _, child := range children {
		// /ks/reco/keeper/default/tables/table_name
		if child == "__RECYCLE_IDS__" {
			continue
		}
		fmt.Printf("start copying table %s\n", child)
		tablePath := fmt.Sprintf("%s/%s", tableRootPath, child)
		tableProps, exists, succ := c.srcZkConn.Get(context.Background(), tablePath)
		logging.Assert(succ && exists, "")
		destData := modifyTableProto(tableProps)
		c.destZkCreate(tablePath, destData)

		partsPath := tablePath + "/parts"
		tasksPath := tablePath + "/tasks"
		succ = c.copyFromSrcToDest(partsPath, partsPath)
		logging.Assert(succ, "")
		succ = c.copyFromSrcToDest(tasksPath, tasksPath)
		logging.Assert(succ, "")

		// copy parts
		fmt.Printf("start copying parts of table %s\n", child)
		parts, exists, succ := c.srcZkConn.Children(context.Background(), partsPath)
		logging.Assert(succ && exists, "")
		for _, part := range parts {
			partPath := fmt.Sprintf("%s/%s", partsPath, part)
			props, exists, succ := c.srcZkConn.Get(context.Background(), partPath)
			logging.Assert(succ && exists, "")
			destData := modifyMembership(props)
			c.destZkCreate(partPath, destData)
		}

		// copy tasks
		fmt.Printf("start copying tasks of table %s\n", child)
		checkpointPath := tasksPath + "/checkpoint"
		succ = c.copyFromSrcToDest(checkpointPath, checkpointPath)
		if !succ {
			continue
		}

		tasks, exists, succ := c.srcZkConn.Children(context.Background(), checkpointPath)
		logging.Assert(succ && exists, "")
		for _, task := range tasks {
			taskPath := fmt.Sprintf("%s/%s", checkpointPath, task)
			succ = c.copyFromSrcToDest(taskPath, taskPath)
			logging.Assert(succ, "")
		}
	}
	return true
}

func (c *copyServer) copyNamespaceData(namespacePath string) bool {
	fmt.Printf(
		"Start copying [%s] data from the src:%s to the dest:%s \n",
		namespacePath,
		*flagSrcZkHosts,
		*flagDestZkHosts,
	)
	succ := c.copyFromSrcToDest(namespacePath, namespacePath)
	if !succ {
		fmt.Printf("This path:%s does not exist in src ZK\n", namespacePath)
		return false
	}

	if !c.copyDelayedExecutor(namespacePath + "/delayed_executor") {
		fmt.Printf("Copy delayed executor fail\n")
		return false
	}

	if !c.copyService(namespacePath + "/service") {
		fmt.Printf("Copy services fail\n")
		return false
	}
	if !c.copyTables(namespacePath + "/tables") {
		fmt.Printf("Copy tables fail\n")
		return false
	}
	return true
}

func (c *copyServer) start() bool {
	_, exists, succ := c.srcZkConn.Get(context.Background(), kZkRootPath)
	logging.Assert(succ, "")
	if !exists {
		fmt.Printf("This path:%s does not exist in src ZK\n", kZkRootPath)
		return false
	}

	if *flagCopyNamespace == "" {
		fmt.Printf("copy_namespace can't be empty\n")
		return false
	}

	succ = c.destZkConn.RecursiveCreate(context.Background(), kZkRootPath)
	logging.Assert(succ, "")

	return c.copyNamespaceData(fmt.Sprintf("%s/%s", kZkRootPath, *flagCopyNamespace))
}

func (c *copyServer) stop() {
	c.file.Close()
	c.srcZkConn.Close()
	c.destZkConn.Close()
}

func (c *copyServer) writeNodeInfoFile(
	serviceName string,
	nodesInfo map[string]*node_mgr.NodeInfo,
) bool {
	key := []byte(serviceName + ":")
	_, err := c.file.Write(key)
	if err != nil {
		fmt.Printf("Failed to write nodes_info file, %s\n", err.Error())
		return false
	}

	first := true
	for _, node := range nodesInfo {
		var line []byte
		if !first {
			line = []byte("&&")
		}
		first = false

		data := utils.MarshalJsonOrDie(node)
		line = append(line, data...)

		_, err := c.file.Write(line)
		if err != nil {
			fmt.Printf("Failed to write nodes_info file, %s\n", err.Error())
			return false
		}
	}
	c.file.WriteString("\n")
	return true
}

func prepareReplicas(srcHost2LocalHost map[string]string) bool {
	if *flagReplicasFile == "" {
		return false
	}

	srcFile, err := os.Open(*flagReplicasFile)
	if err != nil {
		fmt.Printf("Mock onebox open %s fail:%s\n", *flagReplicasFile, err.Error())
		return false
	}
	defer srcFile.Close()

	os.Remove(dbg.MOCK_ALL_REPLICAS_FILE)
	destFile, err := os.OpenFile(dbg.MOCK_ALL_REPLICAS_FILE, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("Mock onebox open %s fail: %s\n", dbg.MOCK_ALL_REPLICAS_FILE, err.Error())
		return false
	}
	defer destFile.Close()

	rd := bufio.NewReader(srcFile)
	for i := 0; ; i++ {
		if i%2 == 0 {
			// hostname
			key := ""
			n, err := fmt.Fscanf(rd, "%s\n", &key)
			if n == 0 || err == io.EOF {
				break
			}
			logging.Assert(err == nil, "")
			data, ok := srcHost2LocalHost[key]
			if ok {
				_, err := fmt.Fprintf(destFile, "%s\n", data)
				if err != nil {
					fmt.Printf(
						"Failed to write %s file, %s\n",
						dbg.MOCK_ALL_REPLICAS_FILE,
						err.Error(),
					)
					return false
				}
			} else {
				// Invalid host
				fmt.Printf("This host :%s is not in the zk", key)
				i++
				var data []byte
				n, err := fmt.Fscanf(rd, "%X\n", &data)
				if n == 0 || err == io.EOF {
					break
				}
			}
		} else {
			// replicas
			var data []byte
			n, err := fmt.Fscanf(rd, "%X\n", &data)
			if n == 0 || err == io.EOF {
				break
			}
			logging.Assert(err == nil, "")

			buffer := bytes.NewBuffer(data)
			resp := &pb.GetReplicasResponse{}

			decoder := gob.NewDecoder(buffer)
			if err := decoder.Decode(&resp); err != nil {
				logging.Fatal("GetReplicasResponse decode fail")
			}

			for _, info := range resp.Infos {
				if info != nil && info.PeerInfo != nil {
					for _, replica := range info.PeerInfo.Peers {
						replica.NodeUniqueId += kMockIdentify
						hostname := replica.Node.NodeName + ":" + strconv.FormatInt(int64(replica.Node.Port), 10)
						dest, ok := srcHost2LocalHost[hostname]
						if ok {
							s := strings.Split(dest, ":")
							logging.Assert(len(s) == 2, "")
							replica.Node.NodeName = s[0]
							destPort, err := strconv.ParseInt(s[1], 10, 32)
							logging.Assert(err == nil, "")
							replica.Node.Port = int32(destPort)
						} else {
							replica.Node.NodeName = kHostName
							replica.Node.Port = int32(i)
							fmt.Printf("host:%s is not in zk, ignore\n", hostname)
						}
					}
				}
			}

			var buf bytes.Buffer
			encoder := gob.NewEncoder(&buf)
			encoder.Encode(resp)
			_, err = fmt.Fprintf(destFile, "%X\n", buf.Bytes())
			if err != nil {
				fmt.Printf("Failed to write %s file, %s\n", dbg.MOCK_ALL_REPLICAS_FILE, err.Error())
				return false
			}
		}
	}
	return true
}

func main() {
	flag.Parse()
	version.MayPrintVersionAndExit()

	if !strings.Contains(*flagDestZkHosts, "127.0.0.1:") {
		fmt.Printf("The dest_zk_hosts address must be 127.0.0.1\n")
		return
	}

	server := copyServer{
		srcHost2LocalHost: make(map[string]string),
	}
	defer server.stop()
	if !server.prepare() {
		return
	}
	if !server.start() {
		fmt.Printf("FAIL!\n")
		return
	}

	if !prepareReplicas(server.srcHost2LocalHost) {
		fmt.Printf("FAIL!\n")
		return
	}
	fmt.Printf("SUCCEED!\n")
}
