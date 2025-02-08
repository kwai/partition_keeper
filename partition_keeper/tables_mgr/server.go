package tables_mgr

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/metastore"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/server/acl"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/version"
	"google.golang.org/grpc"
)

const (
	kPropsZNode   = "property"
	kLockZNode    = "__LOCK__"
	kFirstTableId = 1001
)

type managerProps struct {
	NextTableId int32 `json:"next_table_id"`
}

type TablesManagerServer struct {
	pb.UnimplementedTablesManagerServer

	serviceName string
	grpcPort    int32
	httpPort    int32
	zkHosts     []string
	zkPath      string
	propsPath   string
	lockPath    string

	grpcServer     *grpc.Server
	httpServer     *http.Server
	metaStore      *metastore.ZookeeperStore
	distLeaderLock *zk.Lock

	mu       sync.RWMutex
	props    managerProps
	isLeader bool
}

func NewServer(
	serviceName string,
	grpcPort, httpPort int32,
	zkHosts []string,
	zkPath string,
) *TablesManagerServer {
	output := &TablesManagerServer{
		serviceName: "grpc_" + serviceName,
		grpcPort:    grpcPort,
		httpPort:    httpPort,
		zkHosts:     zkHosts,
		zkPath:      zkPath,
		propsPath:   fmt.Sprintf("%s/%s", zkPath, kPropsZNode),
		lockPath:    fmt.Sprintf("%s/%s", zkPath, kLockZNode),
		isLeader:    false,
	}
	output.prepareZookeeper()
	return output
}

func (s *TablesManagerServer) GetNextId(
	ctx context.Context,
	req *pb.GetNextIdRequest,
) (*pb.GetNextIdResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	output := &pb.GetNextIdResponse{}
	if !s.isLeader {
		output.Status = pb.TableManagerErrorMsg(pb.TablesManagerError_kNotLeader, "not leader")
	} else {
		output.Status = pb.ErrStatusOk()
		output.Id = s.props.NextTableId
	}
	return output, nil
}

func (s *TablesManagerServer) AllocateId(
	ctx context.Context,
	req *pb.AllocateIdRequest,
) (*pb.AllocateIdResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	output := &pb.AllocateIdResponse{}
	if !s.isLeader {
		output.Status = pb.TableManagerErrorMsg(pb.TablesManagerError_kNotLeader, "not leader")
	} else {
		output.Status = pb.ErrStatusOk()
		output.Id = s.props.NextTableId
		logging.Info("allocate id %d", output.Id)
		s.props.NextTableId++
		data := utils.MarshalJsonOrDie(&(s.props))
		succ := s.metaStore.Set(context.Background(), s.propsPath, data)
		logging.Assert(succ, "")
	}
	return output, nil

}

func (s *TablesManagerServer) prepareZookeeper() {
	acls, scheme, auth := acl.GetKeeperACLandAuthForZK()
	s.metaStore = metastore.CreateZookeeperStore(s.zkHosts, time.Second*10, acls, scheme, auth)
	logging.Assert(s.metaStore != nil, "create zookeeper store failed: %v", s.zkHosts)
	logging.Assert(s.metaStore.RecursiveCreate(context.Background(), s.lockPath), "")
	s.distLeaderLock = zk.NewLock(s.metaStore.GetRawSession(), s.lockPath, acls)
}

func (s *TablesManagerServer) acquireLeaderLock() {
	err := s.distLeaderLock.Lock()
	if err == zk.ErrClosing || err == zk.ErrConnectionClosed {
		logging.Info("zk session is closed")
		return
	}
	logging.Assert(err == nil, "acquire lock failed: %v", err)
	logging.Info("acquire leader lock succeed")

	s.afterLeaderElected()
}

func (s *TablesManagerServer) releaseLeaderLock() {
	logging.Info("release leader lock")
	s.mu.Lock()
	s.isLeader = false
	s.mu.Unlock()
	s.distLeaderLock.Unlock()
}

func (s *TablesManagerServer) afterLeaderElected() {
	s.loadNextId()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isLeader = true
}

func (s *TablesManagerServer) loadNextId() {
	data, exists, succ := s.metaStore.Get(context.Background(), s.propsPath)
	logging.Assert(succ, "")
	if exists {
		utils.UnmarshalJsonOrDie(data, &(s.props))
		logging.Info("load next id: %d", s.props.NextTableId)
	} else {
		s.props.NextTableId = kFirstTableId
		data = utils.MarshalJsonOrDie(&(s.props))
		succ := s.metaStore.Create(context.Background(), s.propsPath, data)
		logging.Assert(succ, "")
	}
}

func (s *TablesManagerServer) startGrpcServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", s.grpcPort))
	if err != nil {
		logging.Fatal("failed to listen: %d", s.grpcPort)
	}

	opts := []grpc.ServerOption{}
	s.grpcServer = grpc.NewServer(opts...)
	pb.RegisterTablesManagerServer(s.grpcServer, s)
	logging.Info("start table manager with grpc port %d", s.grpcPort)
	s.grpcServer.Serve(lis)
}

func (s *TablesManagerServer) startHttpServer() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mux := runtime.NewServeMux()
	utils.RegisterKcsPath(mux)
	utils.RegisterProfPath(mux)

	opts := []grpc.DialOption{grpc.WithInsecure()}

	endpoint := fmt.Sprintf("localhost:%d", s.grpcPort)
	err := pb.RegisterTablesManagerHandlerFromEndpoint(ctx, mux, endpoint, opts)
	if err != nil {
		logging.Fatal("register failed: %s", err.Error())
	}

	logging.Info("start to listening to http port %d", s.httpPort)
	s.httpServer = &http.Server{Addr: fmt.Sprintf(":%v", s.httpPort), Handler: mux}
	s.httpServer.ListenAndServe()
}

func (s *TablesManagerServer) Start() {
	logging.Info("start with version %s", version.GitCommitId)
	s.prepareZookeeper()
	go s.acquireLeaderLock()
	go s.startGrpcServer()
	s.startHttpServer()
}

func (s *TablesManagerServer) Stop() {
	s.releaseLeaderLock()
	logging.Info("close http port: %d", s.httpPort)
	s.httpServer.Close()
	logging.Info("close grpc port: %d", s.grpcPort)
	s.grpcServer.Stop()
	logging.Info("server stopped")
}
