package third_party

import (
	"fmt"

	"google.golang.org/grpc"
)

func GetRpcClient(serviceName string) (grpc.ClientConnInterface, error) {
	return nil, fmt.Errorf("not implemented")
}
