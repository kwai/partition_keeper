package third_party

import (
	"fmt"
)

type HostInfo struct {
	Az      string `json:"az"`
	Dc      string `json:"dc"`
	IdcName string `json:"idc_name"`
	Region  string `json:"region"`
}

func QueryHostInfo(hostname string) (*HostInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func QueryHostsInfo(hosts []string) ([]*HostInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
