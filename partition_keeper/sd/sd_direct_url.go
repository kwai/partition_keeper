package sd

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

const (
	optSourceUrl           = "source_url"
	optUsePaz              = "use_paz"
	SdTypeDirectUrl SdType = "direct_url"
)

var (
	flagKessCluster = flag.String("kess_cluster", "PRODUCTION", "kess cluster")
)

func DirectSdUsePaz(flag bool) SDFactoryOpts {
	return func(opts map[string]string) map[string]string {
		return AddKeyValue(opts, optUsePaz, Bool2String(flag))
	}
}

func DirectSdGivenUrl(url string) SDFactoryOpts {
	return func(opts map[string]string) map[string]string {
		return AddKeyValue(opts, optSourceUrl, url)
	}
}

type kessRequest struct {
	Cluster string `json:"cluster"`
	Service string `json:"service"`
}

type kws struct {
	Ksn     string `json:"ksn"`
	Stage   string `json:"stage"`
	Region  string `json:"region"`
	Az      string `json:"az"`
	Dc      string `json:"dc"`
	Catalog string `json:"catalog"`
	Version string `json:"version"`
}

type nodeDescription struct {
	ShardName       string         `json:"shardName"`
	Weight          float64        `json:"weight"`
	Host            string         `json:"host"`
	InitialLiveTime string         `json:"initialLiveTime"`
	Location        string         `json:"location"`
	Entries         map[string]int `json:"entries"`
	Payload         string         `json:"payload"`
	Kws             kws            `json:"kws"`
	Lane            string         `json:"lane"`
	Paz             string         `json:"paz"`
}

func newNodeDescription(sn *pb.ServiceNode) *nodeDescription {
	output := &nodeDescription{
		ShardName:       sn.Shard,
		Weight:          sn.Weight,
		Host:            sn.Host,
		InitialLiveTime: fmt.Sprintf("%d", sn.InitialLiveTime),
		Location:        sn.Location,
		Entries:         map[string]int{"grpc": int(sn.Port)},
		Payload:         sn.Payload,
		Kws:             kws{},
		Lane:            "",
		Paz:             sn.Paz,
	}
	if sn.Kws != nil {
		output.Kws.Region = sn.Kws.Region
		output.Kws.Az = sn.Kws.Az
		output.Kws.Dc = sn.Kws.Dc
	}
	return output
}

func (nd *nodeDescription) toServiceNode(id string, isStaging bool) (*pb.ServiceNode, error) {
	t, err := strconv.ParseInt(nd.InitialLiveTime, 10, 64)
	if err != nil {
		return nil, err
	}
	p := nd.firstPort()
	if p == -1 {
		logging.Warning("can't get port for %s", id)
		return nil, fmt.Errorf("can't get port for %s", id)
	}
	ans := &pb.ServiceNode{
		Id:              id,
		Protocol:        "grpc",
		Host:            nd.Host,
		Port:            int32(p),
		Payload:         nd.Payload,
		Weight:          nd.Weight,
		Shard:           nd.ShardName,
		Location:        nd.Location,
		InitialLiveTime: t,
		Kws: &pb.KwsInfo{
			Region: nd.Kws.Region,
			Az:     nd.Kws.Az,
			Dc:     nd.Kws.Dc,
		},
		Paz: nd.Paz,
	}
	if isStaging {
		ans.Location = "STAGING"
		ans.Kws.Region = "STAGING"
		ans.Kws.Az = "STAGING"
		ans.Kws.Dc = "STAGING"
		ans.Paz = "STAGING"
	}

	return ans, nil
}

func (nd *nodeDescription) firstPort() int {
	for _, port := range nd.Entries {
		return port
	}
	return -1
}

type kessResult struct {
	Original map[string]*nodeDescription `json:"original"`
	KwsTable string                      `json:"kwsTable"`
}

type kessResponse struct {
	Result kessResult `json:"result"`
}

type directUrlDiscovery struct {
	serviceName  string
	specifiedUrl string
	usePaz       bool
	mu           sync.Mutex
	wantAzs      map[string]bool
	regionUrls   map[string]string
}

func (d *directUrlDiscovery) getOneRegion(url string, output map[string]*pb.ServiceNode) error {
	start := time.Now()
	defer func() {
		elapse := time.Since(start).Microseconds()
		third_party.PerfLog(
			"reco",
			"reco.colossusdb.service_discovery_qps",
			d.serviceName,
			uint64(elapse),
		)
	}()

	req := kessRequest{Cluster: *flagKessCluster, Service: "grpc_" + d.serviceName}
	kr := kessResponse{}
	if err := utils.HttpPostJson(url, nil, nil, &req, &kr); err != nil {
		logging.Warning("%s: post data to kess failed: %s", d.serviceName, err.Error())
		return err
	}

	if len(kr.Result.Original) == 0 {
		logging.Warning(
			"%s: perhaps all nodes are dead or parse error, just skip this response",
			d.serviceName,
		)
		return fmt.Errorf("%s: can't get any node from %s", d.serviceName, url)
	}
	logging.Info("%s: got %d results from %s", d.serviceName, len(kr.Result.Original), url)

	isStaging := strings.HasPrefix(url, "http://kess-staging")
	for id, desc := range kr.Result.Original {
		sn, err := desc.toServiceNode(id, isStaging)
		if err != nil {
			return err
		}

		nodeAz := ""
		if d.usePaz {
			nodeAz = sn.Paz
		} else {
			nodeAz = KessLocationAz(sn.Location)
		}

		if d.wantThisAz(nodeAz) {
			logging.Verbose(1, "%s: detect node %s %s", d.serviceName, sn.Id, sn.Host)
			output[sn.Id] = sn
		} else {
			logging.Verbose(1, "%s: skip nodes %s %s from %s", d.serviceName, sn.Id, sn.Host, nodeAz)
		}
	}

	return nil
}

func (d *directUrlDiscovery) GetNodes() (map[string]*pb.ServiceNode, error) {
	output := map[string]*pb.ServiceNode{}
	if d.specifiedUrl != "" {
		err := d.getOneRegion(d.specifiedUrl, output)
		return output, err
	}

	urls := []string{}
	d.mu.Lock()
	for _, url := range d.regionUrls {
		urls = append(urls, url)
	}
	d.mu.Unlock()

	for _, url := range urls {
		err := d.getOneRegion(url, output)
		if err != nil {
			return nil, err
		}
	}
	return output, nil
}

func (d *directUrlDiscovery) wantThisAz(az string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.wantAzs[az]
}

func (d *directUrlDiscovery) updateWantAzs(azs map[string]bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.wantAzs = map[string]bool{}
	utils.GobClone(&(d.wantAzs), azs)
}

func (d *directUrlDiscovery) updateRegionUrls(azs map[string]bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	newUrls := map[string]string{}
	for az := range azs {
		region := utils.RegionOfAz(az)
		if region == "" {
			logging.Warning("%s: can't find region for az %s", d.serviceName, az)
			return fmt.Errorf("can't find region for az %s", az)
		}
		existing, ok := newUrls[region]
		if ok {
			logging.Info(
				"%s: region %s exists with %s, skip az %s",
				d.serviceName,
				region,
				existing,
				az,
			)
			continue
		}

		url, ok := third_party.GetDiscoveryUrlForRegion(region)
		if !ok {
			logging.Warning("%s: can't find kess url for region %s", d.serviceName, region)
			return fmt.Errorf("can't find kess url region target for %s", region)
		}
		logging.Info("%s: use %s as source url for region %s", d.serviceName, url, region)
		newUrls[region] = url
	}

	d.wantAzs = map[string]bool{}
	utils.GobClone(&(d.wantAzs), azs)
	d.regionUrls = newUrls
	return nil
}

func (d *directUrlDiscovery) UpdateAzs(azs map[string]bool) error {
	if d.specifiedUrl != "" {
		d.updateWantAzs(azs)
		return nil
	}
	return d.updateRegionUrls(azs)
}

func (d *directUrlDiscovery) Stop() {}

func directUrlSDFactoryCreator(
	serviceName string,
	opts map[string]string,
) ServiceDiscovery {
	usePaz := false
	if opts[optUsePaz] == "true" {
		usePaz = true
	}
	return newDirectUrlSD(serviceName, usePaz).withGivenUrl(opts[optSourceUrl])
}

func newDirectUrlSD(serviceName string, usePaz bool) *directUrlDiscovery {
	output := &directUrlDiscovery{
		serviceName: serviceName,
		usePaz:      usePaz,
	}
	return output
}

func (d *directUrlDiscovery) withGivenUrl(url string) *directUrlDiscovery {
	d.specifiedUrl = url
	return d
}

func init() {
	sdFactory[SdTypeDirectUrl] = directUrlSDFactoryCreator
}
