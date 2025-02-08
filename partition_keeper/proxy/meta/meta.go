package meta

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/pb"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/third_party"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/utils"
)

var (
	FlagKconfPath = flag.String("proxy_kconf_path", "", "kconf path")
)

type MetaProps struct {
	Namespaces    []string `json:"namespaces"`
	GrayNamespace string   `json:"grayNamespace"`
}

type Meta struct {
	// need lock
	services      map[string]string                 // service_name -> namespace
	tables        map[string]bool                   // table_name
	types         map[pb.ServiceType]map[string]int // service_type -> namespace -> service nums
	namespaces    map[string]string                 // namespace -> url
	kconfChange   int32
	lock          *utils.LooseLock
	grayNamespace string
}

func NewMeta() *Meta {
	ans := &Meta{
		services:      make(map[string]string),
		types:         make(map[pb.ServiceType]map[string]int),
		tables:        make(map[string]bool),
		namespaces:    make(map[string]string),
		lock:          utils.NewLooseLock(),
		kconfChange:   0,
		grayNamespace: "",
	}
	return ans
}

func (m *Meta) GetNamespaces() []string {
	var namespaces []string
	m.lock.LockRead()
	defer m.lock.UnlockRead()
	for k := range m.namespaces {
		namespaces = append(namespaces, k)
	}
	return namespaces
}

func (m *Meta) GetServices() []string {
	var services []string
	m.lock.LockRead()
	defer m.lock.UnlockRead()
	for k := range m.services {
		services = append(services, k)
	}
	return services
}

func (m *Meta) GetUrlByService(service string) string {
	result := ""
	m.lock.LockRead()
	defer m.lock.UnlockRead()

	if namespace, ok := m.services[service]; ok {
		result = m.namespaces[namespace]
	}
	return result
}

func (m *Meta) GetUrlByType(serviceType pb.ServiceType) string {
	minNamespace := ""
	m.lock.LockRead()
	defer m.lock.UnlockRead()

	if namespaces, ok := m.types[serviceType]; ok {
		// Get the url with the least number of services
		minNum := -1
		for namespace, num := range namespaces {
			if m.grayNamespace == namespace {
				if num == 0 {
					minNamespace = namespace
					break
				}
				continue
			}
			if minNum == -1 {
				minNum = num
				minNamespace = namespace
				continue
			}

			if num < minNum {
				minNum = num
				minNamespace = namespace
			} else if num == minNum && namespace < minNamespace {
				minNamespace = namespace
			}
		}
	}
	return m.namespaces[minNamespace]
}

func (m *Meta) GetAllUrls() []string {
	var urls []string
	m.lock.LockRead()
	defer m.lock.UnlockRead()
	for _, v := range m.namespaces {
		urls = append(urls, v)
	}
	return urls
}

func (m *Meta) TableExist(table string) bool {
	m.lock.LockRead()
	defer m.lock.UnlockRead()

	_, ok := m.tables[table]
	return ok
}

func (m *Meta) AddServices(services map[string]pb.ServiceType, namespace string) {
	if len(services) == 0 {
		return
	}
	m.lock.LockWrite()
	defer m.lock.UnlockWrite()

	for service, serviceType := range services {
		m.services[service] = namespace
		if keeper, ok := m.types[serviceType]; ok {
			if value, ok := keeper[namespace]; ok {
				keeper[namespace] = value + 1
				continue
			}
		} else {
			third_party.PerfLog3(
				"reco",
				"reco.colossusdb.keeper_proxy",
				"service_type_error",
				pb.ServiceType_name[int32(serviceType)],
				service,
				namespace,
				1,
			)
			logging.Error(
				"Can't find the namespace for the service type:%s, please check the kconf:%s",
				pb.ServiceType_name[int32(serviceType)],
				*FlagKconfPath,
			)
		}
	}
}

func (m *Meta) DelServices(services map[string]pb.ServiceType, namespace string) {
	if len(services) == 0 {
		return
	}
	m.lock.LockWrite()
	defer m.lock.UnlockWrite()

	for service, serviceType := range services {
		delete(m.services, service)
		if keeper, ok := m.types[serviceType]; ok {
			value := keeper[namespace]
			keeper[namespace] = value - 1
		} else {
			third_party.PerfLog3(
				"reco",
				"reco.colossusdb.keeper_proxy",
				"service_type_error",
				pb.ServiceType_name[int32(serviceType)],
				service,
				namespace,
				1,
			)
			logging.Error(
				"Can't find the namespace for the service type:%s, please check the kconf:%s",
				pb.ServiceType_name[int32(serviceType)],
				*FlagKconfPath,
			)
		}
	}
}

func (m *Meta) AddTables(tables []string) {
	if len(tables) == 0 {
		return
	}

	m.lock.LockWrite()
	defer m.lock.UnlockWrite()
	for _, table := range tables {
		m.tables[table] = true
	}
}
func (m *Meta) DelTables(tables []string) {
	if len(tables) == 0 {
		return
	}
	m.lock.LockWrite()
	defer m.lock.UnlockWrite()
	for _, table := range tables {
		delete(m.tables, table)
	}
}

func (m *Meta) HostChange(host string, namespace string) {
	m.lock.LockWrite()
	defer m.lock.UnlockWrite()
	m.namespaces[namespace] = fmt.Sprintf("http://%s", host)
}

func (m *Meta) addNamespace(s string) bool {
	index := strings.Index(s, ":")
	if index == -1 {
		logging.Error("Invalid namespace string :%s", s)
		return false
	}
	serviceString := s[0:index]
	namespaceString := s[index+1:]
	namespaces := strings.Split(namespaceString, ",")
	serviceType := pb.ServiceType(pb.ServiceType_value[serviceString])

	if len(namespaces) == 0 || serviceType == pb.ServiceType_invalid {
		logging.Error("Invalid service type or namespace:%s", s)
		return false
	}

	m.lock.LockWrite()
	defer m.lock.UnlockWrite()

	for _, namespace := range namespaces {
		m.namespaces[namespace] = ""
		if keepers, ok := m.types[serviceType]; ok {
			keepers[namespace] = 0
		} else {
			tmp := make(map[string]int)
			tmp[namespace] = 0
			m.types[serviceType] = tmp
		}
	}

	return true
}

func (m *Meta) OnValueRemoved(key string) error {
	return nil
}
func (m *Meta) OnStringValueUpdated(key string, data string) error {
	logging.Info("proxy kconf %s change", key)
	value := &MetaProps{}
	err := json.Unmarshal([]byte(data), value)
	logging.Assert(err == nil, "kconf %s is not valid : %v", *FlagKconfPath, err)

	// clean all
	m.lock.LockWrite()
	m.namespaces = make(map[string]string)
	m.types = make(map[pb.ServiceType]map[string]int)
	m.services = make(map[string]string)
	m.tables = make(map[string]bool)
	m.grayNamespace = ""
	m.lock.UnlockWrite()

	for _, namespace := range value.Namespaces {
		if !m.addNamespace(namespace) {
			third_party.PerfLog2(
				"reco",
				"reco.colossusdb.keeper_proxy",
				"kconf_error",
				*FlagKconfPath,
				namespace,
				uint64(1),
			)
			return errors.New("add namespace failed")
		}
	}

	if value.GrayNamespace != "" {
		m.grayNamespace = value.GrayNamespace
		logging.Info("set gray namespace %s", m.grayNamespace)
	}

	m.SetKconfChange(1)
	return nil
}

func (m *Meta) SetKconfChange(val int32) {
	atomic.StoreInt32(&m.kconfChange, val)
}
func (m *Meta) GetKconfChange() bool {
	return atomic.LoadInt32(&m.kconfChange) != 0
}

func (m *Meta) Initialize() {
	err := third_party.ConfigAddStringWatcher(*FlagKconfPath, m)
	logging.Assert(err == nil, "watch kconf path %s failed: %v", *FlagKconfPath, err)
}
