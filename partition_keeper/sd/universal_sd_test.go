package sd

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"gotest.tools/assert"
)

func TestUniversalSdSingleton(t *testing.T) {
	sdInstance := make(chan *UniversalDiscovery, 10)
	for i := 0; i < 10; i++ {
		go func() {
			sdInstance <- GetUniversalDiscoveryInstance(false)
		}()
	}

	var got *UniversalDiscovery = nil
	got = <-sdInstance
	assert.Assert(t, got != nil)
	for i := 1; i < 10; i++ {
		ud := <-sdInstance
		assert.Equal(t, got, ud)
	}
	got.Stop()
}

func TestUniversalSdPazSingleton(t *testing.T) {
	sdInstance := make(chan *UniversalDiscovery, 10)
	for i := 0; i < 10; i++ {
		go func() {
			sdInstance <- GetUniversalDiscoveryInstance(true)
		}()
	}

	var got *UniversalDiscovery = nil
	got = <-sdInstance
	assert.Assert(t, got != nil)
	for i := 1; i < 10; i++ {
		ud := <-sdInstance
		assert.Equal(t, got, ud)
	}
	got.Stop()
}

func TestUniversalDiscoveryUpdateAz(t *testing.T) {
	usePazs := []bool{false, true}
	for _, usePaz := range usePazs {
		ud := newUniversalDiscovery("", usePaz)
		azs := map[string]bool{"GZ1": true, "YZ": true}
		if usePaz {
			azs = map[string]bool{"HB1AZ4": true, "HB1AZ1": true}
		}
		err := ud.UpdateAzs(nil, azs)
		assert.NilError(t, err)

		refCount := map[string]int{"GZ1": 1, "YZ": 1}
		if usePaz {
			refCount = map[string]int{"HB1AZ4": 1, "HB1AZ1": 1}
		}
		assert.DeepEqual(t, ud.azRefs, refCount)

		azs = map[string]bool{"YZ": true}
		if usePaz {
			azs = map[string]bool{"HB1AZ1": true}
		}
		err = ud.UpdateAzs(nil, azs)
		assert.NilError(t, err)

		refCount = map[string]int{"GZ1": 1, "YZ": 2}
		if usePaz {
			refCount = map[string]int{"HB1AZ4": 1, "HB1AZ1": 2}
		}
		assert.DeepEqual(t, ud.azRefs, refCount)

		oldAzs := map[string]bool{"YZ": true}
		newAzs := map[string]bool{"YZ": true, "INVALID": true}
		curAzs := map[string]bool{"GZ1": true, "YZ": true}
		if usePaz {
			oldAzs = map[string]bool{"HB1AZ1": true}
			newAzs = map[string]bool{"HB1AZ1": true, "INVALID": true}
			curAzs = map[string]bool{"HB1AZ4": true, "HB1AZ1": true}
		}
		err = ud.UpdateAzs(oldAzs, newAzs)
		assert.Assert(t, err != nil)
		assert.DeepEqual(t, ud.azRefs, refCount)
		assert.DeepEqual(t, ud.getCurrentAzSet(), curAzs)

		oldAzs = map[string]bool{"GZ1": true}
		curAzs = map[string]bool{"YZ": true}
		refCount = map[string]int{"YZ": 2}
		if usePaz {
			oldAzs = map[string]bool{"HB1AZ4": true}
			curAzs = map[string]bool{"HB1AZ1": true}
			refCount = map[string]int{"HB1AZ1": 2}
		}
		err = ud.updateAzs(oldAzs, nil)
		assert.NilError(t, err)

		assert.DeepEqual(t, ud.azRefs, refCount)
		assert.DeepEqual(t, ud.getCurrentAzSet(), curAzs)
	}
}

type universalDiscoveryTestEnv struct {
	targetUrl string
	mockKess  *DiscoveryMockServer
}

func setupUniversalDiscoveryTestEnv(port int) *universalDiscoveryTestEnv {
	output := &universalDiscoveryTestEnv{
		targetUrl: fmt.Sprintf("http://127.0.0.1:%d", port),
		mockKess:  NewDiscoveryMockServer(),
	}
	output.mockKess.Start(port)
	time.Sleep(time.Millisecond * 200)
	return output
}

func (te *universalDiscoveryTestEnv) stop() {
	te.mockKess.Stop()
}

func (te *universalDiscoveryTestEnv) addNode(ipPort, location, paz string) {
	sn := createTestServiceNode(ipPort, location, paz)
	te.mockKess.UpdateServiceNode(sn)
}

func TestUniversalDiscoveryGetNode(t *testing.T) {
	logging.Info("start to run TestUniversalDiscoveryGetNode")
	usePazs := []bool{false, true}
	for _, usePaz := range usePazs {
		targetPort := 5555
		if usePaz {
			targetPort = 5777
		}
		te := setupUniversalDiscoveryTestEnv(targetPort)
		defer te.stop()

		startPort := 1001

		if usePaz {
			startPort = 2001
		}
		te.addNode(fmt.Sprintf("127.0.0.1:%d", startPort), "BJ.YZ", "HB1AZ1")
		te.addNode(fmt.Sprintf("127.0.0.1:%d", startPort+100), "BJ.ZW", "HB1AZ2")
		te.addNode(fmt.Sprintf("127.0.0.1:%d", startPort+101), "BJ.ZW", "HB1AZ2")
		te.addNode(fmt.Sprintf("127.0.0.1:%d", startPort+102), "BJ.ZW", "HB1AZ2")
		te.addNode(fmt.Sprintf("127.0.0.1:%d", startPort+200), "HN.GZ1", "HB1AZ3")
		te.addNode(fmt.Sprintf("127.0.0.1:%d", startPort+300), "SGP.TXNSGP1", "HB1AZ4")

		ud := newUniversalDiscovery(fmt.Sprintf("http://127.0.0.1:%d", targetPort), usePaz)
		ud.WaitUpdateRoundBeyond(0)

		getAz := "YZ"
		getAz2 := "TXNSGP1"
		if usePaz {
			getAz = "HB1AZ1"
			getAz2 = "HB1AZ4"
		}
		assert.Assert(t, ud.GetNode(getAz, 0) == nil)
		updateAzs := map[string]bool{"YZ": true, "ZW": true}
		if usePaz {
			updateAzs = map[string]bool{"HB1AZ1": true, "HB1AZ2": true}
		}
		ud.UpdateAzs(nil, updateAzs)
		ud.WaitUpdateRoundBeyond(1)

		assert.Assert(t, ud.GetNode(getAz2, 0) == nil)
		assert.Equal(t, ud.GetNode(getAz, 0).Port, int32(startPort))
		assert.Equal(t, ud.GetNode(getAz, 10).Port, int32(startPort))

		getAz = "ZW"
		if usePaz {
			getAz = "HB1AZ2"
		}
		port := []int{}
		for i := 0; i < 3; i++ {
			port = append(port, int(ud.GetNode(getAz, uint32(i)).Port))
		}
		sort.Ints(port)
		expect := []int{startPort + 100, startPort + 101, startPort + 102}
		assert.DeepEqual(t, port, expect)
	}
}
