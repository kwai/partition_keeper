package node_mgr

import (
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestDeadNodeAlert(t *testing.T) {
	nodeAlert := NewNodeAlert("ut_test_service")
	alertTime := nodeAlert.alertTime
	nodeAlert.deadNodesAlert(0, "")
	assert.Equal(t, alertTime == nodeAlert.alertTime, true)

	nodeAlert.alertTime = nodeAlert.alertTime - *flagNodeAlertRecoveryMinutes*60*2
	nodeAlert.deadNodesAlert(2, "host1:123,host2:123")
	now := time.Now().Unix()
	assert.Equal(t, nodeAlert.alertTime >= now, true)
	alertTime = nodeAlert.alertTime
	nodeAlert.deadNodesAlert(2, "host1:123,host2:123")
	assert.Equal(t, alertTime == nodeAlert.alertTime, true)
}
