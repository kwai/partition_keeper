package utils

import (
	"testing"

	"gotest.tools/assert"
)

func TestAzConvert(t *testing.T) {
	assert.Equal(t, KrpAzToStandard("_YZ"), "YZ")
	assert.Equal(t, KrpAzToStandard("_yz"), "YZ")
	assert.Equal(t, KrpAzToStandard("YZ"), "YZ")
	assert.Equal(t, KrpAzToStandard("yz"), "YZ")
}
