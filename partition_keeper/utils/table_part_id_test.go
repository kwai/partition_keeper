package utils

import (
	"testing"

	"gotest.tools/assert"
)

func TestTablePartId(t *testing.T) {
	tpid := MakeTblPartID(1234, 5678)
	assert.Equal(t, tpid.Table(), int32(1234))
	assert.Equal(t, tpid.Part(), int32(5678))
	assert.Equal(t, tpid.String(), "t1234.p5678")
}
