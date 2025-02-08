package utils

import (
	"testing"

	"gotest.tools/assert"
)

func TestParseNodeIndex(t *testing.T) {
	inputs := []string{
		"a.b",
		"1.c",
		"c.1",
		"2.3",
	}
	outputs := []struct {
		hub     int
		sub     int
		succeed bool
	}{
		{-1, -1, false},
		{-1, -1, false},
		{-1, -1, false},
		{2, 3, true},
	}

	for i, input := range inputs {
		h, s, err := ParseNodeIndex(input)
		assert.Equal(t, outputs[i].hub, h)
		assert.Equal(t, outputs[i].sub, s)
		assert.Equal(t, outputs[i].succeed, err == nil)
	}
}
