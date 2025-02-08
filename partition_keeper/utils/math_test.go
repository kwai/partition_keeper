package utils

import (
	"testing"

	"gotest.tools/assert"
)

func TestGcd(t *testing.T) {
	testCases := []struct {
		a, b, result int
	}{
		{1, 5, 1},
		{100, 20, 20},
		{15, 20, 5},
	}
	for i := range testCases {
		c := &testCases[i]
		assert.Equal(t, Gcd(c.a, c.b), c.result)
	}
}
