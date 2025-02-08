package utils

import (
	"testing"

	"gotest.tools/assert"
)

func TestCheckNames(t *testing.T) {
	assert.Assert(t, !IsValidName(""))
	assert.Assert(t, !IsValidName("^"))
	assert.Assert(t, !IsValidName("/hello"))
	assert.Assert(t, !IsValidName("hello/world"))
	assert.Assert(t, !IsValidName("hello\\world"))
	assert.Assert(t, !IsValidName("hello#world"))
	assert.Assert(t, IsValidName("hello"))
	assert.Assert(t, IsValidName("hello_world"))
	assert.Assert(t, IsValidName("hello-world"))
	assert.Assert(t, IsValidName("hello.world"))
	assert.Assert(t, IsValidName("12345.hello"))
}
