package utils

import (
	"testing"

	"gotest.tools/assert"
	"gotest.tools/assert/cmp"
)

func TestFromToHostPort(t *testing.T) {
	node := FromHostPort("127.0.0.1")
	assert.Assert(t, node == nil)

	node = FromHostPort("127.0.0.1:abcd")
	assert.Assert(t, node == nil)

	node = FromHostPort("127.0.0.1:1234")
	assert.Equal(t, node.NodeName, "127.0.0.1")
	assert.Equal(t, node.Port, int32(1234))
	assert.Equal(t, node.String(), "127.0.0.1:1234")

	node = FromHostPort("[fe80::1c03:59ff:feca:3721]:1234")
	assert.Equal(t, node.NodeName, "[fe80::1c03:59ff:feca:3721]")
	assert.Equal(t, node.Port, int32(1234))
	assert.Equal(t, node.String(), "[fe80::1c03:59ff:feca:3721]:1234")

	node = FromHostPort("fe80::1c03:59ff:feca:3721:1234")
	assert.Equal(t, node.NodeName, "fe80::1c03:59ff:feca:3721")
	assert.Equal(t, node.Port, int32(1234))
	assert.Equal(t, node.String(), "fe80::1c03:59ff:feca:3721:1234")

	node = FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1234")
	assert.Equal(t, node.NodeName, "dev-huyifan03-02.dev.kwaidc.com")
	assert.Equal(t, node.Port, int32(1234))
	assert.Equal(t, node.String(), "dev-huyifan03-02.dev.kwaidc.com:1234")
}

func TestFromHostPortsAndCompare(t *testing.T) {
	node := FromHostPorts([]string{})
	assert.Assert(t, cmp.Len(node, 0))

	node = FromHostPorts([]string{"dev-huyifan03-02.dev.kwaidc.com:1234", "127.0.0.1:1234"})
	expected := []*RpcNode{
		FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1234"),
		FromHostPort("127.0.0.1:1234"),
	}

	assert.Assert(t, node[0].Equals(expected[0]))
	assert.Assert(t, node[1].Equals(expected[1]))

	assert.Equal(t, node[0].Compare(expected[0]), 0)

	another := FromHostPort("dev-huyifan03-02.dev.kwaidc.com:1235")

	assert.Equal(t, node[0].Equals(another), false)
	assert.Assert(t, node[0].Compare(another) < 0)
	assert.Assert(t, another.Compare(node[0]) > 0)

	another2 := FromHostPort("dev-huyifan03-03.dev.kwaidc.com:1234")
	assert.Equal(t, node[0].Equals(another2), false)
	assert.Assert(t, node[0].Compare(another2) < 0)
	assert.Assert(t, another2.Compare(node[0]) > 0)
}

func TestClone(t *testing.T) {
	node1 := FromHostPort("127.0.0.1:1234")

	node2 := node1.Clone()
	node2.NodeName = "128.0.0.1"
	assert.Assert(t, node1.Compare(node2) != 0)

	node3 := node1.Clone()
	node3.Port++
	assert.Assert(t, node1.Compare(node3) != 0)

	assert.Equal(t, node1.String(), "127.0.0.1:1234")
}
