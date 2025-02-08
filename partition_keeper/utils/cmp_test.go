package utils

import (
	"sort"
	"testing"

	"gotest.tools/assert"
)

func TestDiff(t *testing.T) {
	left, shared, right := Diff([]string{}, []string{})
	assert.Equal(t, len(left), 0)
	assert.Equal(t, len(right), 0)
	assert.Equal(t, len(shared), 0)

	left, shared, right = Diff([]string{}, []string{"a", "b"})
	assert.Equal(t, len(left), 0)
	assert.Equal(t, len(shared), 0)
	assert.DeepEqual(t, right, []string{"a", "b"})

	left, shared, right = Diff([]string{"a", "b"}, []string{})
	assert.Equal(t, len(right), 0)
	assert.Equal(t, len(shared), 0)
	sort.Strings(left)
	assert.DeepEqual(t, left, []string{"a", "b"})

	left, shared, right = Diff([]string{"a", "b", "c"}, []string{"c", "b", "d", "e"})
	sort.Strings(left)
	sort.Strings(shared)
	sort.Strings(right)
	assert.DeepEqual(t, left, []string{"a"})
	assert.DeepEqual(t, shared, []string{"b", "c"})
	assert.DeepEqual(t, right, []string{"d", "e"})

	left, shared, right = Diff([]string{"a", "bb", "c"}, []string{"cc", "dd"})
	sort.Strings(left)
	sort.Strings(shared)
	sort.Strings(right)
	assert.DeepEqual(t, left, []string{"a", "bb", "c"})
	assert.Equal(t, len(shared), 0)
	assert.DeepEqual(t, right, []string{"cc", "dd"})

	left, shared, right = Diff([]string{"bb", "aa", "cc"}, []string{"cc"})
	sort.Strings(left)
	sort.Strings(shared)
	sort.Strings(right)
	assert.DeepEqual(t, left, []string{"aa", "bb"})
	assert.DeepEqual(t, shared, []string{"cc"})
	assert.Equal(t, len(right), 0)

	left, shared, right = Diff([]string{"cc"}, []string{"bb", "aa", "cc"})
	sort.Strings(left)
	sort.Strings(shared)
	sort.Strings(right)
	assert.Equal(t, len(left), 0)
	assert.DeepEqual(t, shared, []string{"cc"})
	assert.DeepEqual(t, right, []string{"aa", "bb"})
}
