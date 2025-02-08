package utils

import (
	"fmt"
	"sort"
	"strings"

	"github.com/go-zookeeper/zk"
)

func Min(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func Max(left, right int) int {
	if left < right {
		return right
	}
	return left
}

func FindIndex(items []string, target string) int {
	for i, item := range items {
		if target == item {
			return i
		}
	}
	return -1
}

func Bool2Int(flag bool) int {
	if flag {
		return 1
	}
	return 0
}

func Bool2Str(flag bool) string {
	if flag {
		return "true"
	}
	return "false"
}

func FindMinIndex(count int, less func(i, j int) bool) int {
	ans := -1

	for i := 0; i < count; i++ {
		if ans == -1 {
			ans = i
			continue
		}
		if less(i, ans) {
			ans = i
		}
	}

	return ans
}

func StringsKey(items []string) string {
	var tmp []string
	tmp = append(tmp, items...)
	sort.Strings(tmp)
	return strings.Join(tmp, ",")
}

func StringsAcl(items []zk.ACL) string {
	var sortitems []string
	for i := 0; i < len(items); i++ {
		var tmp string
		tmp += fmt.Sprint(items[i].Perms) + ":"
		tmp += items[i].Scheme + ":"
		tmp += items[i].ID
		sortitems = append(sortitems, tmp)
	}

	sort.Strings(sortitems)
	return strings.Join(sortitems, ",")
}

func Diff(a, b []string) (left, shared, right []string) {
	aset := map[string]bool{}
	for _, item := range a {
		aset[item] = true
	}
	for _, item := range b {
		if _, ok := aset[item]; ok {
			shared = append(shared, item)
			delete(aset, item)
		} else {
			right = append(right, item)
		}
	}
	for item := range aset {
		left = append(left, item)
	}
	return
}
