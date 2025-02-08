package utils

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrPathIsEmpty = errors.New("path is empty")
)

type JsonNodeEditor func(input interface{}) (interface{}, error)

func concatJsonPath(path []string, tail int) string {
	return strings.Join(path[0:tail+1], "/")
}

func recursiveEditJson(
	root map[string]interface{},
	path []string,
	offset int,
	editor JsonNodeEditor,
) error {
	current := path[offset]
	sub, ok := root[current]
	if !ok {
		return fmt.Errorf("can't find node %s", concatJsonPath(path, offset))
	}
	if offset == len(path)-1 {
		output, err := editor(sub)
		if err != nil {
			return fmt.Errorf("edit %s failed: %s", concatJsonPath(path, offset), err.Error())
		}
		root[current] = output
		return nil
	}
	switch sub := sub.(type) {
	case map[string]interface{}:
		return recursiveEditJson(sub, path, offset+1, editor)
	default:
		return fmt.Errorf("%s is already leaf, can't step down", concatJsonPath(path, offset))
	}
}

func EditJson(root map[string]interface{}, path []string, editor JsonNodeEditor) error {
	if len(path) < 1 {
		return ErrPathIsEmpty
	}
	return recursiveEditJson(root, path, 0, editor)
}
