package checkpoint

import (
	"fmt"
	"strconv"
	"strings"
)

type checkpointMeta struct {
	prefix    string
	tableId   int32
	sessionId int64
	timestamp int64
}

func parseFromCheckpointPath(path string) (*checkpointMeta, error) {
	// parse path if it's format is /prefix/table_<id>/<session>_<ts>
	output := &checkpointMeta{}

	pos := strings.LastIndex(path, "/table_")
	if pos == -1 {
		return nil, fmt.Errorf("can't find /table_ in checkpoint path %s", path)
	}
	output.prefix = path[0:pos]
	tokens := strings.Split(path[pos+7:], "/")
	if len(tokens) != 2 {
		return nil, fmt.Errorf("not 2 sections after table_ in checkpoint path %s", path)
	}
	ans, err := strconv.ParseInt(tokens[0], 10, 64)
	if err != nil {
		return nil, err
	}
	output.tableId = int32(ans)

	tsTokens := strings.Split(tokens[1], "_")
	if len(tsTokens) != 2 {
		return nil, fmt.Errorf("not 2 sections in leaf of checkpoint path %s", path)
	}
	ans, err = strconv.ParseInt(tsTokens[0], 10, 64)
	if err != nil {
		return nil, err
	}
	output.sessionId = ans

	ans, err = strconv.ParseInt(tsTokens[1], 10, 64)
	if err != nil {
		return nil, err
	}
	output.timestamp = ans
	return output, nil
}

func (c *checkpointMeta) normalPrefix() string {
	return strings.TrimSuffix(c.prefix, "/")
}

func (c *checkpointMeta) concatTableCheckpointPath() string {
	return fmt.Sprintf("%s/table_%d", c.normalPrefix(), c.tableId)
}

func (c *checkpointMeta) concatSessionCheckpointPath() string {
	return fmt.Sprintf("%s/table_%d/%d_%d", c.normalPrefix(), c.tableId, c.sessionId, c.timestamp)
}
