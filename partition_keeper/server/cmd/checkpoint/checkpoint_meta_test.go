package checkpoint

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
	"gotest.tools/assert"
)

func TestParseFromCheckpointPath(t *testing.T) {
	invalidPaths := []string{
		"/hello/world",
		"/hello/world/table_good",
		"/hello/world/table_good/luck",
		"/hello/world/table_1/good_luck",
		"/hello/world/table_1001/12345_good_luck",
		"/hello/world/table_1001/12345_good",
	}

	for _, path := range invalidPaths {
		ans, err := parseFromCheckpointPath(path)
		logging.Info(path)
		assert.Assert(t, ans == nil)
		assert.Assert(t, err != nil)
	}

	validPath := "/hello/world/table_123456/11111_22222"
	ans, err := parseFromCheckpointPath(validPath)
	assert.NilError(t, err)
	assert.DeepEqual(t, ans, &checkpointMeta{
		prefix:    "/hello/world",
		tableId:   123456,
		sessionId: 11111,
		timestamp: 22222,
	}, cmp.AllowUnexported(checkpointMeta{}))

	assert.Equal(t, ans.concatSessionCheckpointPath(), validPath)
	assert.Equal(t, ans.concatTableCheckpointPath(), "/hello/world/table_123456")
}
