package third_party

// test in this file are commented as kconf is now allowed to
// access with api too frequently, when you
// update table_traffic_kconf.go, you can uncomment these codes and test them manually

import (
	"testing"
)

func TestGetTrafficConfig(t *testing.T) {
	// logging.Info("read non-exist config")
	// path := "colossus.traffic.not_exist_kconf"
	// traffic, version, err := GetTrafficConfig(path)
	// assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))
	// logging.Info("error msg: %v", err.Message)
	// assert.Assert(t, traffic == nil)
	// assert.Equal(t, version, int64(-1))

	// logging.Info("read valid config")
	// path := "colossus.traffic.test_model"
	// traffic, version, err := GetTrafficConfig(path)
	// assert.Assert(t, err == nil)
	// logging.Info("snapshot version: %d", version)
	// assert.Assert(t, version > 0)

	// assert.Equal(t, len(traffic.Traffic), 2)
	// assert.Equal(t, traffic.Traffic["test_table1"].KessName,
	//	 "grpc_reco-kuaishou-platform-colossusdb-sim-server-video-item-test_video-item-test")
	// assert.Equal(t, traffic.Traffic["test_table1"].TableName, "")

	// assert.Equal(t, traffic.Traffic["test_table2"].KessName,
	//	 "grpc_KUAISHOU_PLATFORM_feasury-action-follow-01_table-follow-512")
	// assert.Equal(t, traffic.Traffic["test_table2"].TableName, "central")

	// logging.Info("read empty json config")
	// traffic, version, err := GetTrafficConfig("colossus.traffic.oneboxModel")
	// logging.Info("version: %d, err: %v, traffic: %v", version, err, traffic)
	// assert.Assert(t, err == nil)
	// assert.Equal(t, len(traffic.Traffic), 0)

	// logging.Info("read null config")
	// traffic, version, err := GetTrafficConfig("colossus.traffic.nullModel")
	// logging.Info("version: %d, err: %v, traffic: %v", version, err, traffic)
	// assert.Equal(t, err.Code, int32(pb.AdminError_kInvalidParameter))
}

func TestUpdateTrafficConfig(t *testing.T) {
	// path := "colossus.traffic.test_model"
	// err := AddTrafficConfig(path, "grpc_test_service", "test_table_123")
	// logging.Info("error: %v", err)
	// assert.Assert(t, err == nil)
}
