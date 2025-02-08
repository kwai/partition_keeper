// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.0
// 	protoc        (unknown)
// source: teams/reco-arch/colossusdb/proto/route.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type RouteFormat int32

const (
	RouteFormat_JSON       RouteFormat = 0
	RouteFormat_GZIPPED_PB RouteFormat = 1
)

// Enum value maps for RouteFormat.
var (
	RouteFormat_name = map[int32]string{
		0: "JSON",
		1: "GZIPPED_PB",
	}
	RouteFormat_value = map[string]int32{
		"JSON":       0,
		"GZIPPED_PB": 1,
	}
)

func (x RouteFormat) Enum() *RouteFormat {
	p := new(RouteFormat)
	*p = x
	return p
}

func (x RouteFormat) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (RouteFormat) Descriptor() protoreflect.EnumDescriptor {
	return file_teams_reco_arch_colossusdb_proto_route_proto_enumTypes[0].Descriptor()
}

func (RouteFormat) Type() protoreflect.EnumType {
	return &file_teams_reco_arch_colossusdb_proto_route_proto_enumTypes[0]
}

func (x RouteFormat) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use RouteFormat.Descriptor instead.
func (RouteFormat) EnumDescriptor() ([]byte, []int) {
	return file_teams_reco_arch_colossusdb_proto_route_proto_rawDescGZIP(), []int{0}
}

type PartitionLocation struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Replicas     []*ReplicaLocation `protobuf:"bytes,1,rep,name=replicas,proto3" json:"replicas"`
	Version      int64              `protobuf:"varint,2,opt,name=version,proto3" json:"version"`
	SplitVersion int32              `protobuf:"varint,3,opt,name=split_version,json=splitVersion,proto3" json:"split_version,omitempty"`
}

func (x *PartitionLocation) Reset() {
	*x = PartitionLocation{}
	if protoimpl.UnsafeEnabled {
		mi := &file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PartitionLocation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PartitionLocation) ProtoMessage() {}

func (x *PartitionLocation) ProtoReflect() protoreflect.Message {
	mi := &file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PartitionLocation.ProtoReflect.Descriptor instead.
func (*PartitionLocation) Descriptor() ([]byte, []int) {
	return file_teams_reco_arch_colossusdb_proto_route_proto_rawDescGZIP(), []int{0}
}

func (x *PartitionLocation) GetReplicas() []*ReplicaLocation {
	if x != nil {
		return x.Replicas
	}
	return nil
}

func (x *PartitionLocation) GetVersion() int64 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *PartitionLocation) GetSplitVersion() int32 {
	if x != nil {
		return x.SplitVersion
	}
	return 0
}

type ServerLocation struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Host string `protobuf:"bytes,1,opt,name=host,proto3" json:"host"`
	Port int32  `protobuf:"varint,2,opt,name=port,proto3" json:"port"`
	// hub_id refers to one entry in RouteEntries.replica_hubs
	HubId int32             `protobuf:"varint,3,opt,name=hub_id,json=hubId,proto3" json:"hub_id"`
	Info  map[string]string `protobuf:"bytes,4,rep,name=info,proto3" json:"info" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Ip    string            `protobuf:"bytes,5,opt,name=ip,proto3" json:"ip"`
	Alive bool              `protobuf:"varint,6,opt,name=alive,proto3" json:"alive"`
	Op    AdminNodeOp       `protobuf:"varint,7,opt,name=op,proto3,enum=colossusdb.AdminNodeOp" json:"op"`
}

func (x *ServerLocation) Reset() {
	*x = ServerLocation{}
	if protoimpl.UnsafeEnabled {
		mi := &file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ServerLocation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ServerLocation) ProtoMessage() {}

func (x *ServerLocation) ProtoReflect() protoreflect.Message {
	mi := &file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ServerLocation.ProtoReflect.Descriptor instead.
func (*ServerLocation) Descriptor() ([]byte, []int) {
	return file_teams_reco_arch_colossusdb_proto_route_proto_rawDescGZIP(), []int{1}
}

func (x *ServerLocation) GetHost() string {
	if x != nil {
		return x.Host
	}
	return ""
}

func (x *ServerLocation) GetPort() int32 {
	if x != nil {
		return x.Port
	}
	return 0
}

func (x *ServerLocation) GetHubId() int32 {
	if x != nil {
		return x.HubId
	}
	return 0
}

func (x *ServerLocation) GetInfo() map[string]string {
	if x != nil {
		return x.Info
	}
	return nil
}

func (x *ServerLocation) GetIp() string {
	if x != nil {
		return x.Ip
	}
	return ""
}

func (x *ServerLocation) GetAlive() bool {
	if x != nil {
		return x.Alive
	}
	return false
}

func (x *ServerLocation) GetOp() AdminNodeOp {
	if x != nil {
		return x.Op
	}
	return AdminNodeOp_kNoop
}

type TableInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	HashMethod   string `protobuf:"bytes,1,opt,name=hash_method,json=hashMethod,proto3" json:"hash_method"`
	SplitVersion int32  `protobuf:"varint,2,opt,name=split_version,json=splitVersion,proto3" json:"split_version,omitempty"`
	UsePaz       bool   `protobuf:"varint,3,opt,name=use_paz,json=usePaz,proto3" json:"use_paz,omitempty"`
}

func (x *TableInfo) Reset() {
	*x = TableInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TableInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TableInfo) ProtoMessage() {}

func (x *TableInfo) ProtoReflect() protoreflect.Message {
	mi := &file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TableInfo.ProtoReflect.Descriptor instead.
func (*TableInfo) Descriptor() ([]byte, []int) {
	return file_teams_reco_arch_colossusdb_proto_route_proto_rawDescGZIP(), []int{2}
}

func (x *TableInfo) GetHashMethod() string {
	if x != nil {
		return x.HashMethod
	}
	return ""
}

func (x *TableInfo) GetSplitVersion() int32 {
	if x != nil {
		return x.SplitVersion
	}
	return 0
}

func (x *TableInfo) GetUsePaz() bool {
	if x != nil {
		return x.UsePaz
	}
	return false
}

type EntriesBrief struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	PartitionCount int32 `protobuf:"varint,1,opt,name=partition_count,json=partitionCount,proto3" json:"partition_count"`
}

func (x *EntriesBrief) Reset() {
	*x = EntriesBrief{}
	if protoimpl.UnsafeEnabled {
		mi := &file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EntriesBrief) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EntriesBrief) ProtoMessage() {}

func (x *EntriesBrief) ProtoReflect() protoreflect.Message {
	mi := &file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EntriesBrief.ProtoReflect.Descriptor instead.
func (*EntriesBrief) Descriptor() ([]byte, []int) {
	return file_teams_reco_arch_colossusdb_proto_route_proto_rawDescGZIP(), []int{3}
}

func (x *EntriesBrief) GetPartitionCount() int32 {
	if x != nil {
		return x.PartitionCount
	}
	return 0
}

type RouteMeta struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Format       RouteFormat   `protobuf:"varint,1,opt,name=format,proto3,enum=colossusdb.RouteFormat" json:"format"`
	RouteId      string        `protobuf:"bytes,2,opt,name=route_id,json=routeId,proto3" json:"route_id"`
	SegmentCount int32         `protobuf:"varint,3,opt,name=segment_count,json=segmentCount,proto3" json:"segment_count"`
	SegmentId    int32         `protobuf:"varint,4,opt,name=segment_id,json=segmentId,proto3" json:"segment_id"`
	Brief        *EntriesBrief `protobuf:"bytes,5,opt,name=brief,proto3" json:"brief"`
}

func (x *RouteMeta) Reset() {
	*x = RouteMeta{}
	if protoimpl.UnsafeEnabled {
		mi := &file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RouteMeta) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RouteMeta) ProtoMessage() {}

func (x *RouteMeta) ProtoReflect() protoreflect.Message {
	mi := &file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RouteMeta.ProtoReflect.Descriptor instead.
func (*RouteMeta) Descriptor() ([]byte, []int) {
	return file_teams_reco_arch_colossusdb_proto_route_proto_rawDescGZIP(), []int{4}
}

func (x *RouteMeta) GetFormat() RouteFormat {
	if x != nil {
		return x.Format
	}
	return RouteFormat_JSON
}

func (x *RouteMeta) GetRouteId() string {
	if x != nil {
		return x.RouteId
	}
	return ""
}

func (x *RouteMeta) GetSegmentCount() int32 {
	if x != nil {
		return x.SegmentCount
	}
	return 0
}

func (x *RouteMeta) GetSegmentId() int32 {
	if x != nil {
		return x.SegmentId
	}
	return 0
}

func (x *RouteMeta) GetBrief() *EntriesBrief {
	if x != nil {
		return x.Brief
	}
	return nil
}

type RouteEntries struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TableInfo   *TableInfo           `protobuf:"bytes,1,opt,name=table_info,json=tableInfo,proto3" json:"table_info"`
	Partitions  []*PartitionLocation `protobuf:"bytes,2,rep,name=partitions,proto3" json:"partitions"`
	Servers     []*ServerLocation    `protobuf:"bytes,3,rep,name=servers,proto3" json:"servers"`
	ReplicaHubs []*ReplicaHub        `protobuf:"bytes,4,rep,name=replica_hubs,json=replicaHubs,proto3" json:"replica_hubs"`
}

func (x *RouteEntries) Reset() {
	*x = RouteEntries{}
	if protoimpl.UnsafeEnabled {
		mi := &file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RouteEntries) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RouteEntries) ProtoMessage() {}

func (x *RouteEntries) ProtoReflect() protoreflect.Message {
	mi := &file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RouteEntries.ProtoReflect.Descriptor instead.
func (*RouteEntries) Descriptor() ([]byte, []int) {
	return file_teams_reco_arch_colossusdb_proto_route_proto_rawDescGZIP(), []int{5}
}

func (x *RouteEntries) GetTableInfo() *TableInfo {
	if x != nil {
		return x.TableInfo
	}
	return nil
}

func (x *RouteEntries) GetPartitions() []*PartitionLocation {
	if x != nil {
		return x.Partitions
	}
	return nil
}

func (x *RouteEntries) GetServers() []*ServerLocation {
	if x != nil {
		return x.Servers
	}
	return nil
}

func (x *RouteEntries) GetReplicaHubs() []*ReplicaHub {
	if x != nil {
		return x.ReplicaHubs
	}
	return nil
}

var File_teams_reco_arch_colossusdb_proto_route_proto protoreflect.FileDescriptor

var file_teams_reco_arch_colossusdb_proto_route_proto_rawDesc = []byte{
	0x0a, 0x2c, 0x74, 0x65, 0x61, 0x6d, 0x73, 0x2f, 0x72, 0x65, 0x63, 0x6f, 0x2d, 0x61, 0x72, 0x63,
	0x68, 0x2f, 0x63, 0x6f, 0x6c, 0x6f, 0x73, 0x73, 0x75, 0x73, 0x64, 0x62, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2f, 0x72, 0x6f, 0x75, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0a,
	0x63, 0x6f, 0x6c, 0x6f, 0x73, 0x73, 0x75, 0x73, 0x64, 0x62, 0x1a, 0x2d, 0x74, 0x65, 0x61, 0x6d,
	0x73, 0x2f, 0x72, 0x65, 0x63, 0x6f, 0x2d, 0x61, 0x72, 0x63, 0x68, 0x2f, 0x63, 0x6f, 0x6c, 0x6f,
	0x73, 0x73, 0x75, 0x73, 0x64, 0x62, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6d,
	0x6d, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x37, 0x74, 0x65, 0x61, 0x6d, 0x73,
	0x2f, 0x72, 0x65, 0x63, 0x6f, 0x2d, 0x61, 0x72, 0x63, 0x68, 0x2f, 0x63, 0x6f, 0x6c, 0x6f, 0x73,
	0x73, 0x75, 0x73, 0x64, 0x62, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x72, 0x65, 0x70, 0x6c,
	0x69, 0x63, 0x61, 0x5f, 0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0x8b, 0x01, 0x0a, 0x11, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e,
	0x4c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x37, 0x0a, 0x08, 0x72, 0x65, 0x70, 0x6c,
	0x69, 0x63, 0x61, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x63, 0x6f, 0x6c,
	0x6f, 0x73, 0x73, 0x75, 0x73, 0x64, 0x62, 0x2e, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x4c,
	0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x08, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61,
	0x73, 0x12, 0x18, 0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x23, 0x0a, 0x0d, 0x73,
	0x70, 0x6c, 0x69, 0x74, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x05, 0x52, 0x0c, 0x73, 0x70, 0x6c, 0x69, 0x74, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x22, 0x91, 0x02, 0x0a, 0x0e, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x4c, 0x6f, 0x63, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x12, 0x12, 0x0a, 0x04, 0x68, 0x6f, 0x73, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x68, 0x6f, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x6f, 0x72, 0x74, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x70, 0x6f, 0x72, 0x74, 0x12, 0x15, 0x0a, 0x06, 0x68,
	0x75, 0x62, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x05, 0x68, 0x75, 0x62,
	0x49, 0x64, 0x12, 0x38, 0x0a, 0x04, 0x69, 0x6e, 0x66, 0x6f, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x24, 0x2e, 0x63, 0x6f, 0x6c, 0x6f, 0x73, 0x73, 0x75, 0x73, 0x64, 0x62, 0x2e, 0x53, 0x65,
	0x72, 0x76, 0x65, 0x72, 0x4c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x49, 0x6e, 0x66,
	0x6f, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x04, 0x69, 0x6e, 0x66, 0x6f, 0x12, 0x0e, 0x0a, 0x02,
	0x69, 0x70, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x70, 0x12, 0x14, 0x0a, 0x05,
	0x61, 0x6c, 0x69, 0x76, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28, 0x08, 0x52, 0x05, 0x61, 0x6c, 0x69,
	0x76, 0x65, 0x12, 0x27, 0x0a, 0x02, 0x6f, 0x70, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x17,
	0x2e, 0x63, 0x6f, 0x6c, 0x6f, 0x73, 0x73, 0x75, 0x73, 0x64, 0x62, 0x2e, 0x41, 0x64, 0x6d, 0x69,
	0x6e, 0x4e, 0x6f, 0x64, 0x65, 0x4f, 0x70, 0x52, 0x02, 0x6f, 0x70, 0x1a, 0x37, 0x0a, 0x09, 0x49,
	0x6e, 0x66, 0x6f, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x3a, 0x02, 0x38, 0x01, 0x22, 0x6a, 0x0a, 0x09, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x49, 0x6e, 0x66,
	0x6f, 0x12, 0x1f, 0x0a, 0x0b, 0x68, 0x61, 0x73, 0x68, 0x5f, 0x6d, 0x65, 0x74, 0x68, 0x6f, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x68, 0x61, 0x73, 0x68, 0x4d, 0x65, 0x74, 0x68,
	0x6f, 0x64, 0x12, 0x23, 0x0a, 0x0d, 0x73, 0x70, 0x6c, 0x69, 0x74, 0x5f, 0x76, 0x65, 0x72, 0x73,
	0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0c, 0x73, 0x70, 0x6c, 0x69, 0x74,
	0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x17, 0x0a, 0x07, 0x75, 0x73, 0x65, 0x5f, 0x70,
	0x61, 0x7a, 0x18, 0x03, 0x20, 0x01, 0x28, 0x08, 0x52, 0x06, 0x75, 0x73, 0x65, 0x50, 0x61, 0x7a,
	0x22, 0x37, 0x0a, 0x0c, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x42, 0x72, 0x69, 0x65, 0x66,
	0x12, 0x27, 0x0a, 0x0f, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f,
	0x75, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0e, 0x70, 0x61, 0x72, 0x74, 0x69,
	0x74, 0x69, 0x6f, 0x6e, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x22, 0xcb, 0x01, 0x0a, 0x09, 0x52, 0x6f,
	0x75, 0x74, 0x65, 0x4d, 0x65, 0x74, 0x61, 0x12, 0x2f, 0x0a, 0x06, 0x66, 0x6f, 0x72, 0x6d, 0x61,
	0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x17, 0x2e, 0x63, 0x6f, 0x6c, 0x6f, 0x73, 0x73,
	0x75, 0x73, 0x64, 0x62, 0x2e, 0x52, 0x6f, 0x75, 0x74, 0x65, 0x46, 0x6f, 0x72, 0x6d, 0x61, 0x74,
	0x52, 0x06, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x12, 0x19, 0x0a, 0x08, 0x72, 0x6f, 0x75, 0x74,
	0x65, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x72, 0x6f, 0x75, 0x74,
	0x65, 0x49, 0x64, 0x12, 0x23, 0x0a, 0x0d, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x63,
	0x6f, 0x75, 0x6e, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0c, 0x73, 0x65, 0x67, 0x6d,
	0x65, 0x6e, 0x74, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x73, 0x65, 0x67, 0x6d,
	0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x05, 0x52, 0x09, 0x73, 0x65,
	0x67, 0x6d, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x2e, 0x0a, 0x05, 0x62, 0x72, 0x69, 0x65, 0x66,
	0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x63, 0x6f, 0x6c, 0x6f, 0x73, 0x73, 0x75,
	0x73, 0x64, 0x62, 0x2e, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x42, 0x72, 0x69, 0x65, 0x66,
	0x52, 0x05, 0x62, 0x72, 0x69, 0x65, 0x66, 0x22, 0xf4, 0x01, 0x0a, 0x0c, 0x52, 0x6f, 0x75, 0x74,
	0x65, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x12, 0x34, 0x0a, 0x0a, 0x74, 0x61, 0x62, 0x6c,
	0x65, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x63,
	0x6f, 0x6c, 0x6f, 0x73, 0x73, 0x75, 0x73, 0x64, 0x62, 0x2e, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x49,
	0x6e, 0x66, 0x6f, 0x52, 0x09, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x3d,
	0x0a, 0x0a, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x02, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x63, 0x6f, 0x6c, 0x6f, 0x73, 0x73, 0x75, 0x73, 0x64, 0x62, 0x2e,
	0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x4c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x52, 0x0a, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x12, 0x34, 0x0a,
	0x07, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1a,
	0x2e, 0x63, 0x6f, 0x6c, 0x6f, 0x73, 0x73, 0x75, 0x73, 0x64, 0x62, 0x2e, 0x53, 0x65, 0x72, 0x76,
	0x65, 0x72, 0x4c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x07, 0x73, 0x65, 0x72, 0x76,
	0x65, 0x72, 0x73, 0x12, 0x39, 0x0a, 0x0c, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x5f, 0x68,
	0x75, 0x62, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x63, 0x6f, 0x6c, 0x6f,
	0x73, 0x73, 0x75, 0x73, 0x64, 0x62, 0x2e, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x48, 0x75,
	0x62, 0x52, 0x0b, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x48, 0x75, 0x62, 0x73, 0x2a, 0x27,
	0x0a, 0x0b, 0x52, 0x6f, 0x75, 0x74, 0x65, 0x46, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x12, 0x08, 0x0a,
	0x04, 0x4a, 0x53, 0x4f, 0x4e, 0x10, 0x00, 0x12, 0x0e, 0x0a, 0x0a, 0x47, 0x5a, 0x49, 0x50, 0x50,
	0x45, 0x44, 0x5f, 0x50, 0x42, 0x10, 0x01, 0x42, 0x46, 0x0a, 0x1c, 0x63, 0x6f, 0x6d, 0x2e, 0x6b,
	0x75, 0x61, 0x69, 0x73, 0x68, 0x6f, 0x75, 0x2e, 0x72, 0x65, 0x63, 0x6f, 0x2e, 0x63, 0x6f, 0x6c,
	0x6f, 0x73, 0x73, 0x75, 0x73, 0x64, 0x62, 0x50, 0x01, 0x5a, 0x24, 0x67, 0x69, 0x74, 0x68, 0x75,
	0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6b, 0x75, 0x61, 0x69, 0x73, 0x68, 0x6f, 0x75, 0x2f, 0x63,
	0x6f, 0x6c, 0x6f, 0x73, 0x73, 0x75, 0x73, 0x64, 0x62, 0x2f, 0x70, 0x62, 0x3b, 0x70, 0x62, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_teams_reco_arch_colossusdb_proto_route_proto_rawDescOnce sync.Once
	file_teams_reco_arch_colossusdb_proto_route_proto_rawDescData = file_teams_reco_arch_colossusdb_proto_route_proto_rawDesc
)

func file_teams_reco_arch_colossusdb_proto_route_proto_rawDescGZIP() []byte {
	file_teams_reco_arch_colossusdb_proto_route_proto_rawDescOnce.Do(func() {
		file_teams_reco_arch_colossusdb_proto_route_proto_rawDescData = protoimpl.X.CompressGZIP(file_teams_reco_arch_colossusdb_proto_route_proto_rawDescData)
	})
	return file_teams_reco_arch_colossusdb_proto_route_proto_rawDescData
}

var file_teams_reco_arch_colossusdb_proto_route_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_teams_reco_arch_colossusdb_proto_route_proto_goTypes = []interface{}{
	(RouteFormat)(0),          // 0: colossusdb.RouteFormat
	(*PartitionLocation)(nil), // 1: colossusdb.PartitionLocation
	(*ServerLocation)(nil),    // 2: colossusdb.ServerLocation
	(*TableInfo)(nil),         // 3: colossusdb.TableInfo
	(*EntriesBrief)(nil),      // 4: colossusdb.EntriesBrief
	(*RouteMeta)(nil),         // 5: colossusdb.RouteMeta
	(*RouteEntries)(nil),      // 6: colossusdb.RouteEntries
	nil,                       // 7: colossusdb.ServerLocation.InfoEntry
	(*ReplicaLocation)(nil),   // 8: colossusdb.ReplicaLocation
	(AdminNodeOp)(0),          // 9: colossusdb.AdminNodeOp
	(*ReplicaHub)(nil),        // 10: colossusdb.ReplicaHub
}
var file_teams_reco_arch_colossusdb_proto_route_proto_depIdxs = []int32{
	8,  // 0: colossusdb.PartitionLocation.replicas:type_name -> colossusdb.ReplicaLocation
	7,  // 1: colossusdb.ServerLocation.info:type_name -> colossusdb.ServerLocation.InfoEntry
	9,  // 2: colossusdb.ServerLocation.op:type_name -> colossusdb.AdminNodeOp
	0,  // 3: colossusdb.RouteMeta.format:type_name -> colossusdb.RouteFormat
	4,  // 4: colossusdb.RouteMeta.brief:type_name -> colossusdb.EntriesBrief
	3,  // 5: colossusdb.RouteEntries.table_info:type_name -> colossusdb.TableInfo
	1,  // 6: colossusdb.RouteEntries.partitions:type_name -> colossusdb.PartitionLocation
	2,  // 7: colossusdb.RouteEntries.servers:type_name -> colossusdb.ServerLocation
	10, // 8: colossusdb.RouteEntries.replica_hubs:type_name -> colossusdb.ReplicaHub
	9,  // [9:9] is the sub-list for method output_type
	9,  // [9:9] is the sub-list for method input_type
	9,  // [9:9] is the sub-list for extension type_name
	9,  // [9:9] is the sub-list for extension extendee
	0,  // [0:9] is the sub-list for field type_name
}

func init() { file_teams_reco_arch_colossusdb_proto_route_proto_init() }
func file_teams_reco_arch_colossusdb_proto_route_proto_init() {
	if File_teams_reco_arch_colossusdb_proto_route_proto != nil {
		return
	}
	file_teams_reco_arch_colossusdb_proto_common_proto_init()
	file_teams_reco_arch_colossusdb_proto_replica_location_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PartitionLocation); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ServerLocation); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TableInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EntriesBrief); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RouteMeta); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RouteEntries); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_teams_reco_arch_colossusdb_proto_route_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_teams_reco_arch_colossusdb_proto_route_proto_goTypes,
		DependencyIndexes: file_teams_reco_arch_colossusdb_proto_route_proto_depIdxs,
		EnumInfos:         file_teams_reco_arch_colossusdb_proto_route_proto_enumTypes,
		MessageInfos:      file_teams_reco_arch_colossusdb_proto_route_proto_msgTypes,
	}.Build()
	File_teams_reco_arch_colossusdb_proto_route_proto = out.File
	file_teams_reco_arch_colossusdb_proto_route_proto_rawDesc = nil
	file_teams_reco_arch_colossusdb_proto_route_proto_goTypes = nil
	file_teams_reco_arch_colossusdb_proto_route_proto_depIdxs = nil
}
