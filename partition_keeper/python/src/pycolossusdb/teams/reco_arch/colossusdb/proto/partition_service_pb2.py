# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: teams/reco-arch/colossusdb/proto/partition_service.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from teams.reco_arch.colossusdb.proto import common_pb2 as teams_dot_reco__arch_dot_colossusdb_dot_proto_dot_common__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n8teams/reco-arch/colossusdb/proto/partition_service.proto\x12\ncolossusdb\x1a-teams/reco-arch/colossusdb/proto/common.proto\"\xa2\x01\n\x0ePartitionError\"\x8f\x01\n\x04\x43ode\x12\x07\n\x03kOK\x10\x00\x12\x12\n\x0ekReplicaExists\x10\x01\x12\x15\n\x11kReplicaNotExists\x10\x02\x12\x0c\n\x08kUnknown\x10\x03\x12\x16\n\x12kResourceExhausted\x10\x04\x12\x17\n\x13kFailedPrecondition\x10\x05\x12\x14\n\x10kInvalidArgument\x10\x06\"*\n\x07RpcNode\x12\x11\n\tnode_name\x18\x01 \x01(\t\x12\x0c\n\x04port\x18\x02 \x01(\x05\"\xda\x02\n\x10PartitionReplica\x12%\n\x04role\x18\x01 \x01(\x0e\x32\x17.colossusdb.ReplicaRole\x12!\n\x04node\x18\x02 \x01(\x0b\x32\x13.colossusdb.RpcNode\x12I\n\x0fstatistics_info\x18\x03 \x03(\x0b\x32\x30.colossusdb.PartitionReplica.StatisticsInfoEntry\x12\x18\n\x10ready_to_promote\x18\x04 \x01(\x08\x12\x10\n\x08hub_name\x18\x05 \x01(\t\x12\x16\n\x0enode_unique_id\x18\x06 \x01(\t\x12\x17\n\x0frestore_version\x18\x07 \x01(\x03\x12\x1d\n\x15split_cleanup_version\x18\x08 \x01(\x05\x1a\x35\n\x13StatisticsInfoEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"s\n\x11PartitionPeerInfo\x12\x1a\n\x12membership_version\x18\x01 \x01(\x03\x12+\n\x05peers\x18\x02 \x03(\x0b\x32\x1c.colossusdb.PartitionReplica\x12\x15\n\rsplit_version\x18\x03 \x01(\x05\"F\n\x10\x44ynamicTableInfo\x12\x15\n\rpartition_num\x18\x01 \x01(\x05\x12\x1b\n\x13table_split_version\x18\x02 \x01(\x05\"\xc8\x01\n\rPartitionInfo\x12\x14\n\x0cpartition_id\x18\x01 \x01(\x05\x12\x10\n\x08table_id\x18\x02 \x01(\x05\x12\x12\n\ntable_name\x18\x03 \x01(\t\x12\x14\n\x0cservice_type\x18\x05 \x01(\t\x12\x15\n\rpartition_num\x18\x06 \x01(\x05\x12\x17\n\x0ftable_json_args\x18\x04 \x01(\t\x12\x18\n\x10table_kconf_path\x18\x07 \x01(\t\x12\x1b\n\x13table_split_version\x18\x08 \x01(\x05\"\xe5\x01\n\x11\x41\x64\x64ReplicaRequest\x12\'\n\x04part\x18\x01 \x01(\x0b\x32\x19.colossusdb.PartitionInfo\x12\x30\n\tpeer_info\x18\x02 \x01(\x0b\x32\x1d.colossusdb.PartitionPeerInfo\x12\x1a\n\x12\x65stimated_replicas\x18\x03 \x01(\x05\x12\x13\n\x0b\x66or_restore\x18\x04 \x01(\x08\x12\x10\n\x08\x61uth_key\x18\x05 \x01(\t\x12\x32\n\x0bparent_info\x18\x06 \x01(\x0b\x32\x1d.colossusdb.PartitionPeerInfo\"\xec\x01\n\x18ReconfigPartitionRequest\x12\x14\n\x0cpartition_id\x18\x01 \x01(\x05\x12\x10\n\x08table_id\x18\x02 \x01(\x05\x12\x30\n\tpeer_info\x18\x03 \x01(\x0b\x32\x1d.colossusdb.PartitionPeerInfo\x12\x10\n\x08\x61uth_key\x18\x04 \x01(\t\x12\x32\n\x0bparent_info\x18\x05 \x01(\x0b\x32\x1d.colossusdb.PartitionPeerInfo\x12\x30\n\ntable_info\x18\x06 \x01(\x0b\x32\x1c.colossusdb.DynamicTableInfo\"\x9e\x01\n\x14RemoveReplicaRequest\x12\x14\n\x0cpartition_id\x18\x01 \x01(\x05\x12\x10\n\x08table_id\x18\x02 \x01(\x05\x12\x30\n\tpeer_info\x18\x03 \x01(\x0b\x32\x1d.colossusdb.PartitionPeerInfo\x12\x1a\n\x12\x65stimated_replicas\x18\x04 \x01(\x05\x12\x10\n\x08\x61uth_key\x18\x05 \x01(\t\"m\n\x11ReplicaReportInfo\x12\x14\n\x0cpartition_id\x18\x01 \x01(\x05\x12\x10\n\x08table_id\x18\x02 \x01(\x05\x12\x30\n\tpeer_info\x18\x03 \x01(\x0b\x32\x1d.colossusdb.PartitionPeerInfo\"\x80\x02\n\x12GetReplicasRequest\x12,\n\x05infos\x18\x01 \x03(\x0b\x32\x1d.colossusdb.ReplicaReportInfo\x12\x1d\n\x15\x66rom_partition_keeper\x18\x02 \x01(\x08\x12Q\n\x12\x65stimated_replicas\x18\x03 \x03(\x0b\x32\x35.colossusdb.GetReplicasRequest.EstimatedReplicasEntry\x12\x10\n\x08\x61uth_key\x18\x04 \x01(\t\x1a\x38\n\x16\x45stimatedReplicasEntry\x12\x0b\n\x03key\x18\x01 \x01(\x05\x12\r\n\x05value\x18\x02 \x01(\x05:\x02\x38\x01\"\xe9\x01\n\nServerInfo\x12\x14\n\x0cservice_name\x18\x01 \x01(\t\x12\x0f\n\x07node_id\x18\x02 \x01(\t\x12!\n\x04node\x18\x03 \x01(\x0b\x32\x13.colossusdb.RpcNode\x12\x15\n\rbelong_to_hub\x18\x04 \x01(\t\x12\x43\n\x0fstatistics_info\x18\x05 \x03(\x0b\x32*.colossusdb.ServerInfo.StatisticsInfoEntry\x1a\x35\n\x13StatisticsInfoEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\xca\x01\n\x13GetReplicasResponse\x12.\n\rserver_result\x18\x04 \x01(\x0b\x32\x17.colossusdb.ErrorStatus\x12+\n\x0bserver_info\x18\x01 \x01(\x0b\x32\x16.colossusdb.ServerInfo\x12(\n\x07results\x18\x02 \x03(\x0b\x32\x17.colossusdb.ErrorStatus\x12,\n\x05infos\x18\x03 \x03(\x0b\x32\x1d.colossusdb.ReplicaReportInfo\"\x8a\x02\n\x14\x43ustomCommandRequest\x12\x14\n\x0cpartition_id\x18\x01 \x01(\x05\x12\x10\n\x08table_id\x18\x02 \x01(\x05\x12\x1a\n\x12\x63ommand_session_id\x18\x03 \x01(\x03\x12\x1f\n\x17\x63ommand_issue_timestamp\x18\x04 \x01(\x03\x12\x14\n\x0c\x63ommand_name\x18\x05 \x01(\t\x12\x38\n\x04\x61rgs\x18\x06 \x03(\x0b\x32*.colossusdb.CustomCommandRequest.ArgsEntry\x12\x10\n\x08\x61uth_key\x18\x07 \x01(\t\x1a+\n\tArgsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"R\n\x15\x43ustomCommandResponse\x12\'\n\x06status\x18\x01 \x01(\x0b\x32\x17.colossusdb.ErrorStatus\x12\x10\n\x08progress\x18\x02 \x01(\x05\"/\n\x1b\x43hangeAuthenticationRequest\x12\x10\n\x08\x61uth_key\x18\x01 \x01(\t\"\xd5\x01\n\x13ReplicaSplitRequest\x12\x10\n\x08table_id\x18\x01 \x01(\x05\x12\x14\n\x0cpartition_id\x18\x02 \x01(\x05\x12\x34\n\x0enew_table_info\x18\x03 \x01(\x0b\x32\x1c.colossusdb.DynamicTableInfo\x12,\n\x05peers\x18\x04 \x01(\x0b\x32\x1d.colossusdb.PartitionPeerInfo\x12\x32\n\x0b\x63hild_peers\x18\x05 \x01(\x0b\x32\x1d.colossusdb.PartitionPeerInfo\"e\n\x1aReplicaSplitCleanupRequest\x12\x10\n\x08table_id\x18\x01 \x01(\x05\x12\x14\n\x0cpartition_id\x18\x02 \x01(\x05\x12\x1f\n\x17partition_split_version\x18\x03 \x01(\x05\"E\n\x1bPrepareSwitchPrimaryRequest\x12\x10\n\x08table_id\x18\x01 \x01(\x05\x12\x14\n\x0cpartition_id\x18\x02 \x01(\x05\"\x16\n\x14ReplicateInfoRequest\"/\n\x15ReplicateInfoResponse\x12\x16\n\x0ereplicate_port\x18\x01 \x01(\x05\x32\xcd\x06\n\x10PartitionService\x12\x44\n\nAddReplica\x12\x1d.colossusdb.AddReplicaRequest\x1a\x17.colossusdb.ErrorStatus\x12L\n\x0bReconfigure\x12$.colossusdb.ReconfigPartitionRequest\x1a\x17.colossusdb.ErrorStatus\x12J\n\rRemoveReplica\x12 .colossusdb.RemoveReplicaRequest\x1a\x17.colossusdb.ErrorStatus\x12N\n\x0bGetReplicas\x12\x1e.colossusdb.GetReplicasRequest\x1a\x1f.colossusdb.GetReplicasResponse\x12Z\n\x13HandleCustomCommand\x12 .colossusdb.CustomCommandRequest\x1a!.colossusdb.CustomCommandResponse\x12X\n\x14\x43hangeAuthentication\x12\'.colossusdb.ChangeAuthenticationRequest\x1a\x17.colossusdb.ErrorStatus\x12H\n\x0cReplicaSplit\x12\x1f.colossusdb.ReplicaSplitRequest\x1a\x17.colossusdb.ErrorStatus\x12V\n\x13ReplicaSplitCleanup\x12&.colossusdb.ReplicaSplitCleanupRequest\x1a\x17.colossusdb.ErrorStatus\x12X\n\x14PrepareSwitchPrimary\x12\'.colossusdb.PrepareSwitchPrimaryRequest\x1a\x17.colossusdb.ErrorStatus\x12W\n\x10GetReplicateInfo\x12 .colossusdb.ReplicateInfoRequest\x1a!.colossusdb.ReplicateInfoResponseB&Z$github.com/kuaishou/colossusdb/pb;pbb\x06proto3')



_PARTITIONERROR = DESCRIPTOR.message_types_by_name['PartitionError']
_RPCNODE = DESCRIPTOR.message_types_by_name['RpcNode']
_PARTITIONREPLICA = DESCRIPTOR.message_types_by_name['PartitionReplica']
_PARTITIONREPLICA_STATISTICSINFOENTRY = _PARTITIONREPLICA.nested_types_by_name['StatisticsInfoEntry']
_PARTITIONPEERINFO = DESCRIPTOR.message_types_by_name['PartitionPeerInfo']
_DYNAMICTABLEINFO = DESCRIPTOR.message_types_by_name['DynamicTableInfo']
_PARTITIONINFO = DESCRIPTOR.message_types_by_name['PartitionInfo']
_ADDREPLICAREQUEST = DESCRIPTOR.message_types_by_name['AddReplicaRequest']
_RECONFIGPARTITIONREQUEST = DESCRIPTOR.message_types_by_name['ReconfigPartitionRequest']
_REMOVEREPLICAREQUEST = DESCRIPTOR.message_types_by_name['RemoveReplicaRequest']
_REPLICAREPORTINFO = DESCRIPTOR.message_types_by_name['ReplicaReportInfo']
_GETREPLICASREQUEST = DESCRIPTOR.message_types_by_name['GetReplicasRequest']
_GETREPLICASREQUEST_ESTIMATEDREPLICASENTRY = _GETREPLICASREQUEST.nested_types_by_name['EstimatedReplicasEntry']
_SERVERINFO = DESCRIPTOR.message_types_by_name['ServerInfo']
_SERVERINFO_STATISTICSINFOENTRY = _SERVERINFO.nested_types_by_name['StatisticsInfoEntry']
_GETREPLICASRESPONSE = DESCRIPTOR.message_types_by_name['GetReplicasResponse']
_CUSTOMCOMMANDREQUEST = DESCRIPTOR.message_types_by_name['CustomCommandRequest']
_CUSTOMCOMMANDREQUEST_ARGSENTRY = _CUSTOMCOMMANDREQUEST.nested_types_by_name['ArgsEntry']
_CUSTOMCOMMANDRESPONSE = DESCRIPTOR.message_types_by_name['CustomCommandResponse']
_CHANGEAUTHENTICATIONREQUEST = DESCRIPTOR.message_types_by_name['ChangeAuthenticationRequest']
_REPLICASPLITREQUEST = DESCRIPTOR.message_types_by_name['ReplicaSplitRequest']
_REPLICASPLITCLEANUPREQUEST = DESCRIPTOR.message_types_by_name['ReplicaSplitCleanupRequest']
_PREPARESWITCHPRIMARYREQUEST = DESCRIPTOR.message_types_by_name['PrepareSwitchPrimaryRequest']
_REPLICATEINFOREQUEST = DESCRIPTOR.message_types_by_name['ReplicateInfoRequest']
_REPLICATEINFORESPONSE = DESCRIPTOR.message_types_by_name['ReplicateInfoResponse']
_PARTITIONERROR_CODE = _PARTITIONERROR.enum_types_by_name['Code']
PartitionError = _reflection.GeneratedProtocolMessageType('PartitionError', (_message.Message,), {
  'DESCRIPTOR' : _PARTITIONERROR,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.PartitionError)
  })
_sym_db.RegisterMessage(PartitionError)

RpcNode = _reflection.GeneratedProtocolMessageType('RpcNode', (_message.Message,), {
  'DESCRIPTOR' : _RPCNODE,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.RpcNode)
  })
_sym_db.RegisterMessage(RpcNode)

PartitionReplica = _reflection.GeneratedProtocolMessageType('PartitionReplica', (_message.Message,), {

  'StatisticsInfoEntry' : _reflection.GeneratedProtocolMessageType('StatisticsInfoEntry', (_message.Message,), {
    'DESCRIPTOR' : _PARTITIONREPLICA_STATISTICSINFOENTRY,
    '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
    # @@protoc_insertion_point(class_scope:colossusdb.PartitionReplica.StatisticsInfoEntry)
    })
  ,
  'DESCRIPTOR' : _PARTITIONREPLICA,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.PartitionReplica)
  })
_sym_db.RegisterMessage(PartitionReplica)
_sym_db.RegisterMessage(PartitionReplica.StatisticsInfoEntry)

PartitionPeerInfo = _reflection.GeneratedProtocolMessageType('PartitionPeerInfo', (_message.Message,), {
  'DESCRIPTOR' : _PARTITIONPEERINFO,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.PartitionPeerInfo)
  })
_sym_db.RegisterMessage(PartitionPeerInfo)

DynamicTableInfo = _reflection.GeneratedProtocolMessageType('DynamicTableInfo', (_message.Message,), {
  'DESCRIPTOR' : _DYNAMICTABLEINFO,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.DynamicTableInfo)
  })
_sym_db.RegisterMessage(DynamicTableInfo)

PartitionInfo = _reflection.GeneratedProtocolMessageType('PartitionInfo', (_message.Message,), {
  'DESCRIPTOR' : _PARTITIONINFO,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.PartitionInfo)
  })
_sym_db.RegisterMessage(PartitionInfo)

AddReplicaRequest = _reflection.GeneratedProtocolMessageType('AddReplicaRequest', (_message.Message,), {
  'DESCRIPTOR' : _ADDREPLICAREQUEST,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.AddReplicaRequest)
  })
_sym_db.RegisterMessage(AddReplicaRequest)

ReconfigPartitionRequest = _reflection.GeneratedProtocolMessageType('ReconfigPartitionRequest', (_message.Message,), {
  'DESCRIPTOR' : _RECONFIGPARTITIONREQUEST,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.ReconfigPartitionRequest)
  })
_sym_db.RegisterMessage(ReconfigPartitionRequest)

RemoveReplicaRequest = _reflection.GeneratedProtocolMessageType('RemoveReplicaRequest', (_message.Message,), {
  'DESCRIPTOR' : _REMOVEREPLICAREQUEST,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.RemoveReplicaRequest)
  })
_sym_db.RegisterMessage(RemoveReplicaRequest)

ReplicaReportInfo = _reflection.GeneratedProtocolMessageType('ReplicaReportInfo', (_message.Message,), {
  'DESCRIPTOR' : _REPLICAREPORTINFO,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.ReplicaReportInfo)
  })
_sym_db.RegisterMessage(ReplicaReportInfo)

GetReplicasRequest = _reflection.GeneratedProtocolMessageType('GetReplicasRequest', (_message.Message,), {

  'EstimatedReplicasEntry' : _reflection.GeneratedProtocolMessageType('EstimatedReplicasEntry', (_message.Message,), {
    'DESCRIPTOR' : _GETREPLICASREQUEST_ESTIMATEDREPLICASENTRY,
    '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
    # @@protoc_insertion_point(class_scope:colossusdb.GetReplicasRequest.EstimatedReplicasEntry)
    })
  ,
  'DESCRIPTOR' : _GETREPLICASREQUEST,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.GetReplicasRequest)
  })
_sym_db.RegisterMessage(GetReplicasRequest)
_sym_db.RegisterMessage(GetReplicasRequest.EstimatedReplicasEntry)

ServerInfo = _reflection.GeneratedProtocolMessageType('ServerInfo', (_message.Message,), {

  'StatisticsInfoEntry' : _reflection.GeneratedProtocolMessageType('StatisticsInfoEntry', (_message.Message,), {
    'DESCRIPTOR' : _SERVERINFO_STATISTICSINFOENTRY,
    '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
    # @@protoc_insertion_point(class_scope:colossusdb.ServerInfo.StatisticsInfoEntry)
    })
  ,
  'DESCRIPTOR' : _SERVERINFO,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.ServerInfo)
  })
_sym_db.RegisterMessage(ServerInfo)
_sym_db.RegisterMessage(ServerInfo.StatisticsInfoEntry)

GetReplicasResponse = _reflection.GeneratedProtocolMessageType('GetReplicasResponse', (_message.Message,), {
  'DESCRIPTOR' : _GETREPLICASRESPONSE,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.GetReplicasResponse)
  })
_sym_db.RegisterMessage(GetReplicasResponse)

CustomCommandRequest = _reflection.GeneratedProtocolMessageType('CustomCommandRequest', (_message.Message,), {

  'ArgsEntry' : _reflection.GeneratedProtocolMessageType('ArgsEntry', (_message.Message,), {
    'DESCRIPTOR' : _CUSTOMCOMMANDREQUEST_ARGSENTRY,
    '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
    # @@protoc_insertion_point(class_scope:colossusdb.CustomCommandRequest.ArgsEntry)
    })
  ,
  'DESCRIPTOR' : _CUSTOMCOMMANDREQUEST,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.CustomCommandRequest)
  })
_sym_db.RegisterMessage(CustomCommandRequest)
_sym_db.RegisterMessage(CustomCommandRequest.ArgsEntry)

CustomCommandResponse = _reflection.GeneratedProtocolMessageType('CustomCommandResponse', (_message.Message,), {
  'DESCRIPTOR' : _CUSTOMCOMMANDRESPONSE,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.CustomCommandResponse)
  })
_sym_db.RegisterMessage(CustomCommandResponse)

ChangeAuthenticationRequest = _reflection.GeneratedProtocolMessageType('ChangeAuthenticationRequest', (_message.Message,), {
  'DESCRIPTOR' : _CHANGEAUTHENTICATIONREQUEST,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.ChangeAuthenticationRequest)
  })
_sym_db.RegisterMessage(ChangeAuthenticationRequest)

ReplicaSplitRequest = _reflection.GeneratedProtocolMessageType('ReplicaSplitRequest', (_message.Message,), {
  'DESCRIPTOR' : _REPLICASPLITREQUEST,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.ReplicaSplitRequest)
  })
_sym_db.RegisterMessage(ReplicaSplitRequest)

ReplicaSplitCleanupRequest = _reflection.GeneratedProtocolMessageType('ReplicaSplitCleanupRequest', (_message.Message,), {
  'DESCRIPTOR' : _REPLICASPLITCLEANUPREQUEST,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.ReplicaSplitCleanupRequest)
  })
_sym_db.RegisterMessage(ReplicaSplitCleanupRequest)

PrepareSwitchPrimaryRequest = _reflection.GeneratedProtocolMessageType('PrepareSwitchPrimaryRequest', (_message.Message,), {
  'DESCRIPTOR' : _PREPARESWITCHPRIMARYREQUEST,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.PrepareSwitchPrimaryRequest)
  })
_sym_db.RegisterMessage(PrepareSwitchPrimaryRequest)

ReplicateInfoRequest = _reflection.GeneratedProtocolMessageType('ReplicateInfoRequest', (_message.Message,), {
  'DESCRIPTOR' : _REPLICATEINFOREQUEST,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.ReplicateInfoRequest)
  })
_sym_db.RegisterMessage(ReplicateInfoRequest)

ReplicateInfoResponse = _reflection.GeneratedProtocolMessageType('ReplicateInfoResponse', (_message.Message,), {
  'DESCRIPTOR' : _REPLICATEINFORESPONSE,
  '__module__' : 'teams.reco_arch.colossusdb.proto.partition_service_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.ReplicateInfoResponse)
  })
_sym_db.RegisterMessage(ReplicateInfoResponse)

_PARTITIONSERVICE = DESCRIPTOR.services_by_name['PartitionService']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'Z$github.com/kuaishou/colossusdb/pb;pb'
  _PARTITIONREPLICA_STATISTICSINFOENTRY._options = None
  _PARTITIONREPLICA_STATISTICSINFOENTRY._serialized_options = b'8\001'
  _GETREPLICASREQUEST_ESTIMATEDREPLICASENTRY._options = None
  _GETREPLICASREQUEST_ESTIMATEDREPLICASENTRY._serialized_options = b'8\001'
  _SERVERINFO_STATISTICSINFOENTRY._options = None
  _SERVERINFO_STATISTICSINFOENTRY._serialized_options = b'8\001'
  _CUSTOMCOMMANDREQUEST_ARGSENTRY._options = None
  _CUSTOMCOMMANDREQUEST_ARGSENTRY._serialized_options = b'8\001'
  _PARTITIONERROR._serialized_start=120
  _PARTITIONERROR._serialized_end=282
  _PARTITIONERROR_CODE._serialized_start=139
  _PARTITIONERROR_CODE._serialized_end=282
  _RPCNODE._serialized_start=284
  _RPCNODE._serialized_end=326
  _PARTITIONREPLICA._serialized_start=329
  _PARTITIONREPLICA._serialized_end=675
  _PARTITIONREPLICA_STATISTICSINFOENTRY._serialized_start=622
  _PARTITIONREPLICA_STATISTICSINFOENTRY._serialized_end=675
  _PARTITIONPEERINFO._serialized_start=677
  _PARTITIONPEERINFO._serialized_end=792
  _DYNAMICTABLEINFO._serialized_start=794
  _DYNAMICTABLEINFO._serialized_end=864
  _PARTITIONINFO._serialized_start=867
  _PARTITIONINFO._serialized_end=1067
  _ADDREPLICAREQUEST._serialized_start=1070
  _ADDREPLICAREQUEST._serialized_end=1299
  _RECONFIGPARTITIONREQUEST._serialized_start=1302
  _RECONFIGPARTITIONREQUEST._serialized_end=1538
  _REMOVEREPLICAREQUEST._serialized_start=1541
  _REMOVEREPLICAREQUEST._serialized_end=1699
  _REPLICAREPORTINFO._serialized_start=1701
  _REPLICAREPORTINFO._serialized_end=1810
  _GETREPLICASREQUEST._serialized_start=1813
  _GETREPLICASREQUEST._serialized_end=2069
  _GETREPLICASREQUEST_ESTIMATEDREPLICASENTRY._serialized_start=2013
  _GETREPLICASREQUEST_ESTIMATEDREPLICASENTRY._serialized_end=2069
  _SERVERINFO._serialized_start=2072
  _SERVERINFO._serialized_end=2305
  _SERVERINFO_STATISTICSINFOENTRY._serialized_start=622
  _SERVERINFO_STATISTICSINFOENTRY._serialized_end=675
  _GETREPLICASRESPONSE._serialized_start=2308
  _GETREPLICASRESPONSE._serialized_end=2510
  _CUSTOMCOMMANDREQUEST._serialized_start=2513
  _CUSTOMCOMMANDREQUEST._serialized_end=2779
  _CUSTOMCOMMANDREQUEST_ARGSENTRY._serialized_start=2736
  _CUSTOMCOMMANDREQUEST_ARGSENTRY._serialized_end=2779
  _CUSTOMCOMMANDRESPONSE._serialized_start=2781
  _CUSTOMCOMMANDRESPONSE._serialized_end=2863
  _CHANGEAUTHENTICATIONREQUEST._serialized_start=2865
  _CHANGEAUTHENTICATIONREQUEST._serialized_end=2912
  _REPLICASPLITREQUEST._serialized_start=2915
  _REPLICASPLITREQUEST._serialized_end=3128
  _REPLICASPLITCLEANUPREQUEST._serialized_start=3130
  _REPLICASPLITCLEANUPREQUEST._serialized_end=3231
  _PREPARESWITCHPRIMARYREQUEST._serialized_start=3233
  _PREPARESWITCHPRIMARYREQUEST._serialized_end=3302
  _REPLICATEINFOREQUEST._serialized_start=3304
  _REPLICATEINFOREQUEST._serialized_end=3326
  _REPLICATEINFORESPONSE._serialized_start=3328
  _REPLICATEINFORESPONSE._serialized_end=3375
  _PARTITIONSERVICE._serialized_start=3378
  _PARTITIONSERVICE._serialized_end=4223
# @@protoc_insertion_point(module_scope)
