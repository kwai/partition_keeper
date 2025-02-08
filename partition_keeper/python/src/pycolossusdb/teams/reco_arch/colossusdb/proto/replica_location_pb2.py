# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: teams/reco-arch/colossusdb/proto/replica_location.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from teams.reco_arch.colossusdb.proto import common_pb2 as teams_dot_reco__arch_dot_colossusdb_dot_proto_dot_common__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='teams/reco-arch/colossusdb/proto/replica_location.proto',
  package='colossusdb',
  syntax='proto3',
  serialized_pb=_b('\n7teams/reco-arch/colossusdb/proto/replica_location.proto\x12\ncolossusdb\x1a-teams/reco-arch/colossusdb/proto/common.proto\"\xb0\x01\n\x0fReplicaLocation\x12\x14\n\x0cserver_index\x18\x01 \x01(\x05\x12%\n\x04role\x18\x02 \x01(\x0e\x32\x17.colossusdb.ReplicaRole\x12\x33\n\x04info\x18\x03 \x03(\x0b\x32%.colossusdb.ReplicaLocation.InfoEntry\x1a+\n\tInfoEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x42\x46\n\x1c\x63om.kuaishou.reco.colossusdbP\x01Z$github.com/kuaishou/colossusdb/pb;pbb\x06proto3')
  ,
  dependencies=[teams_dot_reco__arch_dot_colossusdb_dot_proto_dot_common__pb2.DESCRIPTOR,])
_sym_db.RegisterFileDescriptor(DESCRIPTOR)




_REPLICALOCATION_INFOENTRY = _descriptor.Descriptor(
  name='InfoEntry',
  full_name='colossusdb.ReplicaLocation.InfoEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='colossusdb.ReplicaLocation.InfoEntry.key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='value', full_name='colossusdb.ReplicaLocation.InfoEntry.value', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=_descriptor._ParseOptions(descriptor_pb2.MessageOptions(), _b('8\001')),
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=252,
  serialized_end=295,
)

_REPLICALOCATION = _descriptor.Descriptor(
  name='ReplicaLocation',
  full_name='colossusdb.ReplicaLocation',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='server_index', full_name='colossusdb.ReplicaLocation.server_index', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='role', full_name='colossusdb.ReplicaLocation.role', index=1,
      number=2, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='info', full_name='colossusdb.ReplicaLocation.info', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_REPLICALOCATION_INFOENTRY, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=119,
  serialized_end=295,
)

_REPLICALOCATION_INFOENTRY.containing_type = _REPLICALOCATION
_REPLICALOCATION.fields_by_name['role'].enum_type = teams_dot_reco__arch_dot_colossusdb_dot_proto_dot_common__pb2._REPLICAROLE
_REPLICALOCATION.fields_by_name['info'].message_type = _REPLICALOCATION_INFOENTRY
DESCRIPTOR.message_types_by_name['ReplicaLocation'] = _REPLICALOCATION

ReplicaLocation = _reflection.GeneratedProtocolMessageType('ReplicaLocation', (_message.Message,), dict(

  InfoEntry = _reflection.GeneratedProtocolMessageType('InfoEntry', (_message.Message,), dict(
    DESCRIPTOR = _REPLICALOCATION_INFOENTRY,
    __module__ = 'teams.reco_arch.colossusdb.proto.replica_location_pb2'
    # @@protoc_insertion_point(class_scope:colossusdb.ReplicaLocation.InfoEntry)
    ))
  ,
  DESCRIPTOR = _REPLICALOCATION,
  __module__ = 'teams.reco_arch.colossusdb.proto.replica_location_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.ReplicaLocation)
  ))
_sym_db.RegisterMessage(ReplicaLocation)
_sym_db.RegisterMessage(ReplicaLocation.InfoEntry)


DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(descriptor_pb2.FileOptions(), _b('\n\034com.kuaishou.reco.colossusdbP\001Z$github.com/kuaishou/colossusdb/pb;pb'))
_REPLICALOCATION_INFOENTRY.has_options = True
_REPLICALOCATION_INFOENTRY._options = _descriptor._ParseOptions(descriptor_pb2.MessageOptions(), _b('8\001'))
# @@protoc_insertion_point(module_scope)
