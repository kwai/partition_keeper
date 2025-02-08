# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: teams/reco-arch/colossusdb/proto/universal_discovery.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.api import annotations_pb2 as google_dot_api_dot_annotations__pb2
from teams.reco_arch.colossusdb.proto import common_pb2 as teams_dot_reco__arch_dot_colossusdb_dot_proto_dot_common__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='teams/reco-arch/colossusdb/proto/universal_discovery.proto',
  package='colossusdb',
  syntax='proto3',
  serialized_pb=_b('\n:teams/reco-arch/colossusdb/proto/universal_discovery.proto\x12\ncolossusdb\x1a\x1cgoogle/api/annotations.proto\x1a-teams/reco-arch/colossusdb/proto/common.proto\"3\n\x0fGetNodesRequest\x12\x14\n\x0cservice_name\x18\x01 \x01(\t\x12\n\n\x02\x61z\x18\x02 \x03(\t\"1\n\x07KwsInfo\x12\x0e\n\x06region\x18\x01 \x01(\t\x12\n\n\x02\x61z\x18\x02 \x01(\t\x12\n\n\x02\x64\x63\x18\x03 \x01(\t\"\xd3\x01\n\x0bServiceNode\x12\n\n\x02id\x18\x01 \x01(\t\x12\x10\n\x08protocol\x18\x02 \x01(\t\x12\x0c\n\x04host\x18\x03 \x01(\t\x12\x0c\n\x04port\x18\x04 \x01(\x05\x12\x0f\n\x07payload\x18\x05 \x01(\t\x12\x0e\n\x06weight\x18\x06 \x01(\x01\x12\r\n\x05shard\x18\x07 \x01(\t\x12\x10\n\x08location\x18\x08 \x01(\t\x12\x19\n\x11initial_live_time\x18\t \x01(\x03\x12 \n\x03kws\x18\n \x01(\x0b\x32\x13.colossusdb.KwsInfo\x12\x0b\n\x03paz\x18\x0b \x01(\t\"\xba\x01\n\x10GetNodesResponse\x12\'\n\x06status\x18\x01 \x01(\x0b\x32\x17.colossusdb.ErrorStatus\x12\x36\n\x05nodes\x18\x02 \x03(\x0b\x32\'.colossusdb.GetNodesResponse.NodesEntry\x1a\x45\n\nNodesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12&\n\x05value\x18\x02 \x01(\x0b\x32\x17.colossusdb.ServiceNode:\x02\x38\x01\x32r\n\x12UniversalDiscovery\x12\\\n\x08GetNodes\x12\x1b.colossusdb.GetNodesRequest\x1a\x1c.colossusdb.GetNodesResponse\"\x15\x82\xd3\xe4\x93\x02\x0f\x12\r/v1/get_nodesB&Z$github.com/kuaishou/colossusdb/pb;pbb\x06proto3')
  ,
  dependencies=[google_dot_api_dot_annotations__pb2.DESCRIPTOR,teams_dot_reco__arch_dot_colossusdb_dot_proto_dot_common__pb2.DESCRIPTOR,])
_sym_db.RegisterFileDescriptor(DESCRIPTOR)




_GETNODESREQUEST = _descriptor.Descriptor(
  name='GetNodesRequest',
  full_name='colossusdb.GetNodesRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='service_name', full_name='colossusdb.GetNodesRequest.service_name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='az', full_name='colossusdb.GetNodesRequest.az', index=1,
      number=2, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=151,
  serialized_end=202,
)


_KWSINFO = _descriptor.Descriptor(
  name='KwsInfo',
  full_name='colossusdb.KwsInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='region', full_name='colossusdb.KwsInfo.region', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='az', full_name='colossusdb.KwsInfo.az', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='dc', full_name='colossusdb.KwsInfo.dc', index=2,
      number=3, type=9, cpp_type=9, label=1,
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
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=204,
  serialized_end=253,
)


_SERVICENODE = _descriptor.Descriptor(
  name='ServiceNode',
  full_name='colossusdb.ServiceNode',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='colossusdb.ServiceNode.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='protocol', full_name='colossusdb.ServiceNode.protocol', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='host', full_name='colossusdb.ServiceNode.host', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='port', full_name='colossusdb.ServiceNode.port', index=3,
      number=4, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='payload', full_name='colossusdb.ServiceNode.payload', index=4,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='weight', full_name='colossusdb.ServiceNode.weight', index=5,
      number=6, type=1, cpp_type=5, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='shard', full_name='colossusdb.ServiceNode.shard', index=6,
      number=7, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='location', full_name='colossusdb.ServiceNode.location', index=7,
      number=8, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='initial_live_time', full_name='colossusdb.ServiceNode.initial_live_time', index=8,
      number=9, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='kws', full_name='colossusdb.ServiceNode.kws', index=9,
      number=10, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='paz', full_name='colossusdb.ServiceNode.paz', index=10,
      number=11, type=9, cpp_type=9, label=1,
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
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=256,
  serialized_end=467,
)


_GETNODESRESPONSE_NODESENTRY = _descriptor.Descriptor(
  name='NodesEntry',
  full_name='colossusdb.GetNodesResponse.NodesEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='colossusdb.GetNodesResponse.NodesEntry.key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='value', full_name='colossusdb.GetNodesResponse.NodesEntry.value', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
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
  serialized_start=587,
  serialized_end=656,
)

_GETNODESRESPONSE = _descriptor.Descriptor(
  name='GetNodesResponse',
  full_name='colossusdb.GetNodesResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='status', full_name='colossusdb.GetNodesResponse.status', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='nodes', full_name='colossusdb.GetNodesResponse.nodes', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_GETNODESRESPONSE_NODESENTRY, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=470,
  serialized_end=656,
)

_SERVICENODE.fields_by_name['kws'].message_type = _KWSINFO
_GETNODESRESPONSE_NODESENTRY.fields_by_name['value'].message_type = _SERVICENODE
_GETNODESRESPONSE_NODESENTRY.containing_type = _GETNODESRESPONSE
_GETNODESRESPONSE.fields_by_name['status'].message_type = teams_dot_reco__arch_dot_colossusdb_dot_proto_dot_common__pb2._ERRORSTATUS
_GETNODESRESPONSE.fields_by_name['nodes'].message_type = _GETNODESRESPONSE_NODESENTRY
DESCRIPTOR.message_types_by_name['GetNodesRequest'] = _GETNODESREQUEST
DESCRIPTOR.message_types_by_name['KwsInfo'] = _KWSINFO
DESCRIPTOR.message_types_by_name['ServiceNode'] = _SERVICENODE
DESCRIPTOR.message_types_by_name['GetNodesResponse'] = _GETNODESRESPONSE

GetNodesRequest = _reflection.GeneratedProtocolMessageType('GetNodesRequest', (_message.Message,), dict(
  DESCRIPTOR = _GETNODESREQUEST,
  __module__ = 'teams.reco_arch.colossusdb.proto.universal_discovery_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.GetNodesRequest)
  ))
_sym_db.RegisterMessage(GetNodesRequest)

KwsInfo = _reflection.GeneratedProtocolMessageType('KwsInfo', (_message.Message,), dict(
  DESCRIPTOR = _KWSINFO,
  __module__ = 'teams.reco_arch.colossusdb.proto.universal_discovery_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.KwsInfo)
  ))
_sym_db.RegisterMessage(KwsInfo)

ServiceNode = _reflection.GeneratedProtocolMessageType('ServiceNode', (_message.Message,), dict(
  DESCRIPTOR = _SERVICENODE,
  __module__ = 'teams.reco_arch.colossusdb.proto.universal_discovery_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.ServiceNode)
  ))
_sym_db.RegisterMessage(ServiceNode)

GetNodesResponse = _reflection.GeneratedProtocolMessageType('GetNodesResponse', (_message.Message,), dict(

  NodesEntry = _reflection.GeneratedProtocolMessageType('NodesEntry', (_message.Message,), dict(
    DESCRIPTOR = _GETNODESRESPONSE_NODESENTRY,
    __module__ = 'teams.reco_arch.colossusdb.proto.universal_discovery_pb2'
    # @@protoc_insertion_point(class_scope:colossusdb.GetNodesResponse.NodesEntry)
    ))
  ,
  DESCRIPTOR = _GETNODESRESPONSE,
  __module__ = 'teams.reco_arch.colossusdb.proto.universal_discovery_pb2'
  # @@protoc_insertion_point(class_scope:colossusdb.GetNodesResponse)
  ))
_sym_db.RegisterMessage(GetNodesResponse)
_sym_db.RegisterMessage(GetNodesResponse.NodesEntry)


DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(descriptor_pb2.FileOptions(), _b('Z$github.com/kuaishou/colossusdb/pb;pb'))
_GETNODESRESPONSE_NODESENTRY.has_options = True
_GETNODESRESPONSE_NODESENTRY._options = _descriptor._ParseOptions(descriptor_pb2.MessageOptions(), _b('8\001'))
# @@protoc_insertion_point(module_scope)
