#!/bin/bash

set -e

if [ ! -f svn_root.env ]; then
  echo "please create a file svn_root.env in this direcotory, and specify the root of your svn/kbuild project, where the protoc_v3 binary can be found"
  echo "the svn_root.env should contain one line in this format:"
  echo "export svn_root=<your_svn_root_path>"
  exit 1
fi

source svn_root.env
protoc_bin=$svn_root/third_party/prebuilt/bin/protoc_v3

for proto_file in `ls teams/reco-arch/colossusdb/proto/*.proto`; do 
  $protoc_bin --go_out=. --go-grpc_out=. $proto_file
  $protoc_bin -I=. --python_out=../../python/src/pycolossusdb $proto_file
done

partition_service_proto=teams/reco-arch/colossusdb/proto/partition_service.proto
python3 -m grpc_tools.protoc -I "./" --python_out=../../python/src/pycolossusdb --grpc_python_out=../../python/src/pycolossusdb $partition_service_proto


#$protoc_bin -I . --grpc-gateway_out . \
#  teams/reco-arch/colossusdb/proto/universal_discovery.proto
#$protoc_bin -I . --grpc-gateway_out . \
#  teams/reco-arch/colossusdb/proto/tables_manager.proto

mv github.com/kuaishou/colossusdb/pb/*.go ./
rm -rf github.com

for go_file in `ls *.go`; do
  sed -i 's/,omitempty//g' $go_file
done

sed -i 's/json:"split_version"/json:"split_version,omitempty"/g' route.pb.go
sed -i 's/json:"use_paz"/json:"use_paz,omitempty"/g' route.pb.go
sed -i 's/json:"info"/json:"-"/g' replica_location.pb.go

mv *.go ../
