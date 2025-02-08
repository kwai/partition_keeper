#!/bin/bash

source svn_root.env
mkdir -p google/protobuf
cp $svn_root/third_party/protobuf_v3/src/google/protobuf/descriptor.proto google/protobuf/descriptor.proto
patch -i fix_descriptor.patch google/protobuf/descriptor.proto

mkdir -p google/api
pushd google/api

for pbfile in annotations.proto field_behavior.proto http.proto httpbody.proto; do 
   wget https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/$pbfile -O $pbfile
done

popd
