#!/bin/bash

set -u

dir=$1
current_dir=`pwd`
pushd $dir
for fname in `ls`; do
   if [ -L $fname ]; then
      orig_file=`readlink $fname`
      ${current_dir}/csc upload $orig_file colossusdb/partition_keeper_log/$orig_file
   fi
done
popd
