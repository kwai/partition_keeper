#!/bin/bash

source scripts/env.sh
if [ $# -lt 2 ]; then 
   >&2 echo "./run_testcase.sh <package> <testcase>"
   exit 1
fi

package=$1
shift
testcase=$1
shift

go test github.com/kuaishou/colossusdb/partition_keeper/$package -v -run ^${testcase}\$ -args -logtostderr $@
