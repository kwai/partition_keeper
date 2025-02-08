#!/bin/bash

source /etc/profile.d/kws_env.sh

cd "$( dirname "${BASH_SOURCE[0]}" )"
log_dir=/home/web_server/kuaishou-runner/logs
./sd_server $OPTS -http_port $AUTO_PORT0 -grpc_port $AUTO_PORT1 -log_dir=${log_dir} > ${log_dir}/stdout.log_`date +"%Y%m%d"` 2>&1
./log_dumper.sh $log_dir
