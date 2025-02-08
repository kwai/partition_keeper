#!/bin/bash

source /etc/profile.d/kws_env.sh

cd "$( dirname "${BASH_SOURCE[0]}" )"
log_dir=/home/web_server/kuaishou-runner/logs
./tables_mgr $OPTS \
   -http_port $AUTO_PORT0 -grpc_port $AUTO_PORT1 \
   -log_dir ${log_dir} \
   -service_name ${SERVICE_NAME} \
   -zk_path ${ZK_PATH} \
   -zk_hosts ${ZK_HOSTS} >> ${log_dir}/stdout.log_`date +"%Y%m%d"` 2>&1
