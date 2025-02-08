#!/bin/bash

source /etc/profile.d/kws_env.sh

cd "$( dirname "${BASH_SOURCE[0]}" )"
log_dir=/home/web_server/kuaishou-runner/logs
./partition_keeper $OPTS \
    -http_port $AUTO_PORT0 -zk_hosts=${ZK_HOSTS} \
    -log_dir=${log_dir} \
    -allow_service_types ${ALLOW_SERVICE_TYPES} \
    -namespace ${NAMESPACE} \
    -global_manager_service_name ${GLOBAL_MANAGER_SERVICE_NAME} \
    -service_name ${SERVICE_NAME} >> /home/web_server/kuaishou-runner/logs/stdout.log_`date +"%Y%m%d"` 2>&1
./log_dumper.sh $log_dir
