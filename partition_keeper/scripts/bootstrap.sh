#!/bin/bash

set -e
set -u

ensure_has_zookeeper() {
    if [ ! -d .pkg ]; then
        mkdir .pkg
    fi
    if [ -d ./.pkg/apache-zookeeper-3.9.3-bin ]; then
        return 0
    fi

    pushd .pkg
    rm -rf apache-zookeeper-3.9.3-bin.tar.gz
    wget https://dlcdn.apache.org/zookeeper/zookeeper-3.9.3/apache-zookeeper-3.9.3-bin.tar.gz
    tar xf apache-zookeeper-3.9.3-bin.tar.gz
    popd
}

bootstrap_zookeeper() {
    ensure_has_zookeeper

    pushd .pkg/apache-zookeeper-3.9.3-bin/bin
    if [ -f ../conf/data/zookeeper_server.pid ]; then
        ./zkServer.sh stop
    fi
    popd

    pushd .pkg/apache-zookeeper-3.9.3-bin/conf
    rm -rf data && mkdir data
    sed "s|dataDir=/tmp/zookeeper|dataDir=${PWD}/data|g" zoo_sample.cfg > zoo.cfg
    echo "admin.enableServer=false" >> zoo.cfg
    popd

    pushd .pkg/apache-zookeeper-3.9.3-bin/bin
    ./zkServer.sh start
    popd
}

stop_zookeeper() {
    if [ ! -d .pkg/apache-zookeeper-3.9.3-bin/bin ]; then
        echo "no zookeeper dir"
        return 0
    fi

    pushd .pkg/apache-zookeeper-3.9.3-bin/bin
    ./zkServer.sh stop
    popd
}

stop_partition_keeper() {
    ps aux | grep "partition_keeper -http" | grep -v grep | awk '{print $2}' | xargs -I {} kill -9 {}
    sleep 1
}

start_partition_keeper() {
    mkdir -p log
    ./partition_keeper -http_port 8899 -zk_hosts=127.0.0.1:2181 -log_dir=./log -default_service_type colossusdb_embedding_server $@ > log/stdout 2>&1 &
    echo "start partition keeper, wait its initialize"
    sleep 3
}

upgrade_partition_keeper() {
    stop_partition_keeper
    update_package
    start_partition_keeper
}

bootstrap_all() {
    bootstrap_zookeeper
    stop_partition_keeper
    start_partition_keeper
}

help() {
    echo "boostrap.sh <subcommand>"
    echo "supported subcommands: "
    echo "    bootstrap_all: start a zookeeper & start a partition keeper"
    echo "    upgrade_partition_keeper: update the partition keeper with neweset version & restart the partition process"
    echo "    start_partition_keeper: start the partition_keeper process"
    echo "    stop_partition_keeper: stop the partition_keeper process"
    echo "    bootstrap_zookeeper: clean the old zookeeper data and restart the zookeeper process"
    exit 1
}

if [ $# -lt 1 ]; then
    help
fi

cmd=$1
shift
eval "$cmd $@"
if [ $? -ne 0 ]; then
    help
fi
