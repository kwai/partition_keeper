#!/bin/bash

set -e

pack="true"
with_pyadmin="true"

print_help() {
    echo "./package.sh [-nopack] [-no_admin]"
    exit 1
}

parse_flags() {
    while [ $# -gt 0 ]; do
        case $1 in
            -nopack)
                pack="false"
                ;;
            -no_admin)
                with_pyadmin="false"
                ;;
            *)
                print_help
                ;;
        esac
        shift
    done
}

gen_pycolossusdb_version() {
    version=`git rev-parse HEAD`

    cat << EOF > git_revision.py
#!/usr/bin/env python3

version="${version}"

def PrintVersion():
    print(version)
EOF
}

do_package() {
    rm -rf output && mkdir -p output
    mkdir -p output/bin
    mkdir -p output/log
    for target in `find bin -type f -executable`; do
        cp -v $target output/bin
    done

    cp -v scripts/supervise output
    cp -v scripts/load.sh output/bin
    cp -v scripts/run output/bin
    cp -v scripts/bootstrap.sh output/bin
    cp -v scripts/run_partition_keeper.sh output/bin
    cp -v scripts/stop_partition_keeper.sh output/bin
    cp -v scripts/run_sdserver.sh output/bin
    cp -v scripts/stop_sdserver.sh output/bin
    cp -v scripts/run_tables_mgr.sh output/bin
    cp -v scripts/stop_tables_mgr.sh output/bin
    cp -v scripts/log_dumper.sh output/bin
    cp -v ../scripts/prepare_mock_onebox_data.sh output/bin

    if [ $with_pyadmin == "true" ]; then
        pushd python/src/pycolossusdb
        gen_pycolossusdb_version
        pyinstaller -p . -F partition_keeper_admin.py
        rm -rf build partition_keeper_admin.spec
        mv dist/partition_keeper_admin ../../../output/bin
        rm -rf dist
        popd
    fi

    if [ $pack == "true" ]; then
        tar -zcf partition_keeper.tar.gz output
        csc upload partition_keeper.tar.gz colossusdb/partition_keeper.tar.gz
        csc upload output/bin/sd colossusdb/sd
        csc upload output/bin/zk_backup colossusdb/zk_backup
    fi
}

parse_flags $@
do_package
