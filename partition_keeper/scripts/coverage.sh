#!/bin/bash

set -e

source scripts/env.sh
logdir=`pwd`/log
coverage_package() {
  rm -rf coverage.out
  rm -rf /var/www/html/coverage.html
  for package in $@; do
    go test github.com/kuaishou/colossusdb/partition_keeper/$package -v -coverprofile=coverage.out -args -v 1 -log_dir $logdir
  done
  go tool cover -html=coverage.out -o coverage.html
}

mkdir -p $logdir
if [ $# -lt 1 ]; then
  coverage_package `find . -name "*.go" | cut -c 3- | awk -F'/' 'NF{NF-=1};1' | sed 's/ /\//g' | sort | uniq`
else
  if [ $1 == "-p" ]; then
    shift
    coverage_package $@
  else
    for dir in "$@"; do
      coverage_package `find $dir -name "*.go" | awk -F'/' 'NF{NF-=1};1' | sed 's/ /\//g' | sort | uniq`
    done
  fi
fi
