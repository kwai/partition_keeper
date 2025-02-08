#!/bin/bash
set -e

pushd "$( dirname "${BASH_SOURCE[0]}" )"
make build
./scripts/package.sh -nopack -no_admin -with_csc
popd
