## setup go environment
1. install go 1.18

## prepare for build proto

this step isn't necessary as the protobuf generated go code & python code have already placed in the repo.
you may need to prepare this step if you change the protobuf IDL.

```  
# Enable module mode
export GO111MODULE=on
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
cd pb/idl && ./gen_proto.sh
```

## build
1. [setup go environment](#setup go environment)
2. install golines for code format: go install github.com/segmentio/golines@latest
3. make

## test

```
# run test case
./scripts/run_testcase.sh <package> <testcase>
# run test for a package with coverage
./scripts/coverage.sh <package>
# run test for all packages
./scripts/coverage.sh
```

## how to implement a service type

1. add a module in strategy, and implement the base.StrategyBase interface
2. add a create function in your module
3. register it in strategy/service_strategy.go
