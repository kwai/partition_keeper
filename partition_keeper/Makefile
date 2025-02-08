GIT_COMMIT=$(shell git rev-parse HEAD)

all:	format build

format:
	go mod tidy
	golines --shorten-comments -w .

build:
	mkdir -p bin
	go build -o bin/partition_keeper \
		-ldflags "-X 'github.com/kuaishou/open_partition_keeper/partition_keeper/version.GitCommitId=${GIT_COMMIT}'" \
		github.com/kuaishou/open_partition_keeper/partition_keeper/cmd/partition_keeper
	go build -o bin/partition_keeper_proxy \
		-ldflags "-X 'github.com/kuaishou/open_partition_keeper/partition_keeper/version.GitCommitId=${GIT_COMMIT}'" \
		github.com/kuaishou/open_partition_keeper/partition_keeper/cmd/partition_keeper_proxy
	go build -o bin/zk_backup \
		-ldflags "-X 'github.com/kuaishou/open_partition_keeper/partition_keeper/version.GitCommitId=${GIT_COMMIT}'" \
		github.com/kuaishou/open_partition_keeper/partition_keeper/cmd/zk_backup
	go build -o bin/zk_copy \
		-ldflags "-X 'github.com/kuaishou/open_partition_keeper/partition_keeper/version.GitCommitId=${GIT_COMMIT}'" \
		github.com/kuaishou/open_partition_keeper/partition_keeper/cmd/zk_copy
	go build -o bin/replica_collector \
		-ldflags "-X 'github.com/kuaishou/open_partition_keeper/partition_keeper/version.GitCommitId=${GIT_COMMIT}'" \
		github.com/kuaishou/open_partition_keeper/partition_keeper/cmd/replica_collector
	go build -o bin/tables_mgr \
		-ldflags "-X 'github.com/kuaishou/open_partition_keeper/partition_keeper/version.GitCommitId=${GIT_COMMIT}'" \
		github.com/kuaishou/open_partition_keeper/partition_keeper/cmd/tables_mgr
