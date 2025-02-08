package metastore

import (
	"context"
)

type MetaStore interface {
	Close()

	Get(ctx context.Context, path string) (data []byte, exists bool, succ bool)
	Children(ctx context.Context, path string) (subs []string, exists bool, succ bool)

	Create(ctx context.Context, path string, data []byte) (succ bool)
	Set(ctx context.Context, path string, data []byte) (succ bool)
	Delete(ctx context.Context, path string) (succ bool)

	MultiCreate(ctx context.Context, path []string, data [][]byte) (succ bool)
	MultiDelete(ctx context.Context, path []string) (succ bool)
	MultiSet(ctx context.Context, path []string, data [][]byte) (succ bool)

	WriteBatch(ctx context.Context, ops ...WriteOp) (succ bool)

	RecursiveCreate(ctx context.Context, path string) (succ bool)
	RecursiveDelete(ctx context.Context, path string) (succ bool)
}
