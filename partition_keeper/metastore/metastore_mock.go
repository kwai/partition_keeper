package metastore

import (
	"strings"
	"sync"

	"context"

	"github.com/kuaishou/open_partition_keeper/partition_keeper/logging"
)

type BlockPoint struct {
	blocked map[string]bool
	mu      sync.Mutex
	cond    *sync.Cond
}

func NewBlockPoint() *BlockPoint {
	output := &BlockPoint{
		blocked: map[string]bool{},
		mu:      sync.Mutex{},
		cond:    nil,
	}
	output.cond = sync.NewCond(&output.mu)
	return output
}

func (bp *BlockPoint) Check(key string) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for {
		if !bp.blocked[key] {
			return
		}
		bp.cond.Wait()
	}
}

func (bp *BlockPoint) Block(key string) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.blocked[key] = true
}

func (bp *BlockPoint) UnBlock(key string) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	delete(bp.blocked, key)
	bp.cond.Broadcast()
}

type treeNode struct {
	data     []byte
	children map[string]*treeNode
}

type WriteRequestRecord struct {
	Method string
	Ops    []WriteOp
}

type MetaStoreMock struct {
	bp             *BlockPoint
	mu             sync.Mutex
	root           treeNode
	writtenBatches []*WriteRequestRecord
}

func NewMockMetaStore() MetaStore {
	return &MetaStoreMock{
		bp: NewBlockPoint(),
		root: treeNode{
			children: make(map[string]*treeNode),
		},
	}
}

func splitPath(path string) []string {
	tmp := strings.Split(path, "/")
	tail := 0
	for _, token := range tmp {
		if len(token) > 0 {
			tmp[tail] = token
			tail++
		}
	}
	return tmp[0:tail]
}

func (m *MetaStoreMock) findParent(root *treeNode, path []string) *treeNode {
	output := m.find(root, path[0:len(path)-1])
	if _, ok := output.children[path[len(path)-1]]; ok {
		return output
	}
	return nil
}

func (m *MetaStoreMock) find(root *treeNode, path []string) *treeNode {
	if len(path) <= 0 {
		return root
	}
	if node, ok := root.children[path[0]]; ok {
		return m.find(node, path[1:])
	} else {
		return nil
	}
}

func (m *MetaStoreMock) BlockApi(key string) {
	m.bp.Block(key)
}

func (m *MetaStoreMock) UnBlockApi(key string) {
	m.bp.UnBlock(key)
}

func (m *MetaStoreMock) Close() {
	m.bp.Check("Close")
}

func (m *MetaStoreMock) Get(
	ctx context.Context,
	path string,
) (data []byte, exists bool, succ bool) {
	m.bp.Check("Get")

	m.mu.Lock()
	defer m.mu.Unlock()

	tokens := splitPath(path)
	if len(tokens) <= 0 {
		return nil, false, false
	}
	node := m.find(&m.root, tokens)
	if node == nil {
		return nil, false, true
	}
	return node.data, true, true
}

func (m *MetaStoreMock) Children(
	ctx context.Context,
	path string,
) (subs []string, exists bool, succ bool) {
	m.bp.Check("Children")

	m.mu.Lock()
	defer m.mu.Unlock()

	tokens := splitPath(path)
	if len(tokens) <= 0 {
		return nil, false, false
	}
	node := m.find(&m.root, tokens)
	if node == nil {
		return nil, false, true
	}
	for key := range node.children {
		subs = append(subs, key)
	}
	return subs, true, true
}

func (m *MetaStoreMock) Create(ctx context.Context, path string, data []byte) (succ bool) {
	m.bp.Check("Create")

	m.mu.Lock()
	defer m.mu.Unlock()

	tokens := splitPath(path)
	if len(tokens) <= 0 {
		return false
	}
	last := len(tokens) - 1
	parent := m.find(&m.root, tokens[0:last])
	if parent == nil {
		return false
	}
	node := parent.children[tokens[last]]
	if node != nil {
		return true
	}
	parent.children[tokens[last]] = &treeNode{
		data:     data,
		children: make(map[string]*treeNode),
	}
	return true
}

func (m *MetaStoreMock) Set(ctx context.Context, path string, data []byte) (succ bool) {
	m.bp.Check("Set")

	m.mu.Lock()
	defer m.mu.Unlock()

	tokens := splitPath(path)
	if len(tokens) <= 0 {
		return false
	}
	node := m.find(&m.root, tokens)
	if node == nil {
		return false
	}
	node.data = data
	return true
}

func (m *MetaStoreMock) deleteInternal(path string) bool {
	tokens := splitPath(path)
	if len(tokens) <= 0 {
		return false
	}
	last := len(tokens) - 1
	parent := m.find(&m.root, tokens[0:last])
	if parent == nil {
		return true
	}
	node := parent.children[tokens[last]]
	if node == nil {
		return true
	}
	if len(node.children) > 0 {
		return false
	}
	delete(parent.children, tokens[last])
	return true
}

func (m *MetaStoreMock) Delete(ctx context.Context, path string) bool {
	m.bp.Check("Delete")

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.deleteInternal(path)
}

func (m *MetaStoreMock) MultiCreate(ctx context.Context, path []string, data [][]byte) (succ bool) {
	m.bp.Check("MultiCreate")

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(path) <= 0 || len(path) != len(data) {
		return false
	}

	i := 0
	for i = 0; i < len(path); i++ {
		tokens := splitPath(path[i])
		if len(tokens) <= 0 {
			break
		}
		last := len(tokens) - 1
		parent := m.find(&m.root, tokens[0:last])
		if parent == nil {
			break
		}
		if parent.children[tokens[last]] != nil {
			break
		}
		parent.children[tokens[last]] = &treeNode{
			data:     data[i],
			children: make(map[string]*treeNode),
		}
	}
	if i >= len(path) {
		return true
	}

	for j := int(i - 1); j >= 0; j-- {
		logging.Assert(m.deleteInternal(path[j]), "")
	}
	return false
}

func (m *MetaStoreMock) MultiDelete(ctx context.Context, path []string) (succ bool) {
	m.bp.Check("MultiDelete")

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(path) <= 0 {
		return false
	}

	parents := []*treeNode{}
	for _, p := range path {
		tokens := splitPath(p)
		if len(tokens) <= 0 {
			return false
		}
		if n := m.findParent(&m.root, tokens); n == nil {
			return false
		} else {
			parents = append(parents, n)
		}
	}
	for i, p := range path {
		tokens := splitPath(p)
		delete(parents[i].children, tokens[len(tokens)-1])
	}
	return true
}

func (m *MetaStoreMock) MultiSet(ctx context.Context, path []string, data [][]byte) (succ bool) {
	m.bp.Check("MultiSet")

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(path) <= 0 || len(path) != len(data) {
		return false
	}

	nodes := []*treeNode{}
	for _, p := range path {
		tokens := splitPath(p)
		if len(tokens) <= 0 {
			return false
		}
		node := m.find(&m.root, tokens)
		if node == nil {
			return false
		}
		nodes = append(nodes, node)
	}
	for i := range nodes {
		nodes[i].data = data[i]
	}
	return true
}

func (m *MetaStoreMock) WriteBatch(ctx context.Context, ops ...WriteOp) (succ bool) {
	m.bp.Check("WriteBatch")

	m.mu.Lock()
	defer m.mu.Unlock()

	m.writtenBatches = append(m.writtenBatches, &WriteRequestRecord{
		Method: "WriteBatch",
		Ops:    ops,
	})

	if len(ops) <= 0 {
		return false
	}

	nodes := []*treeNode{}
	for _, op := range ops {
		tokens := splitPath(op.OpPath())
		if len(tokens) <= 0 {
			return false
		}
		switch op.(type) {
		case *CreateOp:
			if n := m.findParent(&m.root, tokens); n != nil {
				nodes = append(nodes, n)
			} else {
				return false
			}
		case *DeleteOp:
			if n := m.findParent(&m.root, tokens); n != nil {
				nodes = append(nodes, n)
			} else {
				return false
			}
		case *SetOp:
			if n := m.find(&m.root, tokens); n != nil {
				nodes = append(nodes, n)
			} else {
				return false
			}
		default:
			return false
		}
	}

	for i, op := range ops {
		tokens := splitPath(op.OpPath())
		last := tokens[len(tokens)-1]
		switch op := op.(type) {
		case *CreateOp:
			nodes[i].children[last] = &treeNode{
				data:     op.Data,
				children: make(map[string]*treeNode),
			}
		case *DeleteOp:
			delete(nodes[i].children, last)
		case *SetOp:
			nodes[i].data = op.Data
		}
	}
	return true
}

func (m *MetaStoreMock) RecursiveCreate(ctx context.Context, path string) (succ bool) {
	m.bp.Check("RecursiveCreate")

	m.mu.Lock()
	defer m.mu.Unlock()

	tokens := splitPath(path)
	if len(tokens) <= 0 {
		return false
	}
	root := &m.root
	for _, token := range tokens {
		n := root.children[token]
		if n != nil {
			root = n
		} else {
			n := &treeNode{
				data:     []byte{},
				children: make(map[string]*treeNode),
			}
			root.children[token] = n
			root = n
		}
	}
	return true
}

func (m *MetaStoreMock) RecursiveDelete(ctx context.Context, path string) (succ bool) {
	m.bp.Check("RecursiveDelete")

	m.mu.Lock()
	defer m.mu.Unlock()

	tokens := splitPath(path)
	if len(tokens) <= 0 {
		return false
	}
	last := len(tokens) - 1
	parent := m.find(&m.root, tokens[0:last])
	if parent == nil {
		return true
	}
	delete(parent.children, tokens[last])
	return true
}

func (m *MetaStoreMock) GetLastWriteBatchRecord() *WriteRequestRecord {
	if len(m.writtenBatches) == 0 {
		return nil
	}
	return m.writtenBatches[len(m.writtenBatches)-1]
}

func (m *MetaStoreMock) CleanWriteBachRecord() {
	m.writtenBatches = nil
}

func (m *MetaStoreMock) GetAllWriteBatches() []*WriteRequestRecord {
	return m.writtenBatches
}
