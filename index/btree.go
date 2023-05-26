package index

import (
	"bytes"
	"github.com/google/btree"
	"sync"
)

const (
	BTreeDegree = 32
)

type BTree struct {
	tree *btree.BTree
	mu   *sync.RWMutex
}

type Item struct {
	key K
	loc Loc
}

func (i *Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.key, bi.(*Item).key) == -1
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(BTreeDegree),
		mu:   new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key K, loc Loc) bool {
	item := &Item{
		key: key,
		loc: loc,
	}
	bt.mu.Lock()
	defer bt.mu.Unlock()

	bt.tree.ReplaceOrInsert(item)
	return true
}

func (bt *BTree) Get(key K) Loc {
	item := &Item{
		key: key,
	}
	if v := bt.tree.Get(item); v != nil {
		return v.(*Item).loc
	}
	return nil
}

func (bt *BTree) Delete(key K) bool {
	item := &Item{
		key: key,
	}
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if old := bt.tree.Delete(item); old == nil {
		return false
	}
	return true
}
