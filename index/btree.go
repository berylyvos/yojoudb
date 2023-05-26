package index

import (
	"bytes"
	"github.com/google/btree"
	"sort"
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

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return newBtreeIterator(bt.tree, reverse)
}

type btreeIterator struct {
	curIndex int
	reverse  bool
	values   []*Item
}

func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	saveValueFuc := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValueFuc)
	} else {
		tree.Ascend(saveValueFuc)
	}

	return &btreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (bi *btreeIterator) Rewind() {
	bi.curIndex = 0
}

func (bi *btreeIterator) Seek(key []byte) {
	if bi.reverse {
		bi.curIndex = sort.Search(len(bi.values), func(i int) bool {
			return bytes.Compare(bi.values[i].key, key) <= 0
		})
	} else {
		bi.curIndex = sort.Search(len(bi.values), func(i int) bool {
			return bytes.Compare(bi.values[i].key, key) >= 0
		})
	}
}

func (bi *btreeIterator) Next() {
	bi.curIndex += 1
}

func (bi *btreeIterator) Valid() bool {
	return bi.curIndex < len(bi.values)
}

func (bi *btreeIterator) Key() []byte {
	return bi.values[bi.curIndex].key
}

func (bi *btreeIterator) Value() Loc {
	return bi.values[bi.curIndex].loc
}

func (bi *btreeIterator) Close() {
	bi.values = nil
}
