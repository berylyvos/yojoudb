package meta

import (
	"bytes"
	gart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

type ART struct {
	tree gart.Tree
	mu   *sync.RWMutex
}

func NewART() *ART {
	return &ART{
		tree: gart.New(),
		mu:   new(sync.RWMutex),
	}
}

func (art *ART) Put(key K, loc Loc) Loc {
	art.mu.Lock()
	defer art.mu.Unlock()
	if oldVal, ok := art.tree.Insert(key, loc); ok {
		return oldVal.(Loc)
	}
	return nil
}

func (art *ART) Get(key K) Loc {
	art.mu.RLock()
	defer art.mu.RUnlock()
	if val, ok := art.tree.Search(key); ok {
		return val.(Loc)
	}
	return nil
}

func (art *ART) Delete(key K) (Loc, bool) {
	art.mu.Lock()
	defer art.mu.Unlock()
	oldVal, deleted := art.tree.Delete(key)
	if oldVal == nil {
		return nil, false
	}
	return oldVal.(Loc), deleted
}

func (art *ART) Iterator(reverse bool) Iterator {
	if art == nil {
		return nil
	}
	art.mu.RLock()
	defer art.mu.RUnlock()
	return newARTIterator(art.tree, reverse)
}

func (art *ART) Size() int {
	art.mu.RLock()
	defer art.mu.RUnlock()
	return art.tree.Size()
}

type artIterator struct {
	curIndex int
	reverse  bool
	values   []*Item
}

func newARTIterator(tree gart.Tree, reverse bool) *artIterator {
	var idx int
	sz := tree.Size()
	values := make([]*Item, sz)
	if reverse {
		idx = sz - 1
	}

	saveValueFunc := func(node gart.Node) bool {
		values[idx] = &Item{
			key: node.Key(),
			loc: node.Value().(Loc),
		}
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValueFunc)

	return &artIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (ai *artIterator) Rewind() {
	ai.curIndex = 0
}

func (ai *artIterator) Seek(key []byte) {
	if ai.reverse {
		ai.curIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.curIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

func (ai *artIterator) Next() {
	ai.curIndex += 1
}

func (ai *artIterator) Valid() bool {
	return ai.curIndex < len(ai.values)
}

func (ai *artIterator) Key() []byte {
	return ai.values[ai.curIndex].key
}

func (ai *artIterator) Value() Loc {
	return ai.values[ai.curIndex].loc
}

func (ai *artIterator) Close() {
	ai.values = nil
}
