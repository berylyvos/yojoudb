package meta

import (
	"github.com/huandu/skiplist"
	"sync"
)

type Skiplist struct {
	list *skiplist.SkipList
	mu   *sync.RWMutex
}

func NewSkiplist() *Skiplist {
	return &Skiplist{
		list: skiplist.New(skiplist.Bytes),
		mu:   new(sync.RWMutex),
	}
}

func (sl *Skiplist) Put(key K, loc Loc) Loc {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	oldVal, ok := sl.list.GetValue(key)
	_ = sl.list.Set(key, loc)
	if ok {
		return oldVal.(Loc)
	}
	return nil
}

func (sl *Skiplist) Get(key K) Loc {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	if val, ok := sl.list.GetValue(key); ok {
		return val.(Loc)
	}
	return nil
}

func (sl *Skiplist) Delete(key K) (Loc, bool) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	oldElem := sl.list.Remove(key)
	if oldElem != nil {
		return oldElem.Value.(Loc), true
	}
	return nil, false
}

func (sl *Skiplist) Size() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.list.Len()
}

func (sl *Skiplist) Iterator(opt IteratorOpt) Iterator {
	return nil
}
