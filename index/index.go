package index

import (
	"bytes"
	"github.com/google/btree"
	"yojoudb/data"
)

// K alias for []byte
type K = []byte

// Loc alias for *data.LRLoc
type Loc = *data.LRLoc

// Indexer abstract index
type Indexer interface {
	Put(key K, loc Loc) bool
	Get(key K) Loc
	Delete(key K) bool
}

type Item struct {
	key K
	loc Loc
}

func (i *Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.key, bi.(*Item).key) == -1
}
