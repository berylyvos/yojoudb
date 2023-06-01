package meta

import (
	"yojoudb/data"
)

// K alias for []byte
type K = []byte

// Loc alias for *data.LRLoc
type Loc = *data.LRLoc

// Indexer abstract index
type Indexer interface {
	Put(key K, loc Loc) Loc
	Get(key K) Loc
	Delete(key K) (Loc, bool)
	Iterator(reverse bool) Iterator
	Size() int
	Close() error
}

type IndexType = uint8

const (
	IndexBTree IndexType = iota
	IndexART
	IndexBPT
)

func NewIndexer(indexType IndexType, dirPath string, syncWrites bool) Indexer {
	switch indexType {
	case IndexBTree:
		return NewBTree()
	case IndexART:
		return NewART()
	case IndexBPT:
		return NewBPlusTree(dirPath, syncWrites)
	default:
		panic("unsupported index type")
	}
}

type Iterator interface {
	Rewind()
	Seek(key []byte)
	Next()
	Valid() bool
	Key() []byte
	Value() Loc
	Close()
}
