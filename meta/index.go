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
)

func NewIndexer(indexType IndexType) Indexer {
	switch indexType {
	case IndexBTree:
		return NewBTree()
	case IndexART:
		return NewART()
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
