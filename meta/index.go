package meta

import (
	"github.com/berylyvos/yojoudb/wal"
)

// K alias for []byte
type K = []byte

// Loc alias for *wal.ChunkLoc
type Loc = *wal.ChunkLoc
type Loc1 = wal.ChunkLoc

// Indexer is the interface for in-memory index.
type Indexer interface {
	Put(key K, loc Loc) Loc
	Get(key K) Loc
	Delete(key K) (Loc, bool)
	Iterator(opt IteratorOpt) Iterator
	Size() int
}

type IndexType = uint8

const (
	IndexBTree IndexType = iota
	IndexART
	IndexSKL
)

func NewIndexer(indexType IndexType) Indexer {
	switch indexType {
	case IndexBTree:
		return NewBTree()
	case IndexART:
		return NewART()
	case IndexSKL:
		return NewSkiplist()
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

type IteratorOpt struct {
	Prefix  K
	Reverse bool
}
