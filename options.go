package yojoudb

import (
	"os"
)

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

type IndexType = uint8

const (
	IndexBTree IndexType = iota
	IndexART
	IndexSKL
)

type Options struct {
	DirPath      string
	SegmentSize  int64
	Sync         bool
	BytesPerSync uint32
	IndexType    IndexType
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

type BatchOptions struct {
	Sync     bool
	ReadOnly bool
}

var DefaultOptions = Options{
	DirPath:      tempDBDir(),
	SegmentSize:  GB,
	Sync:         false,
	BytesPerSync: 0,
	IndexType:    IndexART,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultBatchOptions = BatchOptions{
	Sync:     true,
	ReadOnly: false,
}

func tempDBDir() string {
	dir, _ := os.MkdirTemp("", "yojoudb-temp-")
	return dir
}
