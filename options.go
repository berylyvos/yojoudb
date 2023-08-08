package yojoudb

import (
	"github.com/berylyvos/yojoudb/meta"
	"os"
)

type Options struct {
	DirPath        string
	SegmentSize    int64
	BlockCacheSize uint32
	Sync           bool
	BytesPerSync   uint32
	IndexType      meta.IndexType
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

type BatchOptions struct {
	Sync     bool
	ReadOnly bool
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:        tempDBDir(),
	SegmentSize:    GB,
	BlockCacheSize: 0,
	Sync:           false,
	BytesPerSync:   0,
	IndexType:      meta.IndexART,
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
