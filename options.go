package yojoudb

import (
	"os"
	"yojoudb/meta"
)

type Options struct {
	DirPath      string
	DataFileSize int64
	SyncWrites   bool
	BytesPerSync uint
	IndexType    meta.IndexType
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

type WriteBatchOptions struct {
	MaxBatchNum uint
	SyncWrites  bool
}

var DefaultOptions = &Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	BytesPerSync: 0,
	IndexType:    meta.IndexART,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
