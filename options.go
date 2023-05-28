package yojoudb

import (
	"os"
	"yojoudb/meta"
)

type Options struct {
	DirPath      string
	DataFileSize int64
	SyncWrites   bool
	IndexType    meta.IndexType
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

var DefaultOptions = &Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    meta.IndexART,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
