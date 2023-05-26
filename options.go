package yojoudb

import "os"

type IndexerType = uint8

const (
	IndexBTree IndexerType = iota
	IndexART
)

type Options struct {
	DirPath      string
	DataFileSize int64
	SyncWrites   bool
	IndexType    IndexerType
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

var DefaultOptions = &Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    IndexBTree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
