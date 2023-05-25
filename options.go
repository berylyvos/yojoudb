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

var DefaultOptions = &Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    IndexBTree,
}
