package yojoudb

type Options struct {
	DirPath      string
	DataFileSize int64
	SyncWrites   bool
	IndexType    IndexerType
}

type IndexerType = uint8

const (
	IndexBTree IndexerType = iota
	IndexART
)
