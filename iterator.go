package yojoudb

import (
	"bytes"
	"yojoudb/index"
)

type Iterator struct {
	options   IteratorOptions
	IndexIter index.Iterator
	db        *DB
}

func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	iterator := db.index.Iterator(options.Reverse)
	return &Iterator{
		IndexIter: iterator,
		options:   options,
		db:        db,
	}
}

func (it *Iterator) Rewind() {
	it.IndexIter.Rewind()
	it.skipToNext()
}

func (it *Iterator) Seek(key []byte) {
	it.IndexIter.Seek(key)
	it.skipToNext()
}

func (it *Iterator) Next() {
	it.IndexIter.Next()
	it.skipToNext()
}

func (it *Iterator) Valid() bool {
	return it.IndexIter.Valid()
}

func (it *Iterator) Key() []byte {
	return it.IndexIter.Key()
}

func (it *Iterator) Value() ([]byte, error) {
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.retrievalByLoc(it.IndexIter.Value())
}

func (it *Iterator) Close() {
	it.IndexIter.Close()
}

// skipToNext skips to next index which key start with it.options.Prefix
func (it *Iterator) skipToNext() {
	if len(it.options.Prefix) == 0 {
		return
	}
	for ; it.IndexIter.Valid(); it.IndexIter.Next() {
		if bytes.HasPrefix(it.IndexIter.Key(), it.options.Prefix) {
			break
		}
	}
}
