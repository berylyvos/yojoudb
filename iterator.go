package yojoudb

import (
	"github.com/berylyvos/yojoudb/meta"
)

// Iterator is the iterator of db based on IndexIter.
type Iterator struct {
	IndexIter meta.Iterator
	db        *DB
}

func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	return &Iterator{
		db: db,
		IndexIter: db.index.Iterator(meta.IteratorOpt{
			Prefix:  options.Prefix,
			Reverse: options.Reverse,
		}),
	}
}

// Rewind seeks the first key in the iterator.
func (it *Iterator) Rewind() {
	it.IndexIter.Rewind()
}

// Seek moves the iterator to the key which is
// greater(or less when reverse) than or equal
// to the specified key.
func (it *Iterator) Seek(key []byte) {
	it.IndexIter.Seek(key)
}

// Next moves the iterator to the next key.
func (it *Iterator) Next() {
	it.IndexIter.Next()
}

// Key gets the current key in the iterator.
func (it *Iterator) Key() []byte {
	return it.IndexIter.Key()
}

// Value gets the current val in the iterator.
func (it *Iterator) Value() ([]byte, error) {
	loc := it.IndexIter.Value()
	chunk, err := it.db.dataFiles.Read(loc)
	if err != nil {
		return nil, err
	}

	record := decodeLR(chunk)
	if record.Type == LRDeleted {
		return nil, ErrKeyNotFound
	}
	return record.Val, nil
}

// Valid checks if the iterator is in valid position.
func (it *Iterator) Valid() bool {
	return it.IndexIter.Valid()
}

// Close closes the iterator.
func (it *Iterator) Close() {
	it.IndexIter.Close()
}
