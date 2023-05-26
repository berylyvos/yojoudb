package yojoudb

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"yojoudb/utils"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-iterator-")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-iterator-")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.TestKey(10), utils.TestKey(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	defer iterator.Close()
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.TestKey(10), iterator.Key())
	val, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.TestKey(10), val)
}

func TestDB_Iterator_Multi_Values(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-iterator-")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("aaa"), utils.RandValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bba"), utils.RandValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bbb"), utils.RandValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bbc"), utils.RandValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ccc"), utils.RandValue(10))
	assert.Nil(t, err)

	// iteration
	iter1 := db.NewIterator(DefaultIteratorOptions)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Rewind()
	for iter1.Seek([]byte("bb")); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Close()

	// reverse iteration
	iterOpts1 := DefaultIteratorOptions
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Rewind()
	for iter2.Seek([]byte("bb")); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Close()

	// with given prefix
	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Prefix = []byte("c")
	iter3 := db.NewIterator(iterOpts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
		t.Log(string(iter3.Key()))
	}
	iter3.Close()
}
