package yojoudb

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"yojoudb/utils"
)

func TestDB_Open(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb")
	t.Log(dir)
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-put-")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// Put
	err = db.Put(utils.TestKey(1), utils.RandValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.TestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// Put same key
	err = db.Put(utils.TestKey(1), utils.RandValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.TestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// nil key
	err = db.Put(nil, utils.RandValue(24))
	assert.Equal(t, ErrKeyEmpty, err)

	// nil value
	err = db.Put(utils.TestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.TestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// write until active file turn into old files constantly
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))

	// restart db
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.RandValue(128)
	err = db2.Put(utils.TestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.TestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}
