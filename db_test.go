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
	opts.DataFileSize = 4 * 1024 * 1024
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
	for i := 0; i < 100000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(128))
		assert.Nil(t, err)
	}
	assert.GreaterOrEqual(t, len(db.olderFiles), 2)

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

func TestDB_Get(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-get-")
	opts.DirPath = dir
	opts.DataFileSize = 4 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// Get
	err = db.Put(utils.TestKey(11), utils.RandValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.TestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// Get an unknown key
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	// Get after continuous put
	err = db.Put(utils.TestKey(22), utils.RandValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.TestKey(22), utils.RandValue(24))
	val3, err := db.Get(utils.TestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// Get after deleted
	err = db.Put(utils.TestKey(33), utils.RandValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.TestKey(33))
	assert.Nil(t, err)
	val4, err := db.Get(utils.TestKey(33))
	assert.Equal(t, 0, len(val4))
	assert.Equal(t, ErrKeyNotFound, err)

	// Get after file grow
	for i := 100; i < 100000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(128))
		assert.Nil(t, err)
	}
	assert.GreaterOrEqual(t, len(db.olderFiles), 2)
	val5, err := db.Get(utils.TestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	// Get old key after restart
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)
	val6, err := db2.Get(utils.TestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)
	val7, err := db2.Get(utils.TestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)
	val8, err := db2.Get(utils.TestKey(33))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-delete-")
	opts.DirPath = dir
	opts.DataFileSize = 4 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// Delete a exist key
	err = db.Put(utils.TestKey(11), utils.RandValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.TestKey(11))
	assert.Nil(t, err)
	_, err = db.Get(utils.TestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	// Delete an unknown key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// Delete nil key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyEmpty, err)

	err = db.Put(utils.TestKey(22), utils.RandValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.TestKey(22))
	assert.Nil(t, err)

	err = db.Put(utils.TestKey(22), utils.RandValue(128))
	assert.Nil(t, err)
	val1, err := db.Get(utils.TestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// restart
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)
	_, err = db2.Get(utils.TestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	val2, err := db2.Get(utils.TestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
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
