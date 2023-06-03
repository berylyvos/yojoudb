package yojoudb

import (
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"yojoudb/utils"
)

func TestDB_Merge(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-merge-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Merge()
	assert.Nil(t, err)
}

func TestDB_Merge2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-merge-2-")
	opts.DataFileSize = 32 * 1024 * 1024
	opts.MergeRatio = 0
	opts.DirPath = dir
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(1024))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 50000, len(keys))

	for i := 0; i < 50000; i++ {
		val, err := db2.Get(utils.TestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}

func TestDB_Merge3(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-merge-3")
	opts.DataFileSize = 32 * 1024 * 1024
	opts.MergeRatio = 0
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(1024))
		assert.Nil(t, err)
	}
	for i := 0; i < 10000; i++ {
		err := db.Delete(utils.TestKey(i))
		assert.Nil(t, err)
	}
	for i := 40000; i < 50000; i++ {
		err := db.Put(utils.TestKey(i), []byte("new value in merge"))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 40000, len(keys))

	for i := 0; i < 10000; i++ {
		_, err := db2.Get(utils.TestKey(i))
		assert.Equal(t, ErrKeyNotFound, err)
	}
	for i := 40000; i < 50000; i++ {
		val, err := db2.Get(utils.TestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, []byte("new value in merge"), val)
	}
}

func TestDB_Merge4(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-merge-4")
	opts.DataFileSize = 32 * 1024 * 1024
	opts.MergeRatio = 0
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(1024))
		assert.Nil(t, err)
	}
	for i := 0; i < 50000; i++ {
		err := db.Delete(utils.TestKey(i))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 0, len(keys))
}

func TestDB_Merge5(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-merge-5")
	opts.DataFileSize = 32 * 1024 * 1024
	opts.MergeRatio = 0
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(1024))
		assert.Nil(t, err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50000; i++ {
			err := db.Delete(utils.TestKey(i))
			assert.Nil(t, err)
		}
		for i := 60000; i < 70000; i++ {
			err := db.Put(utils.TestKey(i), utils.RandValue(1024))
			assert.Nil(t, err)
		}
	}()
	err = db.Merge()
	assert.Nil(t, err)
	wg.Wait()

	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 10000, len(keys))

	for i := 60000; i < 70000; i++ {
		val, err := db2.Get(utils.TestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}
