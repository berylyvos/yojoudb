package yojoudb

import (
	"github.com/berylyvos/yojoudb/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"sync"
	"testing"
)

func destroyDB(db *DB) {
	_ = db.Close()
	_ = os.RemoveAll(db.options.DirPath)
}

func TestDB_Put_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	for i := 0; i < 100; i++ {
		err := db.Put(utils.TestKey(rand.Int()), utils.RandValue(128))
		assert.Nil(t, err)
		err = db.Put(utils.TestKey(rand.Int()), utils.RandValue(KB))
		assert.Nil(t, err)
		err = db.Put(utils.TestKey(rand.Int()), utils.RandValue(5*KB))
		assert.Nil(t, err)
	}

	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	stat := db2.Stat()
	assert.Equal(t, uint64(300), stat.KeyNum)
}

func TestDB_Get_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	// not exist
	val1, err := db.Get([]byte("not-exist"))
	assert.Nil(t, val1)
	assert.Equal(t, ErrKeyNotFound, err)

	generateData(t, db, 1, 100, 128)
	for i := 1; i < 100; i++ {
		val, err := db.Get(utils.TestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, len(val), len(utils.RandValue(128)))
	}
	generateData(t, db, 200, 300, KB)
	for i := 200; i < 300; i++ {
		val, err := db.Get(utils.TestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, len(val), len(utils.RandValue(KB)))
	}
	generateData(t, db, 400, 500, 4*KB)
	for i := 400; i < 500; i++ {
		val, err := db.Get(utils.TestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, len(val), len(utils.RandValue(4*KB)))
	}
}

func TestDB_Close_Sync(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	err = db.Sync()
	assert.Nil(t, err)
}

func TestDB_Concurrent_Put(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	wg := sync.WaitGroup{}
	m := sync.Map{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				key := utils.TestKey(rand.Int())
				m.Store(string(key), struct{}{})
				e := db.Put(key, utils.RandValue(128))
				assert.Nil(t, e)
			}
		}()
	}
	wg.Wait()

	var count int
	m.Range(func(key, value any) bool {
		count++
		return true
	})
	assert.Equal(t, count, db.index.Size())
}
