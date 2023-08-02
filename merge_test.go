package yojoudb

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"testing"
	"yojoudb/utils"
)

func TestDB_Merge_1_Empty(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	err = db.Merge()
	assert.Nil(t, err)
}

func TestDB_Merge_2_All_Invalid(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(128))
		assert.Nil(t, err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Delete(utils.TestKey(i))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	_ = db.Close()
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()

	stat := db2.Stat()
	assert.Equal(t, uint64(0), stat.KeyNum)
}

func TestDB_Merge_3_All_Valid(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(128))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	_ = db.Close()
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()

	for i := 0; i < 100000; i++ {
		val, err := db2.Get(utils.TestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}

func TestDB_Merge_4_Twice(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(128))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)
	err = db.Merge()
	assert.Nil(t, err)

	_ = db.Close()
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()

	for i := 0; i < 100000; i++ {
		val, err := db2.Get(utils.TestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}

func TestDB_Merge_5_Mixed(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(128))
		assert.Nil(t, err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(128))
		assert.Nil(t, err)
	}
	for i := 100000; i < 300000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(128))
		assert.Nil(t, err)
	}
	for i := 100000; i < 200000; i++ {
		err := db.Delete(utils.TestKey(i))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	_ = db.Close()
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	stat := db2.Stat()
	assert.Equal(t, uint64(200000), stat.KeyNum)
}

func TestDB_Merge_6_Appending(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	for i := 0; i < 100000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(128))
		assert.Nil(t, err)
	}
	for i := 0; i < 100000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(128))
		assert.Nil(t, err)
	}
	for i := 100000; i < 300000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(128))
		assert.Nil(t, err)
	}
	for i := 100000; i < 200000; i++ {
		err := db.Delete(utils.TestKey(i))
		assert.Nil(t, err)
	}

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

	err = db.Merge()
	assert.Nil(t, err)

	wg.Wait()

	_ = db.Close()
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	stat := db2.Stat()
	var count int
	m.Range(func(key, value any) bool {
		count++
		return true
	})
	assert.Equal(t, uint64(200000+count), stat.KeyNum)
}
