package yojoudb

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"yojoudb/utils"
)

func TestBatch_Put_Normal(t *testing.T) {
	// value 128B
	batchPutAndIterate(t, 1*GB, 10000, 128)
	// value 1KB
	batchPutAndIterate(t, 1*GB, 10000, KB)
	// value 32KB
	batchPutAndIterate(t, 1*GB, 1000, 32*KB)
}

func TestBatch_Put_IncrSegmentFile(t *testing.T) {
	batchPutAndIterate(t, 64*MB, 5000, 32*KB)
}

func TestBatch_Get_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	batch1 := db.NewBatch(DefaultBatchOptions)
	err = batch1.Put(utils.TestKey(12), utils.RandValue(128))
	assert.Nil(t, err)
	val1, err := batch1.Get(utils.TestKey(12))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	_ = batch1.Commit()

	generateData(t, db, 400, 500, 4*KB)

	batch2 := db.NewBatch(DefaultBatchOptions)
	err = batch2.Delete(utils.TestKey(450))
	assert.Nil(t, err)
	val, err := batch2.Get(utils.TestKey(450))
	assert.Nil(t, val)
	assert.Equal(t, ErrKeyNotFound, err)
	_ = batch2.Commit()

	// reopen
	_ = db.Close()
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	assertKeyExistOrNot(t, db2, utils.TestKey(12), true)
	assertKeyExistOrNot(t, db2, utils.TestKey(450), false)
}

func TestBatch_Delete_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	err = db.Delete([]byte("not exist"))
	assert.Nil(t, err)

	generateData(t, db, 1, 100, 128)
	err = db.Delete(utils.TestKey(99))
	assert.Nil(t, err)

	exist, err := db.Exist(utils.TestKey(99))
	assert.Nil(t, err)
	assert.False(t, exist)

	batch := db.NewBatch(DefaultBatchOptions)
	err = batch.Put(utils.TestKey(200), utils.RandValue(100))
	assert.Nil(t, err)
	err = batch.Delete(utils.TestKey(200))
	assert.Nil(t, err)
	exist1, err := batch.Exist(utils.TestKey(200))
	assert.Nil(t, err)
	assert.False(t, exist1)
	_ = batch.Commit()

	// reopen
	_ = db.Close()
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	assertKeyExistOrNot(t, db2, utils.TestKey(200), false)
}

func TestBatch_Rollback(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	key := []byte("shingo")
	value := []byte("gnix")

	batch := db.NewBatch(DefaultBatchOptions)
	err = batch.Put(key, value)
	assert.Nil(t, err)

	err = batch.Rollback()
	assert.Nil(t, err)

	resp, err := db.Get(key)
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Empty(t, resp)
}

func batchPutAndIterate(t *testing.T, segmentSize int64, size int, valueLen int) {
	options := DefaultOptions
	options.SegmentSize = segmentSize
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	batch := db.NewBatch(BatchOptions{})

	for i := 0; i < size; i++ {
		err := batch.Put(utils.TestKey(i), utils.RandValue(valueLen))
		assert.Nil(t, err)
	}
	err = batch.Commit()
	assert.Nil(t, err)

	for i := 0; i < size; i++ {
		value, err := db.Get(utils.TestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, len(utils.RandValue(valueLen)), len(value))
	}

	// reopen
	_ = db.Close()
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	for i := 0; i < size; i++ {
		value, err := db2.Get(utils.TestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, len(utils.RandValue(valueLen)), len(value))
	}
}

func generateData(t *testing.T, db *DB, start, end int, valueLen int) {
	for ; start < end; start++ {
		err := db.Put(utils.TestKey(start), utils.RandValue(valueLen))
		assert.Nil(t, err)
	}
}

func assertKeyExistOrNot(t *testing.T, db *DB, key []byte, exist bool) {
	val, err := db.Get(key)
	if exist {
		assert.Nil(t, err)
		assert.NotNil(t, val)
	} else {
		assert.Nil(t, val)
		assert.Equal(t, ErrKeyNotFound, err)
	}
}
