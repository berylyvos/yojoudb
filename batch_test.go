package yojoudb

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"yojoudb/utils"
)

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
